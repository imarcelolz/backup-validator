import os
import re

from tqdm import tqdm

from src import db

SHA256_PATTERN = re.compile(r"^[a-fA-F0-9]{64}$")


def run_comparison(consolidated_id: int, source_ids: list[int]) -> dict:
    """
    Runs all 5 comparison layers between source HDs and the consolidated disk.
    Returns a stats dict.
    """
    stats = _layer1_sha256_crossref(consolidated_id, source_ids)
    audit = _layer2_count_audit(consolidated_id, source_ids)
    stats.update(audit)
    corrupted = _layer3_integrity_crossref(consolidated_id)
    stats["corrupted"] = corrupted
    stats["residual_duplicates"] = _layer4_duplicate_audit(consolidated_id)
    _layer5_persist_results(consolidated_id, source_ids)

    return stats


def _layer1_sha256_crossref(consolidated_id: int, source_ids: list[int]) -> dict:
    """
    Layer 1: Find source file hashes missing from consolidated.
    Inserts comparison_results for missing files.
    """
    missing_files = db.fetchall(
        """
        SELECT DISTINCT f.id, f.sha256, f.relative_path
        FROM files f
        WHERE f.source_id = ANY(%s)
          AND f.hash_status = 'hashed'
          AND f.sha256 IS NOT NULL
          AND f.sha256 NOT IN (
              SELECT c.sha256
              FROM files c
              WHERE c.source_id = %s
                AND c.hash_status = 'hashed'
                AND c.sha256 IS NOT NULL
          )
        """,
        (source_ids, consolidated_id),
    )

    for mf in tqdm(missing_files, desc="Recording missing files", unit=" files"):
        db.execute(
            """
            INSERT INTO comparison_results (source_file_id, status, notes)
            VALUES (%s, 'missing', %s)
            ON CONFLICT (source_file_id) DO UPDATE
            SET status = 'missing', notes = EXCLUDED.notes, compared_at = NOW()
            """,
            (mf["id"], f"SHA-256 {mf['sha256']} not found in consolidated"),
        )

    unique_source = db.fetchone(
        """
        SELECT COUNT(DISTINCT sha256) as cnt
        FROM files
        WHERE source_id = ANY(%s) AND hash_status = 'hashed' AND sha256 IS NOT NULL
        """,
        (source_ids,),
    )

    unique_consolidated = db.fetchone(
        """
        SELECT COUNT(DISTINCT sha256) as cnt
        FROM files
        WHERE source_id = %s AND hash_status = 'hashed' AND sha256 IS NOT NULL
        """,
        (consolidated_id,),
    )

    return {
        "unique_source_hashes": unique_source["cnt"],
        "unique_consolidated_hashes": unique_consolidated["cnt"],
        "missing": len(missing_files),
        "found": unique_source["cnt"] - len(missing_files),
    }


def _layer2_count_audit(consolidated_id: int, source_ids: list[int]) -> dict:
    """Layer 2: File count and size audit for sanity check."""
    source_stats = db.fetchone(
        """
        SELECT COUNT(*) as total_files,
               COALESCE(SUM(file_size), 0) as total_size,
               COUNT(DISTINCT sha256) as unique_hashes
        FROM files
        WHERE source_id = ANY(%s) AND hash_status = 'hashed'
        """,
        (source_ids,),
    )

    consolidated_stats = db.fetchone(
        """
        SELECT COUNT(*) as total_files,
               COALESCE(SUM(file_size), 0) as total_size,
               COUNT(DISTINCT sha256) as unique_hashes
        FROM files
        WHERE source_id = %s AND hash_status = 'hashed'
        """,
        (consolidated_id,),
    )

    return {
        "source_total_files": source_stats["total_files"],
        "source_total_size": source_stats["total_size"],
        "consolidated_total_files": consolidated_stats["total_files"],
        "consolidated_total_size": consolidated_stats["total_size"],
    }


def _layer3_integrity_crossref(consolidated_id: int) -> int:
    """
    Layer 3: Mark files that exist on consolidated but are corrupted.
    Updates comparison_results with 'corrupted' status.
    Returns count of corrupted files.
    """
    corrupted_files = db.fetchall(
        """
        SELECT f.id, f.sha256, f.relative_path, ic.error_message, ic.check_type
        FROM files f
        JOIN integrity_checks ic ON ic.file_id = f.id
        WHERE f.source_id = %s
          AND ic.status = 'corrupted'
          AND f.media_type IN ('image', 'video')
        """,
        (consolidated_id,),
    )

    for cf in corrupted_files:
        # Find which source files map to this hash
        source_files = db.fetchall(
            """
            SELECT f.id FROM files f
            JOIN sources s ON f.source_id = s.id AND s.role = 'source'
            WHERE f.sha256 = %s
            """,
            (cf["sha256"],),
        )

        for sf in source_files:
            db.execute(
                """
                INSERT INTO comparison_results (source_file_id, status, consolidated_file_id, notes)
                VALUES (%s, 'corrupted', %s, %s)
                ON CONFLICT (source_file_id) DO UPDATE
                SET status = 'corrupted',
                    consolidated_file_id = EXCLUDED.consolidated_file_id,
                    notes = EXCLUDED.notes,
                    compared_at = NOW()
                """,
                (sf["id"], cf["id"], f"{cf['check_type']}: {cf['error_message']}"),
            )

    return len(corrupted_files)


def _layer4_duplicate_audit(consolidated_id: int) -> int:
    """Layer 4: Count residual duplicate hashes on consolidated."""
    result = db.fetchone(
        """
        SELECT COUNT(*) as cnt FROM (
            SELECT sha256
            FROM files
            WHERE source_id = %s AND sha256 IS NOT NULL
            GROUP BY sha256
            HAVING COUNT(*) > 1
        ) dupes
        """,
        (consolidated_id,),
    )
    return result["cnt"]


def _layer5_persist_results(consolidated_id: int, source_ids: list[int]) -> None:
    """
    Layer 5: Mark all remaining source files as 'found'.
    Only processes files that don't have a comparison_result yet.
    """
    db.execute(
        """
        INSERT INTO comparison_results (source_file_id, status, consolidated_file_id)
        SELECT f.id, 'found', c.id
        FROM files f
        JOIN sources s ON f.source_id = s.id AND s.role = 'source'
        LEFT JOIN comparison_results cr ON cr.source_file_id = f.id
        LEFT JOIN files c ON c.sha256 = f.sha256
            AND c.source_id = %s
            AND c.hash_status = 'hashed'
        WHERE f.source_id = ANY(%s)
          AND f.hash_status = 'hashed'
          AND f.sha256 IS NOT NULL
          AND cr.id IS NULL
        ON CONFLICT (source_file_id) DO NOTHING
        """,
        (consolidated_id, source_ids),
    )


def find_valid_copies(identifier: str) -> list[dict]:
    """
    Finds valid copies of a file in source HDs.
    Accepts either a file path (on consolidated) or a SHA-256 hash.
    """
    sha256 = _resolve_sha256(identifier)
    if not sha256:
        return []

    return db.fetchall(
        """
        SELECT s.name as source_name, s.mount_path, f.relative_path,
               ic.status as integrity_status
        FROM files f
        JOIN sources s ON f.source_id = s.id AND s.role = 'source'
        LEFT JOIN integrity_checks ic ON ic.file_id = f.id
        WHERE f.sha256 = %s
          AND f.hash_status = 'hashed'
        ORDER BY
            CASE WHEN ic.status = 'ok' THEN 0
                 WHEN ic.status IS NULL THEN 1
                 ELSE 2
            END
        """,
        (sha256,),
    )


def _resolve_sha256(identifier: str) -> str | None:
    """Resolves an identifier to a SHA-256 hash."""
    if SHA256_PATTERN.match(identifier):
        return identifier.lower()

    # Try as a file path on consolidated
    for role in ("consolidated", "source"):
        row = db.fetchone(
            """
            SELECT f.sha256
            FROM files f
            JOIN sources s ON f.source_id = s.id
            WHERE (f.relative_path = %s OR %s LIKE '%%' || f.relative_path)
              AND f.sha256 IS NOT NULL
            LIMIT 1
            """,
            (identifier, identifier),
        )
        if row:
            return row["sha256"]

    return None
