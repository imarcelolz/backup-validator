import hashlib
import os
from datetime import datetime, timezone
from multiprocessing import Pool
from pathlib import Path

import magic
from tqdm import tqdm

from src import db
from src.config import (
    DISCOVERY_BATCH_SIZE,
    HASH_BATCH_SIZE,
    HASH_CHUNK_SIZE,
    MEDIA_TYPE_MAP,
)


def classify_media_type(mime: str | None) -> str:
    if not mime:
        return "other"
    prefix = mime.split("/")[0]
    return MEDIA_TYPE_MAP.get(prefix, "other")


def upsert_source(name: str, mount_path: str, role: str) -> int:
    """Creates or updates a source entry. Returns the source id."""
    row = db.fetchone(
        "SELECT id FROM sources WHERE name = %s",
        (name,),
    )
    if row:
        db.execute(
            "UPDATE sources SET mount_path = %s, role = %s WHERE id = %s",
            (mount_path, role, row["id"]),
        )
        return row["id"]

    result = db.fetchone(
        "INSERT INTO sources (name, mount_path, role) VALUES (%s, %s, %s) RETURNING id",
        (name, mount_path, role),
    )
    return result["id"]


def discover_files(source_id: int, mount_path: str, exclude_patterns: list[str] | None = None) -> int:
    """
    Walks the filesystem and inserts file records into the database.
    Returns the number of newly discovered files.
    """
    mount = Path(mount_path).resolve()
    if not mount.is_dir():
        raise FileNotFoundError(f"Mount path does not exist: {mount}")

    mime_detector = magic.Magic(mime=True)
    batch: list[tuple] = []
    discovered = 0
    skipped = 0

    insert_query = """
        INSERT INTO files (source_id, relative_path, filename, file_size, mime_type, media_type, is_symlink)
        VALUES %s
        ON CONFLICT (source_id, relative_path) DO NOTHING
    """
    template = "(%(source_id)s, %(relative_path)s, %(filename)s, %(file_size)s, %(mime_type)s, %(media_type)s, %(is_symlink)s)"

    progress = tqdm(desc="Discovering files", unit=" files")

    for dirpath, _dirnames, filenames in os.walk(mount, followlinks=False):
        for fname in filenames:
            full_path = os.path.join(dirpath, fname)

            if exclude_patterns and _matches_exclude(full_path, exclude_patterns):
                skipped += 1
                continue

            is_symlink = os.path.islink(full_path)

            try:
                stat = os.stat(full_path) if not is_symlink else os.lstat(full_path)
                file_size = stat.st_size
            except OSError:
                continue

            relative = os.path.relpath(full_path, mount)

            try:
                mime_type = mime_detector.from_file(full_path) if not is_symlink else None
            except Exception:
                mime_type = None

            media_type = classify_media_type(mime_type)

            batch.append({
                "source_id": source_id,
                "relative_path": relative,
                "filename": fname,
                "file_size": file_size,
                "mime_type": mime_type,
                "media_type": media_type,
                "is_symlink": is_symlink,
            })

            if len(batch) >= DISCOVERY_BATCH_SIZE:
                _flush_discovery_batch(insert_query, template, batch)
                discovered += len(batch)
                progress.update(len(batch))
                batch.clear()

    if batch:
        _flush_discovery_batch(insert_query, template, batch)
        discovered += len(batch)
        progress.update(len(batch))

    progress.close()

    db.execute(
        "UPDATE sources SET indexed_at = %s WHERE id = %s",
        (datetime.now(timezone.utc), source_id),
    )

    return discovered


def _flush_discovery_batch(query: str, template: str, batch: list[dict]) -> None:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_values
            execute_values(cur, query, batch, template=template, page_size=DISCOVERY_BATCH_SIZE)


def _matches_exclude(path: str, patterns: list[str]) -> bool:
    from fnmatch import fnmatch
    return any(fnmatch(path, p) or fnmatch(os.path.basename(path), p) for p in patterns)


def _hash_file_worker(args: tuple[int, str]) -> tuple[int, str | None, str | None]:
    """
    Worker function for multiprocessing. Pure function: no DB access.
    Returns (file_id, sha256_hex, error_message).
    """
    file_id, file_path = args
    try:
        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(HASH_CHUNK_SIZE):
                h.update(chunk)
        return (file_id, h.hexdigest(), None)
    except Exception as e:
        return (file_id, None, str(e))


def hash_pending_files(source_id: int, mount_path: str, workers: int) -> tuple[int, int]:
    """
    Hashes all files with hash_status='pending' for a given source.
    Returns (hashed_count, error_count).
    """
    mount = Path(mount_path).resolve()

    pending = db.fetchall(
        "SELECT id, relative_path FROM files WHERE source_id = %s AND hash_status = 'pending' AND is_symlink = FALSE",
        (source_id,),
    )

    if not pending:
        return (0, 0)

    work_items = [
        (row["id"], str(mount / row["relative_path"]))
        for row in pending
    ]

    hashed = 0
    errors = 0
    batch_ok: list[tuple] = []
    batch_err: list[tuple] = []

    with Pool(processes=workers) as pool:
        results = pool.imap_unordered(_hash_file_worker, work_items, chunksize=50)

        for file_id, sha256_hex, error_msg in tqdm(results, total=len(work_items), desc="Hashing", unit=" files"):
            if sha256_hex:
                batch_ok.append((sha256_hex, file_id))
                hashed += 1
            else:
                batch_err.append((error_msg, file_id))
                errors += 1

            if len(batch_ok) >= HASH_BATCH_SIZE:
                _flush_hash_ok(batch_ok)
                batch_ok.clear()

            if len(batch_err) >= HASH_BATCH_SIZE:
                _flush_hash_err(batch_err)
                batch_err.clear()

    if batch_ok:
        _flush_hash_ok(batch_ok)
    if batch_err:
        _flush_hash_err(batch_err)

    total_files = db.fetchone(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(file_size), 0) as total_size FROM files WHERE source_id = %s",
        (source_id,),
    )
    db.execute(
        "UPDATE sources SET indexed_at = %s WHERE id = %s",
        (datetime.now(timezone.utc), source_id),
    )

    return (hashed, errors)


def _flush_hash_ok(batch: list[tuple]) -> None:
    db.execute_batch(
        "UPDATE files SET sha256 = %s, hash_status = 'hashed' WHERE id = %s",
        batch,
    )


def _flush_hash_err(batch: list[tuple]) -> None:
    db.execute_batch(
        "UPDATE files SET hash_error = %s, hash_status = 'error' WHERE id = %s",
        batch,
    )


def get_index_stats(source_id: int) -> dict:
    """Returns indexing statistics for a source."""
    return db.fetchone(
        """
        SELECT
            COUNT(*) as total_files,
            COUNT(*) FILTER (WHERE hash_status = 'hashed') as hashed,
            COUNT(*) FILTER (WHERE hash_status = 'pending') as pending,
            COUNT(*) FILTER (WHERE hash_status = 'error') as errors,
            COUNT(*) FILTER (WHERE media_type = 'image') as images,
            COUNT(*) FILTER (WHERE media_type = 'video') as videos,
            COALESCE(SUM(file_size), 0) as total_size
        FROM files
        WHERE source_id = %s
        """,
        (source_id,),
    )
