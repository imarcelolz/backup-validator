import csv
import io
import json
import sys

import click

from src import db


REPORT_QUERY = """
    SELECT
        s.name          AS source_hd,
        sf.relative_path,
        sf.filename,
        sf.file_size,
        sf.media_type,
        sf.sha256,
        cr.status,
        cf.relative_path AS consolidated_path,
        cr.notes
    FROM comparison_results cr
    JOIN files sf ON sf.id = cr.source_file_id
    JOIN sources s ON sf.source_id = s.id
    LEFT JOIN files cf ON cf.id = cr.consolidated_file_id
    WHERE 1=1
"""


def generate_report(
    fmt: str = "table",
    status_filter: str = "all",
    media_only: bool = False,
    output_path: str | None = None,
) -> None:
    """Generates a validation report in the specified format."""
    query = REPORT_QUERY
    params: list = []

    if status_filter != "all":
        query += " AND cr.status = %s"
        params.append(status_filter)

    if media_only:
        query += " AND sf.media_type IN ('image', 'video')"

    query += " ORDER BY cr.status, s.name, sf.relative_path"

    rows = db.fetchall(query, tuple(params))

    if not rows:
        click.echo("No results found.")
        return

    # Enrich corrupted rows with valid copy locations
    if status_filter in ("all", "corrupted"):
        _enrich_valid_copies(rows)

    output = _format_output(rows, fmt)

    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(output)
        click.echo(f"Report written to {output_path} ({len(rows)} rows)")
    else:
        click.echo(output)

    _print_summary(rows)


def _enrich_valid_copies(rows: list[dict]) -> None:
    """Adds valid_copy_location to corrupted rows."""
    for row in rows:
        if row["status"] != "corrupted" or not row["sha256"]:
            continue

        copies = db.fetchall(
            """
            SELECT s.name, s.mount_path, f.relative_path
            FROM files f
            JOIN sources s ON f.source_id = s.id AND s.role = 'source'
            LEFT JOIN integrity_checks ic ON ic.file_id = f.id
            WHERE f.sha256 = %s
              AND (ic.status = 'ok' OR ic.id IS NULL)
            LIMIT 3
            """,
            (row["sha256"],),
        )

        if copies:
            row["valid_copy_location"] = " | ".join(
                f"{c['name']}:{c['relative_path']}" for c in copies
            )
        else:
            row["valid_copy_location"] = None


def _format_output(rows: list[dict], fmt: str) -> str:
    if fmt == "csv":
        return _format_csv(rows)
    if fmt == "json":
        return _format_json(rows)
    return _format_table(rows)


def _format_csv(rows: list[dict]) -> str:
    output = io.StringIO()
    fieldnames = [
        "source_hd", "relative_path", "filename", "file_size",
        "media_type", "sha256", "status", "consolidated_path",
        "valid_copy_location", "notes",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _format_json(rows: list[dict]) -> str:
    clean = []
    for row in rows:
        entry = {
            "source_hd": row["source_hd"],
            "relative_path": row["relative_path"],
            "filename": row["filename"],
            "file_size": row["file_size"],
            "media_type": row["media_type"],
            "sha256": row["sha256"],
            "status": row["status"],
            "consolidated_path": row.get("consolidated_path"),
            "valid_copy_location": row.get("valid_copy_location"),
            "notes": row.get("notes"),
        }
        clean.append(entry)
    return json.dumps(clean, indent=2, ensure_ascii=False)


def _format_table(rows: list[dict]) -> str:
    lines = []
    header = f"{'STATUS':<12} {'SOURCE':<20} {'MEDIA':<8} {'PATH'}"
    lines.append(header)
    lines.append("-" * len(header))

    for row in rows:
        status = row["status"].upper()
        source = row["source_hd"][:20]
        media = (row["media_type"] or "other")[:8]
        path = row["relative_path"]

        line = f"{status:<12} {source:<20} {media:<8} {path}"

        if row["status"] == "corrupted" and row.get("valid_copy_location"):
            line += f"\n{'':>12} Valid copy: {row['valid_copy_location']}"

        lines.append(line)

    return "\n".join(lines)


def _print_summary(rows: list[dict]) -> None:
    counts = {}
    for row in rows:
        status = row["status"]
        counts[status] = counts.get(status, 0) + 1

    click.echo(f"\n--- Report Summary ---")
    for status in ("found", "missing", "corrupted"):
        count = counts.get(status, 0)
        click.echo(f"  {status.upper():<12} {count}")
    click.echo(f"  {'TOTAL':<12} {len(rows)}")
