import shutil
import sys

import click

from src import db
from src.config import DEFAULT_WORKERS


def _format_size(size_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output.")
@click.pass_context
def cli(ctx, verbose):
    """Backup Validator — validate consolidated backup integrity."""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


# ---------------------------------------------------------------------------
# index
# ---------------------------------------------------------------------------
@cli.command()
@click.argument("path", type=click.Path(exists=True, file_okay=False))
@click.option("--name", required=True, help="Human-readable name for this HD (e.g., HD-Seagate-1).")
@click.option("--role", required=True, type=click.Choice(["source", "consolidated"]), help="Role of this disk.")
@click.option("--workers", "-w", default=DEFAULT_WORKERS, show_default=True, help="Parallel workers for hashing.")
@click.option("--hash-only", is_flag=True, help="Skip discovery, only hash pending files.")
@click.option("--exclude", multiple=True, help="Glob patterns to exclude (repeatable).")
def index(path, name, role, workers, hash_only, exclude):
    """Index a disk: discover files and compute SHA-256 hashes."""
    from src.indexer import upsert_source, discover_files, hash_pending_files, get_index_stats

    source_id = upsert_source(name, path, role)
    click.echo(f"Source '{name}' registered (id={source_id}, role={role})")

    if not hash_only:
        click.echo(f"\n--- Phase 1: Discovery ({path}) ---")
        discovered = discover_files(source_id, path, list(exclude) if exclude else None)
        click.echo(f"Discovered {discovered} new files.")

    click.echo(f"\n--- Phase 2: Hashing (workers={workers}) ---")
    hashed, errors = hash_pending_files(source_id, path, workers)
    click.echo(f"Hashed: {hashed} | Errors: {errors}")

    stats = get_index_stats(source_id)
    click.echo(f"\n--- Summary for '{name}' ---")
    click.echo(f"  Total files:  {stats['total_files']}")
    click.echo(f"  Hashed:       {stats['hashed']}")
    click.echo(f"  Pending:      {stats['pending']}")
    click.echo(f"  Errors:       {stats['errors']}")
    click.echo(f"  Images:       {stats['images']}")
    click.echo(f"  Videos:       {stats['videos']}")
    click.echo(f"  Total size:   {_format_size(stats['total_size'])}")


# ---------------------------------------------------------------------------
# check-integrity
# ---------------------------------------------------------------------------
@cli.command("check-integrity")
@click.option("--source", "source_name", required=True, help="Name of the indexed source to check.")
@click.option("--type", "media_filter", default="all", type=click.Choice(["image", "video", "all"]), help="Filter by media type.")
@click.option("--deep", is_flag=True, help="Run deep video decode check (slow but thorough).")
@click.option("--recheck", is_flag=True, help="Re-validate files that already have a check result.")
@click.option("--workers", "-w", default=4, show_default=True, help="Parallel workers for integrity checks.")
@click.option("--limit", default=0, help="Process only N files (0 = all).")
def check_integrity(source_name, media_filter, deep, recheck, workers, limit):
    """Validate media file integrity (ffprobe, exiftool, PIL)."""
    from src.integrity import run_integrity_checks

    _require_tools(["ffprobe", "exiftool"])

    source = db.fetchone("SELECT id, mount_path FROM sources WHERE name = %s", (source_name,))
    if not source:
        click.echo(f"Error: source '{source_name}' not found. Run 'index' first.", err=True)
        sys.exit(1)

    stats = run_integrity_checks(
        source_id=source["id"],
        mount_path=source["mount_path"],
        media_filter=media_filter,
        deep=deep,
        recheck=recheck,
        workers=workers,
        limit=limit,
    )

    click.echo(f"\n--- Integrity Results for '{source_name}' ---")
    click.echo(f"  OK:        {stats['ok']}")
    click.echo(f"  Corrupted: {stats['corrupted']}")
    click.echo(f"  Errors:    {stats['errors']}")
    click.echo(f"  Skipped:   {stats['skipped']}")


# ---------------------------------------------------------------------------
# validate (single file, no DB required)
# ---------------------------------------------------------------------------
@cli.command()
@click.argument("file_path", type=click.Path(exists=True, dir_okay=False))
@click.option("--deep", is_flag=True, help="Run deep video decode check.")
def validate(file_path, deep):
    """Validate a single file's integrity (no database required)."""
    from src.integrity import validate_single_file

    _require_tools(["ffprobe", "exiftool"])

    results = validate_single_file(file_path, deep=deep)

    for result in results:
        icon = "OK" if result.status == "ok" else "FAIL"
        msg = f"  [{icon}] {result.check_type}: {result.status}"
        if result.error_message:
            msg += f" — {result.error_message}"
        msg += f" ({result.duration_ms}ms)"
        click.echo(msg)

    has_failure = any(r.status == "corrupted" for r in results)
    if has_failure:
        click.echo("\nResult: CORRUPTED")
        sys.exit(1)
    else:
        click.echo("\nResult: OK")


# ---------------------------------------------------------------------------
# compare
# ---------------------------------------------------------------------------
@cli.command()
@click.option("--consolidated", "consolidated_name", required=True, help="Name of the consolidated source.")
@click.option("--source", "source_names", multiple=True, help="Limit to specific source HDs (repeatable). If omitted, compares against all sources.")
def compare(consolidated_name, source_names):
    """Cross-reference source HDs against the consolidated disk."""
    from src.comparator import run_comparison

    consolidated = db.fetchone("SELECT id FROM sources WHERE name = %s AND role = 'consolidated'", (consolidated_name,))
    if not consolidated:
        click.echo(f"Error: consolidated source '{consolidated_name}' not found.", err=True)
        sys.exit(1)

    source_ids = []
    if source_names:
        for sn in source_names:
            row = db.fetchone("SELECT id FROM sources WHERE name = %s AND role = 'source'", (sn,))
            if not row:
                click.echo(f"Error: source '{sn}' not found.", err=True)
                sys.exit(1)
            source_ids.append(row["id"])
    else:
        rows = db.fetchall("SELECT id FROM sources WHERE role = 'source'")
        source_ids = [r["id"] for r in rows]

    if not source_ids:
        click.echo("Error: no source HDs found. Run 'index' first.", err=True)
        sys.exit(1)

    stats = run_comparison(consolidated["id"], source_ids)

    click.echo("\n--- Comparison Results ---")
    click.echo(f"  Unique hashes in sources:      {stats['unique_source_hashes']}")
    click.echo(f"  Unique hashes in consolidated:  {stats['unique_consolidated_hashes']}")
    click.echo(f"  Found (matched):               {stats['found']}")
    click.echo(f"  Missing:                       {stats['missing']}")
    click.echo(f"  Corrupted:                     {stats['corrupted']}")
    click.echo(f"  Residual duplicates:           {stats['residual_duplicates']}")


# ---------------------------------------------------------------------------
# find-valid-copy
# ---------------------------------------------------------------------------
@cli.command("find-valid-copy")
@click.argument("identifier")
def find_valid_copy(identifier):
    """Find a valid copy of a file in source HDs. Accepts a file path or SHA-256 hash."""
    from src.comparator import find_valid_copies

    copies = find_valid_copies(identifier)

    if not copies:
        click.echo("No valid copies found in source HDs.")
        sys.exit(1)

    click.echo(f"Found {len(copies)} valid copy(ies):")
    for c in copies:
        click.echo(f"  [{c['source_name']}] {c['mount_path']}/{c['relative_path']}")


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------
@cli.command()
@click.option("--format", "fmt", default="table", type=click.Choice(["csv", "json", "table"]), help="Output format.")
@click.option("--status", "status_filter", default="all", type=click.Choice(["missing", "corrupted", "found", "all"]), help="Filter by status.")
@click.option("--media-only", is_flag=True, help="Only include image/video files.")
@click.option("--output", "-o", type=click.Path(), help="Write output to file instead of stdout.")
def report(fmt, status_filter, media_only, output):
    """Generate validation report."""
    from src.reporter import generate_report

    generate_report(
        fmt=fmt,
        status_filter=status_filter,
        media_only=media_only,
        output_path=output,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _require_tools(tools: list[str]) -> None:
    missing = [t for t in tools if shutil.which(t) is None]
    if missing:
        click.echo(f"Error: required tools not found: {', '.join(missing)}", err=True)
        click.echo("Install them via: sudo apt-get install ffmpeg libimage-exiftool-perl", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
