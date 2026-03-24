import json
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import magic
from PIL import Image
from tqdm import tqdm

from src import db
from src.models import IntegrityResult


def validate_single_file(file_path: str, deep: bool = False) -> list[IntegrityResult]:
    """
    Validates a single file without database access.
    Returns a list of IntegrityResult (one per check performed).
    """
    mime_detector = magic.Magic(mime=True)
    mime_type = mime_detector.from_file(file_path)
    results = []

    if mime_type.startswith("video/"):
        results.append(_check_video_ffprobe(file_path))
        if deep:
            results.append(_check_video_deep(file_path))

    elif mime_type.startswith("image/"):
        results.append(_check_image_exiftool(file_path))
        results.append(_check_image_pil(file_path))

    else:
        results.append(IntegrityResult(status="skipped", check_type="none", error_message=f"Not a media file: {mime_type}"))

    return results


def run_integrity_checks(
    source_id: int,
    mount_path: str,
    media_filter: str = "all",
    deep: bool = False,
    recheck: bool = False,
    workers: int = 4,
    limit: int = 0,
) -> dict:
    """
    Runs integrity checks on all media files for a source.
    Returns stats dict with ok/corrupted/errors/skipped counts.
    """
    mount = Path(mount_path).resolve()

    media_condition = _build_media_condition(media_filter)
    recheck_condition = "" if recheck else """
        AND NOT EXISTS (
            SELECT 1 FROM integrity_checks ic WHERE ic.file_id = f.id
        )
    """
    limit_clause = f"LIMIT {limit}" if limit > 0 else ""

    query = f"""
        SELECT f.id, f.relative_path, f.mime_type, f.media_type
        FROM files f
        WHERE f.source_id = %s
          AND f.is_symlink = FALSE
          AND f.hash_status = 'hashed'
          {media_condition}
          {recheck_condition}
        ORDER BY f.id
        {limit_clause}
    """

    files = db.fetchall(query, (source_id,))

    if not files:
        return {"ok": 0, "corrupted": 0, "errors": 0, "skipped": 0}

    stats = {"ok": 0, "corrupted": 0, "errors": 0, "skipped": 0}

    with ThreadPoolExecutor(max_workers=min(workers, 4)) as executor:
        futures = {}
        for f in files:
            abs_path = str(mount / f["relative_path"])
            future = executor.submit(
                _check_single_file_for_batch,
                f["id"], abs_path, f["mime_type"], deep,
            )
            futures[future] = f

        for future in tqdm(as_completed(futures), total=len(futures), desc="Integrity checks", unit=" files"):
            file_record = futures[future]
            try:
                results = future.result()
            except Exception as e:
                results = [IntegrityResult(status="error", check_type="unknown", error_message=str(e))]

            for result in results:
                _save_integrity_result(file_record["id"], result)
                stats[result.status] = stats.get(result.status, 0) + 1

    return stats


def _check_single_file_for_batch(
    file_id: int, file_path: str, mime_type: str | None, deep: bool
) -> list[IntegrityResult]:
    """Dispatches checks based on mime type. Used by batch processing."""
    if not mime_type:
        return [IntegrityResult(status="skipped", check_type="none")]

    results = []

    if mime_type.startswith("video/"):
        results.append(_check_video_ffprobe(file_path))
        if deep:
            results.append(_check_video_deep(file_path))

    elif mime_type.startswith("image/"):
        results.append(_check_image_exiftool(file_path))
        results.append(_check_image_pil(file_path))

    else:
        results.append(IntegrityResult(status="skipped", check_type="none"))

    return results


def _check_video_ffprobe(file_path: str) -> IntegrityResult:
    """Quick video validation via ffprobe metadata check."""
    start = time.monotonic()
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-i", file_path],
            capture_output=True, text=True, timeout=300,
        )
        duration = int((time.monotonic() - start) * 1000)

        if result.returncode != 0:
            error = result.stderr.strip()[:500]
            return IntegrityResult(status="corrupted", check_type="ffprobe", error_message=error, duration_ms=duration)

        return IntegrityResult(status="ok", check_type="ffprobe", duration_ms=duration)

    except subprocess.TimeoutExpired:
        duration = int((time.monotonic() - start) * 1000)
        return IntegrityResult(status="error", check_type="ffprobe", error_message="Timeout (300s)", duration_ms=duration)
    except Exception as e:
        duration = int((time.monotonic() - start) * 1000)
        return IntegrityResult(status="error", check_type="ffprobe", error_message=str(e), duration_ms=duration)


def _check_video_deep(file_path: str) -> IntegrityResult:
    """Deep video validation: decode entire file to /dev/null."""
    start = time.monotonic()
    try:
        result = subprocess.run(
            ["ffmpeg", "-v", "error", "-i", file_path, "-f", "null", "-"],
            capture_output=True, text=True, timeout=3600,
        )
        duration = int((time.monotonic() - start) * 1000)

        if result.returncode != 0:
            error = result.stderr.strip()[:500]
            return IntegrityResult(status="corrupted", check_type="ffmpeg_deep", error_message=error, duration_ms=duration)

        return IntegrityResult(status="ok", check_type="ffmpeg_deep", duration_ms=duration)

    except subprocess.TimeoutExpired:
        duration = int((time.monotonic() - start) * 1000)
        return IntegrityResult(status="error", check_type="ffmpeg_deep", error_message="Timeout (3600s)", duration_ms=duration)
    except Exception as e:
        duration = int((time.monotonic() - start) * 1000)
        return IntegrityResult(status="error", check_type="ffmpeg_deep", error_message=str(e), duration_ms=duration)


def _check_image_exiftool(file_path: str) -> IntegrityResult:
    """Image metadata validation via exiftool."""
    start = time.monotonic()
    try:
        result = subprocess.run(
            ["exiftool", "-warning", "-error", "-j", file_path],
            capture_output=True, text=True, timeout=60,
        )
        duration = int((time.monotonic() - start) * 1000)

        if result.returncode != 0:
            return IntegrityResult(status="error", check_type="exiftool", error_message=result.stderr.strip()[:500], duration_ms=duration)

        data = json.loads(result.stdout)
        if data and "Error" in data[0]:
            return IntegrityResult(status="corrupted", check_type="exiftool", error_message=data[0]["Error"], duration_ms=duration)

        return IntegrityResult(status="ok", check_type="exiftool", duration_ms=duration)

    except subprocess.TimeoutExpired:
        duration = int((time.monotonic() - start) * 1000)
        return IntegrityResult(status="error", check_type="exiftool", error_message="Timeout (60s)", duration_ms=duration)
    except Exception as e:
        duration = int((time.monotonic() - start) * 1000)
        return IntegrityResult(status="error", check_type="exiftool", error_message=str(e), duration_ms=duration)


def _check_image_pil(file_path: str) -> IntegrityResult:
    """Full pixel decode validation via PIL."""
    start = time.monotonic()
    try:
        with Image.open(file_path) as img:
            img.load()
        duration = int((time.monotonic() - start) * 1000)
        return IntegrityResult(status="ok", check_type="pil", duration_ms=duration)

    except Exception as e:
        duration = int((time.monotonic() - start) * 1000)
        return IntegrityResult(status="corrupted", check_type="pil", error_message=str(e), duration_ms=duration)


def _save_integrity_result(file_id: int, result: IntegrityResult) -> None:
    """Upserts an integrity check result into the database."""
    if result.check_type == "none":
        return

    db.execute(
        """
        INSERT INTO integrity_checks (file_id, check_type, status, error_message, duration_ms)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (file_id, check_type) DO UPDATE
        SET status = EXCLUDED.status,
            error_message = EXCLUDED.error_message,
            duration_ms = EXCLUDED.duration_ms,
            checked_at = NOW()
        """,
        (file_id, result.check_type, result.status, result.error_message, result.duration_ms),
    )


def _build_media_condition(media_filter: str) -> str:
    if media_filter == "all":
        return "AND f.media_type IN ('image', 'video')"
    return f"AND f.media_type = '{media_filter}'"
