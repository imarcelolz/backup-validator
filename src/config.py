import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://validator:validator@localhost:5432/backup_validator",
)

DEFAULT_WORKERS = int(os.environ.get("WORKERS", "4"))

HASH_CHUNK_SIZE = 65_536
DISCOVERY_BATCH_SIZE = 500
HASH_BATCH_SIZE = 200

MEDIA_TYPE_MAP = {
    "image": "image",
    "video": "video",
    "audio": "audio",
}

IMAGE_EXTENSIONS = frozenset({
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif",
    ".webp", ".heic", ".heif", ".raw", ".cr2", ".nef", ".arw",
    ".dng", ".orf", ".rw2", ".svg", ".ico",
})

VIDEO_EXTENSIONS = frozenset({
    ".mp4", ".mov", ".avi", ".mkv", ".wmv", ".flv", ".webm",
    ".m4v", ".mpg", ".mpeg", ".3gp", ".mts", ".m2ts", ".ts",
    ".vob",
})
