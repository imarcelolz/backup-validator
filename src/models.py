from dataclasses import dataclass
from datetime import datetime


@dataclass
class Source:
    id: int
    name: str
    mount_path: str
    role: str
    indexed_at: datetime | None = None


@dataclass
class FileRecord:
    id: int
    source_id: int
    relative_path: str
    filename: str
    file_size: int
    sha256: str | None = None
    mime_type: str | None = None
    media_type: str | None = None
    is_symlink: bool = False
    hash_status: str = "pending"
    hash_error: str | None = None

    @property
    def absolute_path(self) -> str:
        """Requires source mount_path to be set externally."""
        return self._absolute_path

    @absolute_path.setter
    def absolute_path(self, value: str) -> None:
        self._absolute_path = value


@dataclass
class IntegrityResult:
    status: str
    check_type: str
    error_message: str | None = None
    duration_ms: int = 0


@dataclass
class ComparisonRow:
    source_hd: str
    relative_path: str
    filename: str
    file_size: int
    media_type: str | None
    sha256: str | None
    status: str
    consolidated_path: str | None = None
    valid_copy_location: str | None = None
    notes: str | None = None
