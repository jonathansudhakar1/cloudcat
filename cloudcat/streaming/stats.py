"""Streaming statistics tracking for read efficiency reporting."""

from dataclasses import dataclass, field
from typing import Optional, List


def format_bytes(num_bytes: int) -> str:
    """Format bytes as human-readable string.

    Args:
        num_bytes: Number of bytes.

    Returns:
        Human-readable string (e.g., "1.5 MB", "256 KB").
    """
    if num_bytes < 0:
        return "0 B"

    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(num_bytes) < 1024.0:
            if unit == 'B':
                return f"{num_bytes} {unit}"
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0

    return f"{num_bytes:.1f} PB"


@dataclass
class StreamingStats:
    """Tracks streaming read statistics for efficiency reporting.

    Attributes:
        file_size: Total file size in bytes.
        bytes_read: Actual bytes read from storage.
        format_type: File format (parquet, csv, etc.).
        compression: Compression type if any.
        columns_requested: List of columns requested (for column projection).
        rows_requested: Number of rows requested.
        is_streaming: Whether true streaming was used.
        used_native_fs: Whether PyArrow native filesystem was used.
    """
    file_size: int = 0
    bytes_read: int = 0
    format_type: str = ""
    compression: Optional[str] = None
    columns_requested: Optional[List[str]] = None
    rows_requested: Optional[int] = None
    is_streaming: bool = True
    used_native_fs: bool = False

    @property
    def efficiency_percent(self) -> float:
        """Return percentage of file that was NOT read (savings).

        Returns:
            Efficiency as percentage (0-100). Higher means more savings.
        """
        if self.file_size == 0:
            return 0.0
        if self.bytes_read >= self.file_size:
            return 0.0
        return ((self.file_size - self.bytes_read) / self.file_size) * 100

    @property
    def read_percent(self) -> float:
        """Return percentage of file that was read.

        Returns:
            Read percentage (0-100).
        """
        if self.file_size == 0:
            return 100.0
        return min(100.0, (self.bytes_read / self.file_size) * 100)

    def format_report(self) -> str:
        """Generate human-readable efficiency report.

        Returns:
            Formatted string showing file size, data read, and percentage.
        """
        file_size_str = format_bytes(self.file_size)
        bytes_read_str = format_bytes(self.bytes_read)

        if self.file_size > 0 and self.bytes_read < self.file_size:
            return f"File size: {file_size_str} | Data read: {bytes_read_str} ({self.read_percent:.1f}%)"
        else:
            return f"File size: {file_size_str} | Data read: {bytes_read_str}"

    def add_bytes(self, count: int) -> None:
        """Add to bytes read counter.

        Args:
            count: Number of bytes to add.
        """
        self.bytes_read += count
