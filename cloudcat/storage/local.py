"""Local filesystem "storage" backend.

Lets cloudcat preview files on disk (bare paths or file:// URLs) with the
exact same pipeline as cloud objects — formats, compression, filtering,
multi-file directories — and no credentials.

The (bucket, object_path) convention maps to bucket='' and object_path being
an absolute filesystem path.
"""

import os
from typing import BinaryIO, List, Tuple


def get_local_stream(bucket: str, file_path: str) -> BinaryIO:
    """Open a local file for binary reading.

    Args:
        bucket: Unused (kept for the storage-dispatch signature).
        file_path: Absolute path to the file.

    Returns:
        Binary file handle.
    """
    return open(file_path, 'rb')


def get_local_file_size(bucket: str, file_path: str) -> int:
    """Get the size of a local file in bytes."""
    return os.path.getsize(file_path)


def list_local_directory(bucket: str, prefix: str) -> List[Tuple[str, int]]:
    """Recursively list files under a local directory.

    Mirrors the cloud listing contract: returns (path, size) tuples for
    files only, using absolute paths.

    Args:
        bucket: Unused (kept for the storage-dispatch signature).
        prefix: Directory path.

    Returns:
        List of (absolute_path, size) tuples.
    """
    root = prefix.rstrip('/') or '/'
    if not os.path.isdir(root):
        return []

    file_list = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            full = os.path.join(dirpath, name)
            try:
                file_list.append((full, os.path.getsize(full)))
            except OSError:
                continue  # vanished or unreadable; skip like cloud listings do
    return file_list
