"""Base storage utilities for cloud path parsing and operations."""

import io
from typing import Tuple, List, Union, BinaryIO


def parse_cloud_path(path: str) -> Tuple[str, str, str]:
    """Parse a cloud storage path into service, bucket/container, and object components.

    The URL is split manually rather than with urllib.parse: '#' and '?' are
    legal characters in object keys, and urlparse would silently truncate the
    key at either (fragment/query), fetching the wrong object.

    Args:
        path: Cloud storage URL (gcs://, s3://, or abfss://).

    Returns:
        Tuple of (service, bucket, object_path).

    Raises:
        ValueError: If the URL scheme is not supported.
    """
    if '://' in path:
        scheme, rest = path.split('://', 1)
        scheme = scheme.lower()
    else:
        scheme, rest = '', path

    if '/' in rest:
        netloc, object_path = rest.split('/', 1)
    else:
        netloc, object_path = rest, ''
    object_path = object_path.lstrip('/')

    if scheme in ('gs', 'gcs'):
        service = 'gcs'
        bucket = netloc
    elif scheme == 's3':
        service = 's3'
        bucket = netloc
    elif scheme == 'abfss':
        # Azure Data Lake Storage Gen2 (ADLS Gen2)
        # Format: abfss://container@storageaccount.dfs.core.windows.net/path
        # The storage account is encoded in the host, which is why bare az://
        # URLs are not supported — they carry no account information.
        service = 'azure'
        if '@' in netloc:
            bucket, storage_account = netloc.split('@', 1)
            # Store storage account info for later use
            from ..config import cloud_config
            # Extract account name from storage_account (e.g., "account.dfs.core.windows.net")
            # Always assign so a second URL with a different account is honored
            # (avoids a stale account latching across paths in one process).
            cloud_config.azure_account = storage_account.split('.')[0]
        else:
            bucket = netloc
    else:
        raise ValueError(
            f"Unsupported scheme: {scheme or '(none)'}. "
            "Use gs://, gcs://, s3://, or abfss://."
        )

    return service, bucket, object_path


def get_stream(service: str, bucket: str, object_path: str) -> Union[io.BytesIO, BinaryIO]:
    """Get a file stream from the appropriate cloud storage service.

    Args:
        service: Cloud service identifier ('gcs', 's3', or 'azure').
        bucket: Bucket or container name.
        object_path: Object path within the bucket.

    Returns:
        File stream.

    Raises:
        ValueError: If the service is not supported.
    """
    if service == 'gcs':
        from .gcs import get_gcs_stream
        return get_gcs_stream(bucket, object_path)
    elif service == 's3':
        from .s3 import get_s3_stream
        return get_s3_stream(bucket, object_path)
    elif service == 'azure':
        from .azure import get_azure_stream
        return get_azure_stream(bucket, object_path)
    else:
        raise ValueError(f"Unsupported service: {service}")


def get_file_size(service: str, bucket: str, object_path: str) -> int:
    """Get the size of a file without downloading it.

    Args:
        service: Cloud service identifier ('gcs', 's3', or 'azure').
        bucket: Bucket or container name.
        object_path: Object path within the bucket.

    Returns:
        File size in bytes.

    Raises:
        ValueError: If the service is not supported.
    """
    if service == 'gcs':
        from .gcs import get_gcs_file_size
        return get_gcs_file_size(bucket, object_path)
    elif service == 's3':
        from .s3 import get_s3_file_size
        return get_s3_file_size(bucket, object_path)
    elif service == 'azure':
        from .azure import get_azure_file_size
        return get_azure_file_size(bucket, object_path)
    else:
        raise ValueError(f"Unsupported service: {service}")


def list_directory(service: str, bucket: str, prefix: str) -> List[Tuple[str, int]]:
    """List files in a cloud storage directory.

    Args:
        service: Cloud service identifier ('gcs', 's3', or 'azure').
        bucket: Bucket or container name.
        prefix: Directory prefix.

    Returns:
        List of (filename, size) tuples.

    Raises:
        ValueError: If the service is not supported.
    """
    if service == 'gcs':
        from .gcs import list_gcs_directory
        return list_gcs_directory(bucket, prefix)
    elif service == 's3':
        from .s3 import list_s3_directory
        return list_s3_directory(bucket, prefix)
    elif service == 'azure':
        from .azure import list_azure_directory
        return list_azure_directory(bucket, prefix)
    else:
        raise ValueError(f"Unsupported service: {service}")
