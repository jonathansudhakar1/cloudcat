"""Base storage utilities for cloud path parsing and operations."""

import io
from typing import Tuple, List, Union, BinaryIO
from urllib.parse import urlparse


def parse_cloud_path(path: str) -> Tuple[str, str, str]:
    """Parse a cloud storage path into service, bucket/container, and object components.

    Args:
        path: Cloud storage URL (gcs://, s3://, or az://).

    Returns:
        Tuple of (service, bucket, object_path).

    Raises:
        ValueError: If the URL scheme is not supported.
    """
    parsed = urlparse(path)

    if parsed.scheme == 'gs' or parsed.scheme == 'gcs':
        service = 'gcs'
        bucket = parsed.netloc
        object_path = parsed.path.lstrip('/')
    elif parsed.scheme == 's3':
        service = 's3'
        bucket = parsed.netloc
        object_path = parsed.path.lstrip('/')
    elif parsed.scheme == 'az' or parsed.scheme == 'azure':
        service = 'azure'
        bucket = parsed.netloc  # This is the container name
        object_path = parsed.path.lstrip('/')
    else:
        raise ValueError(f"Unsupported scheme: {parsed.scheme}. Use gcs://, s3://, or az://")

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
