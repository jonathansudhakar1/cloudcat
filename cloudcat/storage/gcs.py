"""Google Cloud Storage client and operations."""

import io
import sys
from typing import List, Tuple

from colorama import Fore, Style

from ..config import cloud_config

# Try to import GCS client
try:
    from google.cloud import storage as gcs
    HAS_GCS = True
except ImportError:
    gcs = None
    HAS_GCS = False


def get_gcs_client():
    """Get a GCS client with optional project/credentials configuration.

    Returns:
        google.cloud.storage.Client instance.

    Raises:
        SystemExit: If google-cloud-storage is not installed.
    """
    if not HAS_GCS:
        sys.stderr.write(
            Fore.RED + "Error: google-cloud-storage package is required for GCS access.\n" +
            "Install it with: pip install google-cloud-storage\n" + Style.RESET_ALL
        )
        sys.exit(1)

    kwargs = {}
    if cloud_config.gcp_project:
        kwargs['project'] = cloud_config.gcp_project
    if cloud_config.gcp_credentials:
        # Use explicit credentials file
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_file(
            cloud_config.gcp_credentials
        )
        kwargs['credentials'] = credentials

    return gcs.Client(**kwargs)


def get_gcs_stream(bucket_name: str, object_name: str) -> io.BytesIO:
    """Get a file stream from GCS.

    Args:
        bucket_name: GCS bucket name.
        object_name: Object path within the bucket.

    Returns:
        BytesIO buffer containing the file content.
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    # Create a streaming buffer
    buffer = io.BytesIO()
    blob.download_to_file(buffer)
    buffer.seek(0)

    return buffer


def list_gcs_directory(bucket_name: str, prefix: str) -> List[Tuple[str, int]]:
    """List files in a GCS directory.

    Args:
        bucket_name: GCS bucket name.
        prefix: Directory prefix.

    Returns:
        List of (filename, size) tuples.
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)

    # Ensure prefix ends with / to indicate a directory
    if not prefix.endswith('/'):
        prefix = prefix + '/'

    blobs = bucket.list_blobs(prefix=prefix)

    # Return a list of files with their size
    return [(blob.name, blob.size) for blob in blobs if not blob.name.endswith('/')]
