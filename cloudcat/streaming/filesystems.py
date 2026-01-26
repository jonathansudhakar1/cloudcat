"""PyArrow filesystem factory for native cloud access."""

from typing import Optional, Tuple, Any
import sys

from colorama import Fore, Style


# Track PyArrow filesystem availability
HAS_PYARROW_FS = False
try:
    from pyarrow import fs as pa_fs
    HAS_PYARROW_FS = True
except ImportError:
    pa_fs = None


def supports_pyarrow_fs() -> bool:
    """Check if PyArrow filesystem is available.

    Returns:
        True if pyarrow.fs module is available.
    """
    return HAS_PYARROW_FS


def get_pyarrow_filesystem(
    service: str,
    aws_profile: Optional[str] = None,
    gcp_project: Optional[str] = None,
    gcp_credentials: Optional[str] = None,
    azure_account: Optional[str] = None,
    azure_access_key: Optional[str] = None
) -> Tuple[Any, str]:
    """Create a PyArrow filesystem for native cloud access.

    This enables true streaming with range requests for columnar formats,
    allowing column projection to only fetch required column chunks.

    Args:
        service: Cloud service identifier ('gcs', 's3', or 'azure').
        aws_profile: AWS profile name for S3.
        gcp_project: GCP project ID for GCS.
        gcp_credentials: Path to GCP service account JSON for GCS.
        azure_account: Azure storage account name.
        azure_access_key: Azure storage account access key.

    Returns:
        Tuple of (filesystem, path_prefix) where path_prefix is used
        to construct the full path for the filesystem.

    Raises:
        ImportError: If pyarrow.fs is not available.
        ValueError: If the service is not supported.
    """
    if not HAS_PYARROW_FS:
        raise ImportError(
            "pyarrow.fs is required for native cloud filesystem access. "
            "Install with: pip install pyarrow"
        )

    if service == 's3':
        return _get_s3_filesystem(aws_profile)
    elif service == 'gcs':
        return _get_gcs_filesystem(gcp_project, gcp_credentials)
    elif service == 'azure':
        return _get_azure_filesystem(azure_account, azure_access_key)
    else:
        raise ValueError(f"Unsupported service for PyArrow filesystem: {service}")


def _get_s3_filesystem(
    aws_profile: Optional[str] = None
) -> Tuple[Any, str]:
    """Create PyArrow S3FileSystem.

    Args:
        aws_profile: AWS profile name.

    Returns:
        Tuple of (S3FileSystem, path_prefix).
    """
    kwargs = {}

    if aws_profile:
        # PyArrow S3FileSystem doesn't directly support profiles,
        # but we can use boto3 to get credentials from profile
        try:
            import boto3
            session = boto3.Session(profile_name=aws_profile)
            creds = session.get_credentials()
            if creds:
                frozen_creds = creds.get_frozen_credentials()
                kwargs['access_key'] = frozen_creds.access_key
                kwargs['secret_key'] = frozen_creds.secret_key
                if frozen_creds.token:
                    kwargs['session_token'] = frozen_creds.token
        except ImportError:
            # boto3 not available, let PyArrow use default credentials
            pass
        except Exception:
            # Profile not found or other error, let PyArrow use default
            pass

    filesystem = pa_fs.S3FileSystem(**kwargs)
    return filesystem, ''


def _get_gcs_filesystem(
    gcp_project: Optional[str] = None,
    gcp_credentials: Optional[str] = None
) -> Tuple[Any, str]:
    """Create PyArrow GcsFileSystem.

    Args:
        gcp_project: GCP project ID.
        gcp_credentials: Path to service account JSON file.

    Returns:
        Tuple of (GcsFileSystem, path_prefix).
    """
    kwargs = {}

    if gcp_project:
        kwargs['project_id'] = gcp_project

    if gcp_credentials:
        # Read the credentials file for PyArrow
        # PyArrow GcsFileSystem accepts credentials as JSON string
        try:
            with open(gcp_credentials, 'r') as f:
                import json
                creds_data = json.load(f)
                # PyArrow expects the JSON as a string
                kwargs['credentials'] = json.dumps(creds_data)
        except Exception:
            # Fall back to default credentials
            pass

    filesystem = pa_fs.GcsFileSystem(**kwargs)
    return filesystem, ''


def _get_azure_filesystem(
    azure_account: Optional[str] = None,
    azure_access_key: Optional[str] = None
) -> Tuple[Any, str]:
    """Create PyArrow AzureFileSystem for ADLS Gen2.

    Args:
        azure_account: Azure storage account name.
        azure_access_key: Azure storage account access key.

    Returns:
        Tuple of (AzureFileSystem, path_prefix).
    """
    import os

    kwargs = {}

    if azure_account:
        kwargs['account_name'] = azure_account

    # Check for access key (parameter or environment variable)
    access_key = azure_access_key or os.environ.get('AZURE_STORAGE_ACCESS_KEY')
    if access_key:
        kwargs['account_key'] = access_key

    filesystem = pa_fs.AzureFileSystem(**kwargs)
    return filesystem, ''


def get_pyarrow_path(service: str, bucket: str, object_path: str) -> str:
    """Construct the full path for PyArrow filesystem.

    Args:
        service: Cloud service identifier.
        bucket: Bucket or container name.
        object_path: Object path within the bucket.

    Returns:
        Full path string for PyArrow filesystem operations.
    """
    # PyArrow expects paths as bucket/object_path
    return f"{bucket}/{object_path}"
