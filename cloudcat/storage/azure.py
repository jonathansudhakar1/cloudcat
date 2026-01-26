"""Azure Data Lake Storage Gen2 (ADLS Gen2) client and operations."""

import io
import os
import sys
from typing import List, Tuple

from colorama import Fore, Style

from ..config import cloud_config

# Try to import Azure Data Lake client
try:
    from azure.storage.filedatalake import DataLakeServiceClient
    HAS_AZURE_DATALAKE = True
except ImportError:
    DataLakeServiceClient = None
    HAS_AZURE_DATALAKE = False

# Try to import Azure Blob client (fallback for non-HNS accounts)
try:
    from azure.storage.blob import BlobServiceClient
    HAS_AZURE_BLOB = True
except ImportError:
    BlobServiceClient = None
    HAS_AZURE_BLOB = False

# Combined availability check
HAS_AZURE = HAS_AZURE_DATALAKE or HAS_AZURE_BLOB


def get_azure_datalake_service_client():
    """Get an Azure DataLakeServiceClient with optional account configuration.

    Authentication priority:
    1. Access key (--az-access-key or AZURE_STORAGE_ACCESS_KEY env var)
    2. DefaultAzureCredential (az login, managed identity, etc.)

    Returns:
        azure.storage.filedatalake.DataLakeServiceClient instance.

    Raises:
        SystemExit: If azure-storage-file-datalake is not installed.
        ValueError: If Azure credentials are not configured.
    """
    if not HAS_AZURE_DATALAKE:
        sys.stderr.write(
            Fore.RED + "Error: azure-storage-file-datalake package is required for Azure access.\n" +
            "Install it with: pip install azure-storage-file-datalake\n" + Style.RESET_ALL
        )
        sys.exit(1)

    # Get account name (set by abfss:// URL parsing)
    account_name = cloud_config.azure_account
    if not account_name:
        raise ValueError(
            "Azure storage account not found. Use abfss:// URL format: "
            "abfss://container@account.dfs.core.windows.net/path"
        )

    account_url = f"https://{account_name}.dfs.core.windows.net"

    # Check for access key (CLI option or environment variable)
    access_key = cloud_config.azure_access_key or os.environ.get('AZURE_STORAGE_ACCESS_KEY')

    if access_key:
        # Use access key authentication
        return DataLakeServiceClient(account_url=account_url, credential=access_key)
    else:
        # Fall back to DefaultAzureCredential (az login, managed identity, etc.)
        from azure.identity import DefaultAzureCredential
        credential = DefaultAzureCredential()
        return DataLakeServiceClient(account_url=account_url, credential=credential)


def _get_blob_service_client():
    """Get an Azure BlobServiceClient for fallback operations.

    Used when DataLake API is not available or fails (e.g., non-HNS accounts).

    Returns:
        azure.storage.blob.BlobServiceClient instance.
    """
    if not HAS_AZURE_BLOB:
        sys.stderr.write(
            Fore.RED + "Error: azure-storage-blob package is required for Azure access.\n" +
            "Install it with: pip install azure-storage-blob\n" + Style.RESET_ALL
        )
        sys.exit(1)

    account_name = cloud_config.azure_account
    if not account_name:
        raise ValueError(
            "Azure storage account not found. Use abfss:// URL format: "
            "abfss://container@account.dfs.core.windows.net/path"
        )

    account_url = f"https://{account_name}.blob.core.windows.net"
    access_key = cloud_config.azure_access_key or os.environ.get('AZURE_STORAGE_ACCESS_KEY')

    if access_key:
        return BlobServiceClient(account_url=account_url, credential=access_key)
    else:
        from azure.identity import DefaultAzureCredential
        credential = DefaultAzureCredential()
        return BlobServiceClient(account_url=account_url, credential=credential)


# Keep old function name as alias for backwards compatibility
get_azure_blob_service_client = get_azure_datalake_service_client


def get_azure_stream(container_name: str, file_path: str) -> io.BytesIO:
    """Get a file stream from Azure Data Lake Storage Gen2.

    Args:
        container_name: Azure filesystem (container) name.
        file_path: File path within the filesystem.

    Returns:
        BytesIO buffer containing the file content.
    """
    datalake_service_client = get_azure_datalake_service_client()
    file_system_client = datalake_service_client.get_file_system_client(file_system=container_name)
    file_client = file_system_client.get_file_client(file_path)

    # Download file to a BytesIO buffer
    buffer = io.BytesIO()
    download = file_client.download_file()
    buffer.write(download.readall())
    buffer.seek(0)

    return buffer


def get_azure_file_size(container_name: str, file_path: str) -> int:
    """Get the size of an Azure Data Lake file without downloading it.

    Args:
        container_name: Azure filesystem (container) name.
        file_path: File path within the filesystem.

    Returns:
        File size in bytes.
    """
    datalake_service_client = get_azure_datalake_service_client()
    file_system_client = datalake_service_client.get_file_system_client(file_system=container_name)
    file_client = file_system_client.get_file_client(file_path)
    properties = file_client.get_file_properties()
    return properties.size


def _list_azure_directory_datalake(container_name: str, prefix: str) -> List[Tuple[str, int]]:
    """List files using Azure Data Lake Storage API (requires HNS enabled)."""
    datalake_service_client = get_azure_datalake_service_client()
    file_system_client = datalake_service_client.get_file_system_client(file_system=container_name)

    # Ensure prefix ends with / to indicate a directory
    if prefix and not prefix.endswith('/'):
        prefix = prefix + '/'

    # List paths with the prefix
    file_list = []
    paths = file_system_client.get_paths(path=prefix.rstrip('/') if prefix else None)
    for path in paths:
        if not path.is_directory:
            file_list.append((path.name, path.content_length))

    return file_list


def _list_azure_directory_blob(container_name: str, prefix: str) -> List[Tuple[str, int]]:
    """List files using Azure Blob Storage API (works with any storage account)."""
    blob_service_client = _get_blob_service_client()
    container_client = blob_service_client.get_container_client(container_name)

    # Ensure prefix ends with / to indicate a directory (if not empty)
    if prefix and not prefix.endswith('/'):
        prefix = prefix + '/'

    # List blobs with the prefix
    file_list = []
    blobs = container_client.list_blobs(name_starts_with=prefix if prefix else None)
    for blob in blobs:
        # Skip "directory" blobs (size 0, name ends with /)
        if blob.size > 0 and not blob.name.endswith('/'):
            file_list.append((blob.name, blob.size))

    return file_list


def list_azure_directory(container_name: str, prefix: str) -> List[Tuple[str, int]]:
    """List files in an Azure storage directory.

    Tries Data Lake API first (for HNS-enabled accounts), then falls back
    to Blob Storage API (works with any storage account).

    Args:
        container_name: Azure filesystem (container) name.
        prefix: Directory prefix.

    Returns:
        List of (filename, size) tuples.
    """
    # Try Data Lake API first (better performance for HNS accounts)
    if HAS_AZURE_DATALAKE:
        try:
            return _list_azure_directory_datalake(container_name, prefix)
        except Exception as e:
            # Check if it's an endpoint/feature incompatibility error
            error_str = str(e)
            if 'EndpointUnsupportedAccountFeatures' in error_str or 'BlobStorageEvents' in error_str:
                # Fall back to Blob API
                if HAS_AZURE_BLOB:
                    return _list_azure_directory_blob(container_name, prefix)
            # Re-raise other errors
            raise

    # Fall back to Blob API if Data Lake not available
    if HAS_AZURE_BLOB:
        return _list_azure_directory_blob(container_name, prefix)

    # Neither API available
    sys.stderr.write(
        Fore.RED + "Error: azure-storage-file-datalake or azure-storage-blob package is required.\n" +
        "Install with: pip install azure-storage-file-datalake\n" + Style.RESET_ALL
    )
    sys.exit(1)
