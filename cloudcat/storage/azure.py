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
    HAS_AZURE = True
except ImportError:
    DataLakeServiceClient = None
    HAS_AZURE = False


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
    if not HAS_AZURE:
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


def list_azure_directory(container_name: str, prefix: str) -> List[Tuple[str, int]]:
    """List files in an Azure Data Lake Storage directory.

    Args:
        container_name: Azure filesystem (container) name.
        prefix: Directory prefix.

    Returns:
        List of (filename, size) tuples.
    """
    datalake_service_client = get_azure_datalake_service_client()
    file_system_client = datalake_service_client.get_file_system_client(file_system=container_name)

    # Ensure prefix ends with / to indicate a directory
    if prefix and not prefix.endswith('/'):
        prefix = prefix + '/'

    # List paths with the prefix
    file_list = []
    paths = file_system_client.get_paths(path=prefix.rstrip('/'))
    for path in paths:
        if not path.is_directory:
            file_list.append((path.name, path.content_length))

    return file_list
