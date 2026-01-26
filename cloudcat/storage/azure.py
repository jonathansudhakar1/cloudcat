"""Azure Blob Storage client and operations."""

import io
import os
import sys
from typing import List, Tuple

from colorama import Fore, Style

from ..config import cloud_config

# Try to import Azure client
try:
    from azure.storage.blob import BlobServiceClient, ContainerClient
    HAS_AZURE = True
except ImportError:
    BlobServiceClient = None
    ContainerClient = None
    HAS_AZURE = False


def get_azure_blob_service_client():
    """Get an Azure BlobServiceClient with optional account configuration.

    Returns:
        azure.storage.blob.BlobServiceClient instance.

    Raises:
        SystemExit: If azure-storage-blob is not installed.
        ValueError: If Azure credentials are not configured.
    """
    if not HAS_AZURE:
        sys.stderr.write(
            Fore.RED + "Error: azure-storage-blob package is required for Azure access.\n" +
            "Install it with: pip install azure-storage-blob\n" + Style.RESET_ALL
        )
        sys.exit(1)

    # Check for explicit account override
    if cloud_config.azure_account:
        account_url = f"https://{cloud_config.azure_account}.blob.core.windows.net"
        from azure.identity import DefaultAzureCredential
        credential = DefaultAzureCredential()
        return BlobServiceClient(account_url=account_url, credential=credential)

    # Fall back to environment variables
    connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    account_url = os.environ.get('AZURE_STORAGE_ACCOUNT_URL')

    if connection_string:
        return BlobServiceClient.from_connection_string(connection_string)
    elif account_url:
        from azure.identity import DefaultAzureCredential
        credential = DefaultAzureCredential()
        return BlobServiceClient(account_url=account_url, credential=credential)
    else:
        raise ValueError(
            "Azure credentials not found. Use --account, or set "
            "AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_URL environment variable."
        )


def get_azure_stream(container_name: str, blob_name: str) -> io.BytesIO:
    """Get a file stream from Azure Blob Storage.

    Args:
        container_name: Azure container name.
        blob_name: Blob path within the container.

    Returns:
        BytesIO buffer containing the file content.
    """
    blob_service_client = get_azure_blob_service_client()
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Download blob to a BytesIO buffer
    buffer = io.BytesIO()
    download_stream = blob_client.download_blob()
    buffer.write(download_stream.readall())
    buffer.seek(0)

    return buffer


def get_azure_file_size(container_name: str, blob_name: str) -> int:
    """Get the size of an Azure blob without downloading it.

    Args:
        container_name: Azure container name.
        blob_name: Blob path within the container.

    Returns:
        File size in bytes.
    """
    blob_service_client = get_azure_blob_service_client()
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    properties = blob_client.get_blob_properties()
    return properties.size


def list_azure_directory(container_name: str, prefix: str) -> List[Tuple[str, int]]:
    """List files in an Azure Blob Storage container directory.

    Args:
        container_name: Azure container name.
        prefix: Directory prefix.

    Returns:
        List of (filename, size) tuples.
    """
    blob_service_client = get_azure_blob_service_client()
    container_client = blob_service_client.get_container_client(container_name)

    # Ensure prefix ends with / to indicate a directory
    if prefix and not prefix.endswith('/'):
        prefix = prefix + '/'

    # List blobs with the prefix
    file_list = []
    blobs = container_client.list_blobs(name_starts_with=prefix)
    for blob in blobs:
        if not blob.name.endswith('/'):
            file_list.append((blob.name, blob.size))

    return file_list
