"""Cloud storage client modules."""

from .base import parse_cloud_path, get_stream, list_directory
from .gcs import get_gcs_client, get_gcs_stream, list_gcs_directory, HAS_GCS
from .s3 import get_s3_client, get_s3_stream, list_s3_directory, HAS_S3
from .azure import get_azure_blob_service_client, get_azure_stream, list_azure_directory, HAS_AZURE

__all__ = [
    'parse_cloud_path',
    'get_stream',
    'list_directory',
    'get_gcs_client',
    'get_gcs_stream',
    'list_gcs_directory',
    'HAS_GCS',
    'get_s3_client',
    'get_s3_stream',
    'list_s3_directory',
    'HAS_S3',
    'get_azure_blob_service_client',
    'get_azure_stream',
    'list_azure_directory',
    'HAS_AZURE',
]
