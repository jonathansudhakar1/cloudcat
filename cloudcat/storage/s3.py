"""Amazon S3 client and operations."""

import sys
from typing import List, Tuple, BinaryIO

from colorama import Fore, Style

from ..config import cloud_config

# Try to import S3 client
try:
    import boto3
    import botocore
    HAS_S3 = True
except ImportError:
    boto3 = None
    botocore = None
    HAS_S3 = False


def get_s3_client():
    """Get an S3 client with optional profile configuration.

    Returns:
        boto3 S3 client instance.

    Raises:
        SystemExit: If boto3 is not installed.
    """
    if not HAS_S3:
        sys.stderr.write(
            Fore.RED + "Error: boto3 package is required for S3 access.\n" +
            "Install it with: pip install boto3\n" + Style.RESET_ALL
        )
        sys.exit(1)

    if cloud_config.aws_profile:
        session = boto3.Session(profile_name=cloud_config.aws_profile)
        return session.client('s3')
    else:
        return boto3.client('s3')


def get_s3_stream(bucket_name: str, object_name: str) -> BinaryIO:
    """Get a file stream from S3.

    Args:
        bucket_name: S3 bucket name.
        object_name: Object key within the bucket.

    Returns:
        Streaming body from S3 response.
    """
    s3 = get_s3_client()
    response = s3.get_object(Bucket=bucket_name, Key=object_name)
    return response['Body']


def list_s3_directory(bucket_name: str, prefix: str) -> List[Tuple[str, int]]:
    """List files in an S3 directory.

    Args:
        bucket_name: S3 bucket name.
        prefix: Directory prefix.

    Returns:
        List of (filename, size) tuples.
    """
    s3 = get_s3_client()

    # Ensure prefix ends with / to indicate a directory
    if not prefix.endswith('/'):
        prefix = prefix + '/'

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    # Return a list of files with their size
    file_list = []
    for page in pages:
        if 'Contents' in page:
            file_list.extend([
                (item['Key'], item['Size'])
                for item in page['Contents']
                if not item['Key'].endswith('/')
            ])

    return file_list
