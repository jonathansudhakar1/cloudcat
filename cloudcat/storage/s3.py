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

    endpoint = cloud_config.resolve_s3_endpoint()
    if cloud_config.s3_scheme == 'r2' and not endpoint:
        raise ValueError(
            "r2:// needs your account's endpoint. Pass "
            "--endpoint-url https://<accountid>.r2.cloudflarestorage.com, "
            "set endpoint-url in ~/.config/cloudcat/config.toml, or export "
            "AWS_ENDPOINT_URL_S3. (R2 uses S3-style access keys: set "
            "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY or use --profile.)"
        )

    session = (boto3.Session(profile_name=cloud_config.aws_profile)
               if cloud_config.aws_profile else boto3.Session())
    kwargs = {}
    if endpoint:
        kwargs['endpoint_url'] = endpoint
        # R2 expects region 'auto'; any custom endpoint needs *a* region for
        # signing even when the provider ignores it.
        if session.region_name is None:
            kwargs['region_name'] = 'auto' if cloud_config.s3_scheme == 'r2' else 'us-east-1'
    return session.client('s3', **kwargs)


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


def get_s3_file_size(bucket_name: str, object_name: str) -> int:
    """Get the size of an S3 object without downloading it.

    Args:
        bucket_name: S3 bucket name.
        object_name: Object key within the bucket.

    Returns:
        File size in bytes.
    """
    s3 = get_s3_client()
    response = s3.head_object(Bucket=bucket_name, Key=object_name)
    return response['ContentLength']


def list_s3_directory(bucket_name: str, prefix: str) -> List[Tuple[str, int]]:
    """List files in an S3 directory.

    Args:
        bucket_name: S3 bucket name.
        prefix: Directory prefix.

    Returns:
        List of (filename, size) tuples.
    """
    s3 = get_s3_client()

    # Ensure a non-empty prefix ends with / to indicate a directory.
    # An empty prefix (bucket root) must stay empty: S3 keys never begin with
    # '/', so a '/' prefix would match nothing and hide the whole bucket.
    if prefix and not prefix.endswith('/'):
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
