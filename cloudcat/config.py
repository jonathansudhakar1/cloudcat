"""Configuration management for cloudcat."""

import os
from typing import Optional


class CloudConfig:
    """Global configuration for cloud provider credentials.

    This class stores credential settings that can be configured via CLI options
    and are used by storage client functions.
    """

    def __init__(self) -> None:
        self.aws_profile: Optional[str] = None
        self.gcp_project: Optional[str] = None
        self.gcp_credentials: Optional[str] = None  # Path to service account JSON
        self.azure_account: Optional[str] = None  # Extracted from abfss:// URL
        self.azure_access_key: Optional[str] = None  # Storage account access key
        self.s3_endpoint: Optional[str] = None  # Custom S3 endpoint (--endpoint-url)
        self.s3_scheme: str = 's3'  # URL scheme the user typed: 's3' or 'r2'

    def reset(self) -> None:
        """Reset all configuration to defaults. Useful for testing."""
        self.aws_profile = None
        self.gcp_project = None
        self.gcp_credentials = None
        self.azure_account = None
        self.azure_access_key = None
        self.s3_endpoint = None
        self.s3_scheme = 's3'

    def resolve_s3_endpoint(self) -> Optional[str]:
        """The effective custom S3 endpoint, if any.

        Precedence: --endpoint-url (or config-file key) > the standard AWS
        endpoint env vars (which boto3 honors natively, but pyarrow and the
        lakehouse engines do not — so cloudcat resolves them explicitly).
        """
        return (
            self.s3_endpoint
            or os.environ.get('AWS_ENDPOINT_URL_S3')
            or os.environ.get('AWS_ENDPOINT_URL')
        )


# Global configuration instance
cloud_config = CloudConfig()


# Patterns for files to skip when scanning directories
SKIP_PATTERNS = [
    r'_SUCCESS$',
    r'\.crc$',
    r'\.committed$',
    r'\.pending$',
    r'_metadata$'
]

# Compression extensions
COMPRESSION_EXTENSIONS = ['.gz', '.gzip', '.zst', '.zstd', '.lz4', '.snappy', '.bz2']

# Compression suffix pattern for matching compressed files
_COMPRESSION_SUFFIX = r'(\.gz|\.gzip|\.zst|\.zstd|\.lz4|\.snappy|\.bz2)?$'

# Format extension mappings (includes optional compression extensions)
FORMAT_EXTENSION_MAP = {
    'csv': r'\.csv' + _COMPRESSION_SUFFIX,
    'json': r'\.(json|jsonl|ndjson)' + _COMPRESSION_SUFFIX,
    'parquet': r'\.parquet' + _COMPRESSION_SUFFIX,
    'avro': r'\.avro' + _COMPRESSION_SUFFIX,
    'orc': r'\.orc' + _COMPRESSION_SUFFIX,
    'text': r'\.(txt|log)' + _COMPRESSION_SUFFIX
}
