#!/usr/bin/env python
import click
import pandas as pd
import sys
import io
import os
from tabulate import tabulate
from colorama import init, Fore, Style
import json
from urllib.parse import urlparse
import tempfile
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize colorama
init()

# For GCS
try:
    from google.cloud import storage as gcs
    HAS_GCS = True
except ImportError:
    HAS_GCS = False

# For S3
try:
    import boto3
    import botocore
    HAS_S3 = True
except ImportError:
    HAS_S3 = False

# For Parquet
try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False

# For Azure Blob Storage
try:
    from azure.storage.blob import BlobServiceClient, ContainerClient
    HAS_AZURE = True
except ImportError:
    HAS_AZURE = False

# For Avro
try:
    import fastavro
    HAS_AVRO = True
except ImportError:
    HAS_AVRO = False

# For ORC
try:
    import pyarrow.orc as orc
    HAS_ORC = True
except ImportError:
    HAS_ORC = False

# For compression support
import gzip
try:
    import lz4.frame as lz4
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False

try:
    import zstandard as zstd
    HAS_ZSTD = True
except ImportError:
    HAS_ZSTD = False

# Snappy support (used in Parquet, but we can also support .snappy files)
try:
    import snappy
    HAS_SNAPPY = True
except ImportError:
    HAS_SNAPPY = False


# Global configuration for cloud credentials
# This will be set by CLI options and used by all cloud functions
class CloudConfig:
    """Global configuration for cloud provider credentials."""
    aws_profile = None
    gcp_project = None
    gcp_credentials = None  # Path to service account JSON
    azure_account = None

cloud_config = CloudConfig()


def get_gcs_client():
    """Get a GCS client with optional project/credentials configuration."""
    if not HAS_GCS:
        sys.stderr.write(Fore.RED + "Error: google-cloud-storage package is required for GCS access.\n" +
                        "Install it with: pip install google-cloud-storage\n" + Style.RESET_ALL)
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


def get_s3_client():
    """Get an S3 client with optional profile configuration."""
    if not HAS_S3:
        sys.stderr.write(Fore.RED + "Error: boto3 package is required for S3 access.\n" +
                        "Install it with: pip install boto3\n" + Style.RESET_ALL)
        sys.exit(1)

    if cloud_config.aws_profile:
        session = boto3.Session(profile_name=cloud_config.aws_profile)
        return session.client('s3')
    else:
        return boto3.client('s3')


def get_azure_blob_service_client():
    """Get an Azure BlobServiceClient with optional account configuration."""
    if not HAS_AZURE:
        sys.stderr.write(Fore.RED + "Error: azure-storage-blob package is required for Azure access.\n" +
                        "Install it with: pip install azure-storage-blob\n" + Style.RESET_ALL)
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
            "Azure credentials not found. Use --azure-account, or set "
            "AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_URL environment variable."
        )


def detect_compression(path):
    """Detect compression type from file extension."""
    path_lower = path.lower()
    if path_lower.endswith('.gz') or path_lower.endswith('.gzip'):
        return 'gzip'
    elif path_lower.endswith('.zst') or path_lower.endswith('.zstd'):
        return 'zstd'
    elif path_lower.endswith('.lz4'):
        return 'lz4'
    elif path_lower.endswith('.snappy'):
        return 'snappy'
    elif path_lower.endswith('.bz2'):
        return 'bz2'
    return None


def decompress_stream(stream, compression):
    """Decompress a stream based on compression type."""
    if hasattr(stream, 'read'):
        data = stream.read()
    else:
        data = stream

    if compression == 'gzip':
        decompressed = gzip.decompress(data)
    elif compression == 'zstd':
        if not HAS_ZSTD:
            raise ValueError("zstandard package is required for .zst files. Install with: pip install zstandard")
        dctx = zstd.ZstdDecompressor()
        decompressed = dctx.decompress(data)
    elif compression == 'lz4':
        if not HAS_LZ4:
            raise ValueError("lz4 package is required for .lz4 files. Install with: pip install lz4")
        decompressed = lz4.decompress(data)
    elif compression == 'snappy':
        if not HAS_SNAPPY:
            raise ValueError("python-snappy package is required for .snappy files. Install with: pip install python-snappy")
        decompressed = snappy.decompress(data)
    elif compression == 'bz2':
        import bz2
        decompressed = bz2.decompress(data)
    else:
        return stream  # No compression

    return io.BytesIO(decompressed)


def strip_compression_extension(path):
    """Remove compression extension from path to get the actual file extension."""
    compression_exts = ['.gz', '.gzip', '.zst', '.zstd', '.lz4', '.snappy', '.bz2']
    path_lower = path.lower()
    for ext in compression_exts:
        if path_lower.endswith(ext):
            return path[:-len(ext)]
    return path


def parse_where_clause(where_clause):
    """Parse a simple WHERE clause into column, operator, and value.

    Supports: =, !=, <, >, <=, >=, contains, startswith, endswith
    Examples:
        "status=active"
        "age>30"
        "name contains john"
        "email endswith @gmail.com"
    """
    # Handle multi-word operators first
    for op in ['contains', 'startswith', 'endswith', 'not contains']:
        if f' {op} ' in where_clause.lower():
            parts = where_clause.lower().split(f' {op} ', 1)
            if len(parts) == 2:
                column = where_clause[:len(parts[0])].strip()
                value = where_clause[len(parts[0]) + len(op) + 2:].strip()
                # Remove quotes if present
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
                return column, op, value

    # Handle comparison operators
    for op in ['!=', '<=', '>=', '=', '<', '>']:
        if op in where_clause:
            parts = where_clause.split(op, 1)
            if len(parts) == 2:
                column = parts[0].strip()
                value = parts[1].strip()
                # Remove quotes if present
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
                return column, op, value

    raise ValueError(f"Invalid WHERE clause: {where_clause}. Use format: column=value, column>value, column contains value, etc.")


def apply_where_filter(df, where_clause):
    """Apply a WHERE filter to a DataFrame."""
    if not where_clause or df.empty:
        return df

    column, op, value = parse_where_clause(where_clause)

    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found. Available columns: {', '.join(df.columns)}")

    # Try to convert value to the column's type for comparison
    col_dtype = df[column].dtype
    try:
        if pd.api.types.is_numeric_dtype(col_dtype):
            value = float(value) if '.' in value else int(value)
        elif pd.api.types.is_bool_dtype(col_dtype):
            value = value.lower() in ('true', '1', 'yes')
    except (ValueError, TypeError):
        pass  # Keep as string

    # Apply the filter
    if op == '=':
        mask = df[column] == value
    elif op == '!=':
        mask = df[column] != value
    elif op == '<':
        mask = df[column] < value
    elif op == '>':
        mask = df[column] > value
    elif op == '<=':
        mask = df[column] <= value
    elif op == '>=':
        mask = df[column] >= value
    elif op == 'contains':
        mask = df[column].astype(str).str.contains(str(value), case=False, na=False)
    elif op == 'not contains':
        mask = ~df[column].astype(str).str.contains(str(value), case=False, na=False)
    elif op == 'startswith':
        mask = df[column].astype(str).str.startswith(str(value), na=False)
    elif op == 'endswith':
        mask = df[column].astype(str).str.endswith(str(value), na=False)
    else:
        raise ValueError(f"Unsupported operator: {op}")

    return df[mask]


def parse_cloud_path(path):
    """Parse a cloud storage path into service, bucket/container, and object components."""
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
        # For Azure: az://account/container/path or az://container/path (uses default account)
        # We'll support az://container/path format using environment variable for account
        bucket = parsed.netloc  # This is the container name
        object_path = parsed.path.lstrip('/')
    else:
        raise ValueError(f"Unsupported scheme: {parsed.scheme}. Use gcs://, s3://, or az://")

    return service, bucket, object_path


def list_gcs_directory(bucket_name, prefix):
    """List files in a GCS directory."""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    
    # Ensure prefix ends with / to indicate a directory
    if not prefix.endswith('/'):
        prefix = prefix + '/'
    
    blobs = bucket.list_blobs(prefix=prefix)
    
    # Return a list of files with their size
    return [(blob.name, blob.size) for blob in blobs if not blob.name.endswith('/')]


def list_s3_directory(bucket_name, prefix):
    """List files in an S3 directory."""
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
            file_list.extend([(item['Key'], item['Size']) for item in page['Contents']
                              if not item['Key'].endswith('/')])

    return file_list


def list_azure_directory(container_name, prefix):
    """List files in an Azure Blob Storage container directory."""
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


def get_files_for_multiread(service, bucket, prefix, input_format=None, max_size_mb=25):
    """Get a list of files to read up to max_size_mb."""
    if service == 'gcs':
        files = list_gcs_directory(bucket, prefix)
    elif service == 's3':
        files = list_s3_directory(bucket, prefix)
    elif service == 'azure':
        files = list_azure_directory(bucket, prefix)
    else:
        raise ValueError(f"Unsupported service: {service}")
    
    if not files:
        raise ValueError(f"No files found in {service}://{bucket}/{prefix}")
    
    # Filter files by size > 0
    non_empty_files = [f for f in files if f[1] > 0]
    
    if not non_empty_files:
        raise ValueError(f"No non-empty files found in {service}://{bucket}/{prefix}")
    
    # Skip common metadata files
    skip_patterns = [r'_SUCCESS$', r'\.crc$', r'\.committed$', r'\.pending$', r'_metadata$']
    non_metadata_files = []
    
    for file_name, file_size in non_empty_files:
        # Skip if the file matches any of the patterns to ignore
        if not any(re.search(pattern, file_name) for pattern in skip_patterns):
            non_metadata_files.append((file_name, file_size))
    
    # If no non-metadata files found, use all non-empty files
    if not non_metadata_files:
        click.echo(Fore.YELLOW + "Only found metadata files, using all non-empty files." + Style.RESET_ALL)
        filtered_files = non_empty_files
    else:
        filtered_files = non_metadata_files
    
    # Filter by input format if specified
    if input_format:
        format_ext_map = {
            'csv': r'\.csv$',
            'json': r'\.(json|jsonl|ndjson)$',
            'parquet': r'\.parquet$',
            'avro': r'\.avro$',
            'orc': r'\.orc$',
            'text': r'\.(txt|log)$'
        }
        
        format_regex = format_ext_map.get(input_format, None)
        if format_regex:
            matching_files = [f for f in filtered_files if re.search(format_regex, f[0], re.IGNORECASE)]
            if matching_files:
                filtered_files = matching_files
            else:
                click.echo(Fore.YELLOW + f"No files matching format '{input_format}' found. Using all available files." + Style.RESET_ALL)
    
    # Sort by name for deterministic behavior
    filtered_files.sort(key=lambda x: x[0])
    
    # Select files up to max_size_mb
    max_size_bytes = max_size_mb * 1024 * 1024
    selected_files = []
    total_size = 0
    
    for file_name, file_size in filtered_files:
        selected_files.append((file_name, file_size))
        total_size += file_size
        
        if total_size >= max_size_bytes:
            break
    
    if not selected_files:
        raise ValueError(f"No suitable files found in {service}://{bucket}/{prefix}")
    
    # Report on selected files
    total_mb = total_size / (1024 * 1024)
    click.echo(Fore.BLUE + f"Reading {len(selected_files)} files totaling {total_mb:.2f} MB" + Style.RESET_ALL)
    
    return selected_files


def find_first_non_empty_file(service, bucket, prefix, input_format=None):
    """Find the first non-empty file in a directory that matches the input format."""
    if service == 'gcs':
        files = list_gcs_directory(bucket, prefix)
    elif service == 's3':
        files = list_s3_directory(bucket, prefix)
    elif service == 'azure':
        files = list_azure_directory(bucket, prefix)
    else:
        raise ValueError(f"Unsupported service: {service}")
    
    if not files:
        raise ValueError(f"No files found in {service}://{bucket}/{prefix}")
    
    # Filter files by size > 0
    non_empty_files = [f for f in files if f[1] > 0]
    
    if not non_empty_files:
        raise ValueError(f"No non-empty files found in {service}://{bucket}/{prefix}")
    
    # Sort by name to ensure deterministic behavior
    non_empty_files.sort(key=lambda x: x[0])
    
    # Filter by input format if specified
    if input_format:
        format_ext_map = {
            'csv': r'\.csv$',
            'json': r'\.(json|jsonl|ndjson)$',
            'parquet': r'\.parquet$',
            'avro': r'\.avro$',
            'orc': r'\.orc$',
            'text': r'\.(txt|log)$'
        }
        
        format_regex = format_ext_map.get(input_format, None)
        if format_regex:
            matching_files = [f for f in non_empty_files if re.search(format_regex, f[0], re.IGNORECASE)]
            if matching_files:
                # Use the first matching file
                selected_file = matching_files[0]
                click.echo(Fore.BLUE + f"Selected file: {selected_file[0]} ({selected_file[1]} bytes)" + Style.RESET_ALL)
                return selected_file[0]
    
    # If no input_format specified or no matching files found, use the first non-empty file
    # Skip common metadata files
    skip_patterns = [r'_SUCCESS$', r'\.crc$', r'\.committed$', r'\.pending$', r'_metadata$']
    
    for file_name, file_size in non_empty_files:
        # Skip if the file matches any of the patterns to ignore
        if not any(re.search(pattern, file_name) for pattern in skip_patterns):
            click.echo(Fore.BLUE + f"Selected file: {file_name} ({file_size} bytes)" + Style.RESET_ALL)
            return file_name
    
    # If all files are skipped, use the first non-empty file anyway
    selected_file = non_empty_files[0]
    click.echo(Fore.YELLOW + f"Only found metadata files, using: {selected_file[0]} ({selected_file[1]} bytes)" + Style.RESET_ALL)
    return selected_file[0]


def detect_format_from_path(path):
    """Detect file format from file extension, handling compressed files."""
    # Strip compression extension first to get actual file format
    base_path = strip_compression_extension(path)
    path_lower = base_path.lower()

    if path_lower.endswith('.json') or path_lower.endswith('.jsonl') or path_lower.endswith('.ndjson'):
        return 'json'
    elif path_lower.endswith('.csv'):
        return 'csv'
    elif path_lower.endswith('.parquet'):
        return 'parquet'
    elif path_lower.endswith('.avro'):
        return 'avro'
    elif path_lower.endswith('.orc'):
        return 'orc'
    elif path_lower.endswith('.txt') or path_lower.endswith('.log'):
        return 'text'
    else:
        raise ValueError(f"Could not infer format from path: {path}. Please specify --input-format.")


def get_gcs_stream(bucket_name, object_name):
    """Get a file stream from GCS with minimal downloading."""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    
    # Create a streaming buffer
    buffer = io.BytesIO()
    blob.download_to_file(buffer)
    buffer.seek(0)
    
    return buffer


def get_s3_stream(bucket_name, object_name):
    """Get a file stream from S3."""
    s3 = get_s3_client()
    response = s3.get_object(Bucket=bucket_name, Key=object_name)
    return response['Body']


def get_azure_stream(container_name, blob_name):
    """Get a file stream from Azure Blob Storage."""
    blob_service_client = get_azure_blob_service_client()
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Download blob to a BytesIO buffer
    buffer = io.BytesIO()
    download_stream = blob_client.download_blob()
    buffer.write(download_stream.readall())
    buffer.seek(0)

    return buffer


def read_csv_data(stream, num_rows, columns=None, delimiter=None):
    """Read CSV data from a stream."""
    # First read the data without column filtering to get full schema
    pd_args = {'nrows': num_rows} if num_rows > 0 else {}
    
    # Add delimiter if specified
    if delimiter:
        pd_args['delimiter'] = delimiter
    
    full_df = pd.read_csv(stream, **pd_args)
    
    # Store the full schema for later use
    full_schema = full_df.dtypes
    
    # Apply column filtering if specified
    if columns:
        cols = [c.strip() for c in columns.split(',')]
        valid_cols = [c for c in cols if c in full_df.columns]
        if len(valid_cols) != len(cols):
            missing = set(cols) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        df = full_df[valid_cols]
    else:
        df = full_df
    
    # Return both the filtered dataframe and the full schema
    return df, full_schema


def read_json_data(stream, num_rows, columns=None):
    """Read JSON data from a stream. Supports both JSON Lines and regular JSON formats."""
    # Read the raw content to determine the format
    if hasattr(stream, 'read'):
        content = stream.read()
        if isinstance(content, bytes):
            content = content.decode('utf-8')
    else:
        content = stream

    # Try to detect if it's JSON Lines or regular JSON
    content_stripped = content.strip()
    is_json_lines = False

    if content_stripped:
        # JSON Lines starts with { or [ on each line, not a single array/object
        # Check if first non-whitespace char is { and there are multiple lines with {
        first_char = content_stripped[0]
        if first_char == '{':
            # Could be JSON Lines or a single JSON object
            # Check if there are multiple lines starting with {
            lines = [l.strip() for l in content_stripped.split('\n') if l.strip()]
            if len(lines) > 1 and all(l.startswith('{') for l in lines[:min(5, len(lines))]):
                is_json_lines = True
        elif first_char == '[':
            # Regular JSON array
            is_json_lines = False

    # Create a new stream from the content for pandas
    content_stream = io.StringIO(content)

    try:
        if is_json_lines:
            # JSON Lines format
            if num_rows > 0:
                full_df = pd.read_json(content_stream, lines=True, nrows=num_rows)
            else:
                full_df = pd.read_json(content_stream, lines=True)
        else:
            # Regular JSON (array or object)
            parsed = json.loads(content)

            # Handle different JSON structures
            if isinstance(parsed, list):
                # JSON array - convert to dataframe
                full_df = pd.DataFrame(parsed)
            elif isinstance(parsed, dict):
                # Single JSON object - treat as single row
                full_df = pd.DataFrame([parsed])
            else:
                raise ValueError("JSON must be an array or object")

            # Apply num_rows limit
            if num_rows > 0 and len(full_df) > num_rows:
                full_df = full_df.head(num_rows)
    except json.JSONDecodeError:
        # Fall back to trying JSON Lines if regular JSON parsing fails
        content_stream = io.StringIO(content)
        if num_rows > 0:
            full_df = pd.read_json(content_stream, lines=True, nrows=num_rows)
        else:
            full_df = pd.read_json(content_stream, lines=True)

    # Store the full schema for later use
    full_schema = full_df.dtypes

    # Apply column filtering if specified
    if columns:
        cols = [c.strip() for c in columns.split(',')]
        valid_cols = [c for c in cols if c in full_df.columns]
        if len(valid_cols) != len(cols):
            missing = set(cols) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        df = full_df[valid_cols]
    else:
        df = full_df

    # Return both the filtered dataframe and the full schema
    return df, full_schema


def read_parquet_data(stream, num_rows, columns=None):
    """Read Parquet data from a stream."""
    if not HAS_PARQUET:
        sys.stderr.write(Fore.RED + "Error: pyarrow package is required for Parquet support.\n" + 
                         "Install it with: pip install pyarrow\n" + Style.RESET_ALL)
        sys.exit(1)
    
    # For Parquet, we need a temporary file to properly read the metadata
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_path = temp_file.name
    
    try:
        # If stream is a file-like object, copy to temp file
        if hasattr(stream, 'read'):
            with open(temp_path, 'wb') as f:
                f.write(stream.read())
        else:
            # Assume it's already a path
            temp_path = stream
        
        parquet_file = pq.ParquetFile(temp_path)
        
        # Read the full schema first
        full_schema = parquet_file.schema_arrow
        
        # Extract columns if specified for filtering
        col_names = columns.split(',') if columns else None
        
        # Read the data efficiently
        if num_rows > 0:
            tables = []
            rows_read = 0
            
            for i in range(parquet_file.num_row_groups):
                if rows_read >= num_rows:
                    break
                
                table = parquet_file.read_row_group(i, columns=col_names)
                
                # Limit rows if needed for the last batch
                if rows_read + table.num_rows > num_rows:
                    table = table.slice(0, num_rows - rows_read)
                
                tables.append(table)
                rows_read += min(table.num_rows, num_rows - rows_read)
            
            if tables:
                result_table = pa.concat_tables(tables)
                df = result_table.to_pandas()
            else:
                df = pd.DataFrame()
        else:
            # Read all data (potentially with column filtering)
            table = parquet_file.read(columns=col_names)
            df = table.to_pandas()
        
        # Get the full schema as a pandas Series for consistency with other formats
        # First, read a small sample to get pandas dtypes - use read_row_group to limit rows
        # FIXED: PyArrow doesn't have nrows parameter for read_table
        if parquet_file.num_row_groups > 0:
            # Read just first row group
            sample_table = parquet_file.read_row_group(0)
            if sample_table.num_rows > 1:
                # Slice to just the first row if needed
                sample_table = sample_table.slice(0, 1)
            full_df = sample_table.to_pandas()
        else:
            # If no row groups, create empty DataFrame with correct schema
            full_df = df.iloc[0:0] if not df.empty else pd.DataFrame()
        
        full_schema = full_df.dtypes
        
        return df, full_schema
    
    finally:
        # Clean up the temporary file
        import os
        try:
            if hasattr(stream, 'read'):  # Only delete if we created temp file
                os.unlink(temp_path)
        except:
            pass


def read_avro_data(stream, num_rows, columns=None):
    """Read Avro data from a stream."""
    if not HAS_AVRO:
        sys.stderr.write(Fore.RED + "Error: fastavro package is required for Avro support.\n" +
                         "Install it with: pip install fastavro\n" + Style.RESET_ALL)
        sys.exit(1)

    # Read the Avro file
    if hasattr(stream, 'read'):
        reader = fastavro.reader(stream)
    else:
        with open(stream, 'rb') as f:
            reader = fastavro.reader(f)

    # Read records into a list
    records = []
    for i, record in enumerate(reader):
        if num_rows > 0 and i >= num_rows:
            break
        records.append(record)

    # Convert to DataFrame
    if records:
        full_df = pd.DataFrame(records)
    else:
        full_df = pd.DataFrame()

    # Store the full schema for later use
    full_schema = full_df.dtypes

    # Apply column filtering if specified
    if columns and not full_df.empty:
        cols = [c.strip() for c in columns.split(',')]
        valid_cols = [c for c in cols if c in full_df.columns]
        if len(valid_cols) != len(cols):
            missing = set(cols) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        df = full_df[valid_cols]
    else:
        df = full_df

    return df, full_schema


def read_orc_data(stream, num_rows, columns=None):
    """Read ORC data from a stream."""
    if not HAS_ORC:
        sys.stderr.write(Fore.RED + "Error: pyarrow with ORC support is required.\n" +
                         "Install it with: pip install pyarrow\n" + Style.RESET_ALL)
        sys.exit(1)

    # For ORC, we need a temporary file
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_path = temp_file.name

    try:
        # If stream is a file-like object, copy to temp file
        if hasattr(stream, 'read'):
            with open(temp_path, 'wb') as f:
                f.write(stream.read())
        else:
            temp_path = stream

        # Read the ORC file
        orc_file = orc.ORCFile(temp_path)

        # Extract columns if specified
        col_names = [c.strip() for c in columns.split(',')] if columns else None

        # Read data
        table = orc_file.read(columns=col_names)

        # Convert to pandas
        full_df = table.to_pandas()

        # Apply num_rows limit
        if num_rows > 0 and len(full_df) > num_rows:
            full_df = full_df.head(num_rows)

        # Get full schema (read without column filter for schema)
        if columns:
            full_table = orc_file.read()
            full_schema = full_table.to_pandas().dtypes
        else:
            full_schema = full_df.dtypes

        return full_df, full_schema

    finally:
        try:
            if hasattr(stream, 'read'):
                os.unlink(temp_path)
        except:
            pass


def read_text_data(stream, num_rows, columns=None):
    """Read plain text data from a stream, treating each line as a row."""
    # Read the content
    if hasattr(stream, 'read'):
        content = stream.read()
        if isinstance(content, bytes):
            content = content.decode('utf-8')
    else:
        with open(stream, 'r') as f:
            content = f.read()

    # Split into lines
    lines = content.splitlines()

    # Apply num_rows limit
    if num_rows > 0:
        lines = lines[:num_rows]

    # Create DataFrame with a single 'line' column
    full_df = pd.DataFrame({'line': lines, 'line_number': range(1, len(lines) + 1)})

    # Store the full schema
    full_schema = full_df.dtypes

    # Apply column filtering if specified
    if columns:
        cols = [c.strip() for c in columns.split(',')]
        valid_cols = [c for c in cols if c in full_df.columns]
        if len(valid_cols) != len(cols):
            missing = set(cols) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        df = full_df[valid_cols]
    else:
        df = full_df

    return df, full_schema


def read_data_from_multiple_files(service, bucket, file_list, input_format, num_rows, columns=None, delimiter=None, offset=0):
    """Read data from multiple files and concatenate the results."""
    dfs = []
    schemas = []
    rows_read = 0
    rows_skipped = 0
    total_rows = 0

    # Calculate how many rows we need to read total (including offset)
    total_rows_needed = (offset + num_rows) if num_rows > 0 else 0

    # Define a function to process each file
    def process_file(file_info, remaining_to_skip, remaining_to_read):
        file_name, file_size = file_info
        click.echo(Fore.BLUE + f"Reading file: {file_name} ({file_size/1024:.1f} KB)" + Style.RESET_ALL)

        if service == 'gcs':
            stream = get_gcs_stream(bucket, file_name)
        elif service == 's3':
            stream = get_s3_stream(bucket, file_name)
        elif service == 'azure':
            stream = get_azure_stream(bucket, file_name)
        else:
            raise ValueError(f"Unsupported service: {service}")

        # Check for compression and decompress if needed
        compression = detect_compression(file_name)
        if compression:
            click.echo(Fore.BLUE + f"Detected {compression} compression, decompressing..." + Style.RESET_ALL)
            stream = decompress_stream(stream, compression)

        # Calculate how many rows to read from this file
        # We need to read enough to cover offset + remaining rows needed
        rows_to_read_from_file = (remaining_to_skip + remaining_to_read) if remaining_to_read > 0 else 0

        # Read the file
        if input_format == 'csv':
            df, schema = read_csv_data(stream, rows_to_read_from_file if rows_to_read_from_file > 0 else 0, columns, delimiter)
        elif input_format == 'json':
            df, schema = read_json_data(stream, rows_to_read_from_file if rows_to_read_from_file > 0 else 0, columns)
        elif input_format == 'parquet':
            df, schema = read_parquet_data(stream, rows_to_read_from_file if rows_to_read_from_file > 0 else 0, columns)
        elif input_format == 'avro':
            df, schema = read_avro_data(stream, rows_to_read_from_file if rows_to_read_from_file > 0 else 0, columns)
        elif input_format == 'orc':
            df, schema = read_orc_data(stream, rows_to_read_from_file if rows_to_read_from_file > 0 else 0, columns)
        elif input_format == 'text':
            df, schema = read_text_data(stream, rows_to_read_from_file if rows_to_read_from_file > 0 else 0, columns)
        else:
            raise ValueError(f"Unsupported format: {input_format}")

        return df, schema, len(df)
    
    # Process files in order until we have enough rows
    remaining_offset = offset
    remaining_rows = num_rows if num_rows > 0 else float('inf')

    for file_info in file_list:
        try:
            df, schema, file_rows = process_file(file_info, remaining_offset, remaining_rows if remaining_rows != float('inf') else 0)

            if not df.empty:
                total_rows += len(df)

                # Handle offset: skip rows from the beginning
                if remaining_offset > 0:
                    if remaining_offset >= len(df):
                        # Skip entire file
                        remaining_offset -= len(df)
                        rows_skipped += len(df)
                        schemas.append(schema)  # Still track schema
                        continue
                    else:
                        # Skip partial rows from this file
                        df = df.iloc[remaining_offset:]
                        rows_skipped += remaining_offset
                        remaining_offset = 0

                dfs.append(df)
                schemas.append(schema)
                rows_read += len(df)

                # Stop if we've read enough rows
                if num_rows > 0 and rows_read >= num_rows:
                    break
        except Exception as e:
            click.echo(Fore.YELLOW + f"Warning: Error reading file {file_info[0]}: {str(e)}" + Style.RESET_ALL)

    if not dfs:
        if rows_skipped > 0:
            click.echo(Fore.YELLOW + f"Warning: Offset ({offset}) skipped all available rows." + Style.RESET_ALL)
            return pd.DataFrame(), pd.Series(), total_rows
        raise ValueError("No data could be read from any of the files")

    # Concatenate the dataframes
    result_df = pd.concat(dfs, ignore_index=True)

    # For the full schema, merge all schemas
    all_columns = {}
    for schema in schemas:
        for col, dtype in schema.items():
            if col in all_columns:
                # If the same column has different types, use object type
                if all_columns[col] != dtype:
                    all_columns[col] = 'object'
            else:
                all_columns[col] = dtype

    full_schema = pd.Series(all_columns)

    # If we read more rows than requested, truncate the result
    if num_rows > 0 and len(result_df) > num_rows:
        result_df = result_df.iloc[:num_rows]

    return result_df, full_schema, total_rows


def read_data(service, bucket, object_path, input_format, num_rows, columns=None, delimiter=None, offset=0):
    """Read data from cloud storage."""
    # Get appropriate stream based on service
    if service == 'gcs':
        stream = get_gcs_stream(bucket, object_path)
    elif service == 's3':
        stream = get_s3_stream(bucket, object_path)
    elif service == 'azure':
        stream = get_azure_stream(bucket, object_path)
    else:
        raise ValueError(f"Unsupported service: {service}")

    # Check for compression and decompress if needed
    compression = detect_compression(object_path)
    if compression:
        click.echo(Fore.BLUE + f"Detected {compression} compression, decompressing..." + Style.RESET_ALL)
        stream = decompress_stream(stream, compression)

    # Calculate how many rows to read including offset
    # We need to read offset + num_rows to then skip the first offset rows
    rows_to_read = (offset + num_rows) if num_rows > 0 else 0

    # Read based on format
    if input_format == 'csv':
        df, schema = read_csv_data(stream, rows_to_read, columns, delimiter)
    elif input_format == 'json':
        df, schema = read_json_data(stream, rows_to_read, columns)
    elif input_format == 'parquet':
        df, schema = read_parquet_data(stream, rows_to_read, columns)
    elif input_format == 'avro':
        df, schema = read_avro_data(stream, rows_to_read, columns)
    elif input_format == 'orc':
        df, schema = read_orc_data(stream, rows_to_read, columns)
    elif input_format == 'text':
        df, schema = read_text_data(stream, rows_to_read, columns)
    else:
        raise ValueError(f"Unsupported format: {input_format}")

    # Apply offset - skip first N rows
    if offset > 0 and not df.empty:
        if offset >= len(df):
            click.echo(Fore.YELLOW + f"Warning: Offset ({offset}) >= total rows read ({len(df)}). No data to display." + Style.RESET_ALL)
            df = df.iloc[0:0]  # Empty dataframe with same schema
        else:
            df = df.iloc[offset:].reset_index(drop=True)

    return df, schema


def get_record_count(service, bucket, object_path, input_format, delimiter=None):
    """Get record count from a file."""
    if input_format == 'parquet' and HAS_PARQUET:
        # For Parquet, we can get count from metadata
        if service == 'gcs':
            stream = get_gcs_stream(bucket, object_path)
        elif service == 's3':
            stream = get_s3_stream(bucket, object_path)
        elif service == 'azure':
            stream = get_azure_stream(bucket, object_path)
        
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            if hasattr(stream, 'read'):
                with open(temp_path, 'wb') as f:
                    f.write(stream.read())
            else:
                temp_path = stream
            
            parquet_file = pq.ParquetFile(temp_path)
            return parquet_file.metadata.num_rows
        finally:
            import os
            try:
                if hasattr(stream, 'read'):
                    os.unlink(temp_path)
            except:
                pass
    else:
        # For CSV and JSON, we need to count the rows
        click.echo(Fore.YELLOW + "Counting records (this might take a while for large files)..." + Style.RESET_ALL)

        # Use pandas to count rows in chunks
        if service == 'gcs':
            stream = get_gcs_stream(bucket, object_path)
        elif service == 's3':
            stream = get_s3_stream(bucket, object_path)
        elif service == 'azure':
            stream = get_azure_stream(bucket, object_path)

        if input_format == 'csv':
            chunk_count = 0
            
            # Add delimiter if specified
            read_args = {'chunksize': 10000}
            if delimiter:
                read_args['delimiter'] = delimiter
                
            for chunk in pd.read_csv(stream, **read_args):
                chunk_count += len(chunk)
            return chunk_count
        elif input_format == 'json':
            # Read content to detect format
            content = stream.read()
            if isinstance(content, bytes):
                content = content.decode('utf-8')

            content_stripped = content.strip()
            if not content_stripped:
                return 0

            # Check if it's JSON Lines or regular JSON
            first_char = content_stripped[0]
            if first_char == '[':
                # Regular JSON array
                parsed = json.loads(content)
                return len(parsed) if isinstance(parsed, list) else 1
            elif first_char == '{':
                # Could be JSON Lines or single object
                lines = [l.strip() for l in content_stripped.split('\n') if l.strip()]
                if len(lines) > 1 and all(l.startswith('{') for l in lines[:min(5, len(lines))]):
                    # JSON Lines - count lines
                    return len(lines)
                else:
                    # Single JSON object
                    return 1
            else:
                # Try JSON Lines as fallback
                content_stream = io.StringIO(content)
                chunk_count = 0
                for chunk in pd.read_json(content_stream, lines=True, chunksize=10000):
                    chunk_count += len(chunk)
                return chunk_count
        elif input_format == 'avro':
            if not HAS_AVRO:
                return "Unknown (fastavro not installed)"
            reader = fastavro.reader(stream)
            count = sum(1 for _ in reader)
            return count
        elif input_format == 'orc':
            if not HAS_ORC:
                return "Unknown (pyarrow ORC not installed)"
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_path = temp_file.name
            try:
                if hasattr(stream, 'read'):
                    with open(temp_path, 'wb') as f:
                        f.write(stream.read())
                orc_file = orc.ORCFile(temp_path)
                return orc_file.nrows
            finally:
                try:
                    os.unlink(temp_path)
                except:
                    pass
        elif input_format == 'text':
            content = stream.read()
            if isinstance(content, bytes):
                content = content.decode('utf-8')
            return len(content.splitlines())

        return "Unknown"


def colorize_json(json_str):
    """Add colors to JSON for better readability."""
    # Parse the JSON
    parsed = json.loads(json_str)
    
    # Convert to a colored string
    result = []
    
    for item in parsed:
        item_parts = []
        item_parts.append('{')
        
        for i, (key, value) in enumerate(item.items()):
            # Format key
            key_str = f'  {Fore.BLUE}"{key}"{Style.RESET_ALL}: '
            
            # Format value based on type
            if isinstance(value, str):
                val_str = f'{Fore.GREEN}"{value}"{Style.RESET_ALL}'
            elif isinstance(value, (int, float)):
                val_str = f'{Fore.CYAN}{value}{Style.RESET_ALL}'
            elif value is None:
                val_str = f'{Fore.RED}null{Style.RESET_ALL}'
            elif isinstance(value, bool):
                val_str = f'{Fore.YELLOW}{str(value).lower()}{Style.RESET_ALL}'
            else:
                # For complex types, just convert to string
                val_str = f'{json.dumps(value)}'
            
            # Add comma if not the last item
            if i < len(item) - 1:
                item_parts.append(f"{key_str}{val_str},")
            else:
                item_parts.append(f"{key_str}{val_str}")
        
        item_parts.append('}')
        result.append('\n'.join(item_parts))
    
    return '\n'.join(result)


def format_table_with_colored_header(df):
    """Format a dataframe as a table with colored and bold headers."""
    if df.empty:
        return "Empty dataset"
    
    # Get the column headers and format them
    headers = [f"{Fore.CYAN}{Style.BRIGHT}{col}{Style.RESET_ALL}" for col in df.columns]
    
    # Convert the dataframe to a list of lists for tabulate
    data = df.values.tolist()
    
    # Use tabulate with the formatted headers
    return tabulate(data, headers, tablefmt='psql')


@click.command()
@click.option('--path', '-p', required=True, help='Path to the file or directory (gcs://, s3://, or az://)')
@click.option('--output-format', '-o', type=click.Choice(['json', 'jsonp', 'csv', 'table']), default='table',
              help='Output format (default: table)')
@click.option('--input-format', '-i', type=click.Choice(['json', 'csv', 'parquet', 'avro', 'orc', 'text']),
              help='Input format (default: inferred from path)')
@click.option('--columns', '-c', help='Comma-separated list of columns to display (default: all)')
@click.option('--num-rows', '-n', default=10, type=int, help='Number of rows to display (default: 10)')
@click.option('--offset', default=0, type=int, help='Skip first N rows (default: 0)')
@click.option('--where', '-w', help='Filter rows (e.g., "status=active", "age>30", "name contains john")')
@click.option('--schema', '-s', type=click.Choice(['show', 'dont_show', 'schema_only']), default='show',
              help='Schema display option (default: show)')
@click.option('--no-count', is_flag=True, help='Disable record count display')
@click.option('--multi-file-mode', '-m', type=click.Choice(['first', 'auto', 'all']), default='auto',
              help='How to handle directories with multiple files (default: auto)')
@click.option('--max-size-mb', default=25, type=int,
              help='Maximum size in MB to read when reading multiple files (default: 25)')
@click.option('--delimiter', '-d', help='Delimiter to use for CSV files (use "\\t" for tab)')
@click.option('--profile', help='AWS profile name (for S3 access)')
@click.option('--project', help='GCP project ID (for GCS access)')
@click.option('--credentials', help='Path to GCP service account JSON file')
@click.option('--account', help='Azure storage account name')
def main(path, output_format, input_format, columns, num_rows, offset, where, schema, no_count,
         multi_file_mode, max_size_mb, delimiter, profile, project, credentials, account):
    """Display data from files in Google Cloud Storage, AWS S3, or Azure Blob Storage.

    Supported formats: CSV, JSON, Parquet, Avro, ORC, and plain text.
    Supports compressed files: .gz, .zst, .lz4, .snappy, .bz2

    Example usage:

    \b
    # Read from GCS
    cloudcat --path gcs://my-bucket/data.csv --output-format table

    \b
    # Read from S3 with column selection
    cloudcat --path s3://my-bucket/data.parquet --columns id,name,value

    \b
    # Read from Azure Blob Storage
    cloudcat --path az://my-container/data.json --output-format jsonp

    \b
    # Read Avro files from Kafka exports
    cloudcat --path s3://my-bucket/kafka-export.avro

    \b
    # Read ORC files from Hive
    cloudcat --path gcs://my-bucket/hive-table.orc

    \b
    # Read log files as plain text
    cloudcat --path az://logs/app.log --input-format text

    \b
    # Read from a directory (reads first non-empty data file)
    cloudcat --path gcs://my-bucket/sparkoutput/ --input-format parquet

    \b
    # Read from multiple files in a directory (up to 25MB)
    cloudcat --path s3://my-bucket/daily-data/ --multi-file-mode all --max-size-mb 25

    \b
    # Read a tab-delimited file
    cloudcat --path gcs://my-bucket/data.csv --delimiter "\\t"

    \b
    # Skip first 100 rows and show next 10
    cloudcat --path gcs://my-bucket/data.csv --offset 100 --num-rows 10

    \b
    # Filter rows with WHERE clause
    cloudcat --path s3://bucket/users.parquet --where "status=active"
    cloudcat --path s3://bucket/events.json --where "age>30"
    cloudcat --path gcs://bucket/logs.csv --where "message contains error"

    \b
    # Read compressed files (auto-detected)
    cloudcat --path gcs://my-bucket/data.csv.gz
    cloudcat --path s3://my-bucket/logs.json.zst

    \b
    # Use AWS profile for S3 access
    cloudcat --path s3://my-bucket/data.csv --profile production

    \b
    # Use specific GCP project
    cloudcat --path gcs://my-bucket/data.csv --project my-gcp-project

    \b
    # Use GCP service account credentials
    cloudcat --path gcs://bucket/data.csv --credentials /path/to/service-account.json

    \b
    # Use specific Azure storage account
    cloudcat --path az://container/data.csv --account mystorageaccount
    """
    try:
        # Configure cloud credentials from CLI options
        if profile:
            cloud_config.aws_profile = profile
        if project:
            cloud_config.gcp_project = project
        if credentials:
            cloud_config.gcp_credentials = credentials
        if account:
            cloud_config.azure_account = account

        # Handle special characters in delimiter
        if delimiter == "\\t":
            delimiter = "\t"

        # Parse the path
        service, bucket, object_path = parse_cloud_path(path)
        
        # Check if path is a directory (ends with '/')
        is_directory = object_path.endswith('/')
        
        # Handle directory paths based on multi-file-mode
        if is_directory:
            click.echo(Fore.BLUE + f"Path is a directory" + Style.RESET_ALL)

            if multi_file_mode == 'first' or (multi_file_mode == 'auto' and max_size_mb <= 0):
                # Use a single file
                click.echo(Fore.BLUE + f"Looking for first suitable file..." + Style.RESET_ALL)
                object_path = find_first_non_empty_file(service, bucket, object_path, input_format)

                # Determine input format if not specified
                if not input_format:
                    input_format = detect_format_from_path(object_path)
                    click.echo(Fore.BLUE + f"Inferred input format: {input_format}" + Style.RESET_ALL)

                # Read the data from the single file
                df, full_schema = read_data(service, bucket, object_path, input_format, num_rows, columns, delimiter, offset)
                total_record_count = None  # Will be computed later if needed
            else:
                # Read from multiple files
                click.echo(Fore.BLUE + f"Reading multiple files (up to {max_size_mb}MB)..." + Style.RESET_ALL)

                # Determine input format if not specified (use the first file to infer)
                if not input_format:
                    first_file = find_first_non_empty_file(service, bucket, object_path)
                    input_format = detect_format_from_path(first_file)
                    click.echo(Fore.BLUE + f"Inferred input format from first file: {input_format}" + Style.RESET_ALL)

                # Get files to read
                file_list = get_files_for_multiread(service, bucket, object_path, input_format, max_size_mb)

                # Read data from multiple files
                df, full_schema, total_record_count = read_data_from_multiple_files(
                    service, bucket, file_list, input_format, num_rows, columns, delimiter, offset
                )

                # Update object_path for display/logging purposes
                object_path = f"{object_path} ({len(file_list)} files)"
        else:
            # Single file path
            # Determine input format if not specified
            if not input_format:
                input_format = detect_format_from_path(object_path)
                click.echo(Fore.BLUE + f"Inferred input format: {input_format}" + Style.RESET_ALL)

            # Read the data
            df, full_schema = read_data(service, bucket, object_path, input_format, num_rows, columns, delimiter, offset)
            total_record_count = None  # Will be computed later if needed

        # Apply WHERE filter if specified
        if where and not df.empty:
            original_count = len(df)
            df = apply_where_filter(df, where)
            filtered_count = len(df)
            click.echo(Fore.BLUE + f"Filtered: {filtered_count} of {original_count} rows match '{where}'" + Style.RESET_ALL)
        
        # Display schema if requested
        if schema in ['show', 'schema_only']:
            click.echo(Fore.GREEN + "Schema:" + Style.RESET_ALL)
            for col, dtype in full_schema.items():
                click.echo(f"  {col}: {dtype}")
            click.echo("")
        
        # Exit if only schema was requested
        if schema == 'schema_only':
            # Still show count even with schema_only unless --no-count is specified
            if not no_count:
                try:
                    if total_record_count is None:
                        total_record_count = get_record_count(service, bucket, object_path, input_format, delimiter)
                    click.echo(Fore.CYAN + f"Total records: {total_record_count}" + Style.RESET_ALL)
                except Exception as e:
                    click.echo(Fore.YELLOW + f"Could not count records: {str(e)}" + Style.RESET_ALL)
            return
        
        # Display the data
        if output_format == 'table':
            # Use our custom function for formatted table output
            click.echo(format_table_with_colored_header(df))
        elif output_format == 'jsonp':
            # Pretty print JSON with colors
            json_str = df.to_json(orient='records')
            pretty_json = json.dumps(json.loads(json_str), indent=2)
            # Apply colors for better readability
            click.echo(colorize_json(json_str))
        elif output_format == 'json':
            click.echo(df.to_json(orient='records', lines=True))
        elif output_format == 'csv':
            click.echo(df.to_csv(index=False))
        
        # Count records by default unless --no-count is specified
        if not no_count:
            try:
                if total_record_count is None:
                    total_record_count = get_record_count(service, bucket, object_path, input_format, delimiter)
                click.echo(Fore.CYAN + f"\nTotal records: {total_record_count}" + Style.RESET_ALL)
            except Exception as e:
                click.echo(Fore.YELLOW + f"\nCould not count records: {str(e)}" + Style.RESET_ALL)
    
    except Exception as e:
        click.echo(Fore.RED + f"Error: {str(e)}" + Style.RESET_ALL, err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()