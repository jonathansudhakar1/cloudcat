#!/usr/bin/env python
"""CloudCat CLI - Preview and analyze data files in cloud storage."""

import click
import pandas as pd
import sys
import io
import json
import re
import tempfile
from typing import Optional, Tuple, List

from colorama import init, Fore, Style

# Initialize colorama
init()

# Import version
from . import __version__

# Import from modular components
from .config import cloud_config, SKIP_PATTERNS, FORMAT_EXTENSION_MAP
from .compression import detect_compression, decompress_stream, strip_compression_extension
from .filtering import parse_where_clause, apply_where_filter
from .formatters import colorize_json, format_table_with_colored_header
from .storage import (
    parse_cloud_path,
    get_stream,
    list_directory,
)
from .storage.gcs import get_gcs_stream, list_gcs_directory
from .storage.s3 import get_s3_stream, list_s3_directory
from .storage.azure import get_azure_stream, list_azure_directory
from .readers import (
    read_csv_data,
    read_json_data,
    read_parquet_data,
    read_avro_data,
    read_orc_data,
    read_text_data,
    HAS_PARQUET,
    HAS_AVRO,
    HAS_ORC,
)

# Import pyarrow for parquet metadata if available
try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None


def get_files_for_multiread(
    service: str,
    bucket: str,
    prefix: str,
    input_format: Optional[str] = None,
    max_size_mb: int = 25
) -> List[Tuple[str, int]]:
    """Get a list of files to read up to max_size_mb.

    Args:
        service: Cloud service identifier.
        bucket: Bucket or container name.
        prefix: Directory prefix.
        input_format: Optional format filter.
        max_size_mb: Maximum total size in MB.

    Returns:
        List of (filename, size) tuples.

    Raises:
        ValueError: If no suitable files are found.
    """
    files = list_directory(service, bucket, prefix)

    if not files:
        raise ValueError(f"No files found in {service}://{bucket}/{prefix}")

    # Filter files by size > 0
    non_empty_files = [f for f in files if f[1] > 0]

    if not non_empty_files:
        raise ValueError(f"No non-empty files found in {service}://{bucket}/{prefix}")

    # Skip common metadata files
    non_metadata_files = []

    for file_name, file_size in non_empty_files:
        # Skip if the file matches any of the patterns to ignore
        if not any(re.search(pattern, file_name) for pattern in SKIP_PATTERNS):
            non_metadata_files.append((file_name, file_size))

    # If no non-metadata files found, use all non-empty files
    if not non_metadata_files:
        click.echo(Fore.YELLOW + "Only found metadata files, using all non-empty files." + Style.RESET_ALL)
        filtered_files = non_empty_files
    else:
        filtered_files = non_metadata_files

    # Filter by input format if specified
    if input_format:
        format_regex = FORMAT_EXTENSION_MAP.get(input_format, None)
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


def find_first_non_empty_file(
    service: str,
    bucket: str,
    prefix: str,
    input_format: Optional[str] = None
) -> str:
    """Find the first non-empty file in a directory that matches the input format.

    Args:
        service: Cloud service identifier.
        bucket: Bucket or container name.
        prefix: Directory prefix.
        input_format: Optional format filter.

    Returns:
        File path of the first suitable file.

    Raises:
        ValueError: If no suitable files are found.
    """
    files = list_directory(service, bucket, prefix)

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
        format_regex = FORMAT_EXTENSION_MAP.get(input_format, None)
        if format_regex:
            matching_files = [f for f in non_empty_files if re.search(format_regex, f[0], re.IGNORECASE)]
            if matching_files:
                # Use the first matching file
                selected_file = matching_files[0]
                click.echo(Fore.BLUE + f"Selected file: {selected_file[0]} ({selected_file[1]} bytes)" + Style.RESET_ALL)
                return selected_file[0]

    # If no input_format specified or no matching files found, use the first non-empty file
    # Skip common metadata files
    for file_name, file_size in non_empty_files:
        # Skip if the file matches any of the patterns to ignore
        if not any(re.search(pattern, file_name) for pattern in SKIP_PATTERNS):
            click.echo(Fore.BLUE + f"Selected file: {file_name} ({file_size} bytes)" + Style.RESET_ALL)
            return file_name

    # If all files are skipped, use the first non-empty file anyway
    selected_file = non_empty_files[0]
    click.echo(Fore.YELLOW + f"Only found metadata files, using: {selected_file[0]} ({selected_file[1]} bytes)" + Style.RESET_ALL)
    return selected_file[0]


def detect_format_from_path(path: str) -> str:
    """Detect file format from file extension, handling compressed files.

    Args:
        path: File path to detect format from.

    Returns:
        Format string ('csv', 'json', 'parquet', 'avro', 'orc', 'text').

    Raises:
        ValueError: If format cannot be determined.
    """
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


def read_data_from_multiple_files(
    service: str,
    bucket: str,
    file_list: List[Tuple[str, int]],
    input_format: str,
    num_rows: int,
    columns: Optional[str] = None,
    delimiter: Optional[str] = None,
    offset: int = 0
) -> Tuple[pd.DataFrame, pd.Series, int]:
    """Read data from multiple files and concatenate the results.

    Args:
        service: Cloud service identifier.
        bucket: Bucket or container name.
        file_list: List of (filename, size) tuples.
        input_format: Data format.
        num_rows: Maximum rows to read.
        columns: Columns to select.
        delimiter: CSV delimiter.
        offset: Rows to skip.

    Returns:
        Tuple of (DataFrame, schema, total_rows).
    """
    dfs = []
    schemas = []
    rows_read = 0
    rows_skipped = 0
    total_rows = 0

    def process_file(file_info, remaining_to_skip, remaining_to_read):
        file_name, file_size = file_info
        click.echo(Fore.BLUE + f"Reading file: {file_name} ({file_size/1024:.1f} KB)" + Style.RESET_ALL)

        stream = get_stream(service, bucket, file_name)

        # Check for compression and decompress if needed
        compression = detect_compression(file_name)
        if compression:
            click.echo(Fore.BLUE + f"Detected {compression} compression, decompressing..." + Style.RESET_ALL)
            stream = decompress_stream(stream, compression)

        # Calculate how many rows to read from this file
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
            return pd.DataFrame(), pd.Series(dtype=object), total_rows
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


def read_data(
    service: str,
    bucket: str,
    object_path: str,
    input_format: str,
    num_rows: int,
    columns: Optional[str] = None,
    delimiter: Optional[str] = None,
    offset: int = 0
) -> Tuple[pd.DataFrame, pd.Series]:
    """Read data from cloud storage.

    Args:
        service: Cloud service identifier.
        bucket: Bucket or container name.
        object_path: Object path.
        input_format: Data format.
        num_rows: Maximum rows to read.
        columns: Columns to select.
        delimiter: CSV delimiter.
        offset: Rows to skip.

    Returns:
        Tuple of (DataFrame, schema).
    """
    # Get appropriate stream based on service
    stream = get_stream(service, bucket, object_path)

    # Check for compression and decompress if needed
    compression = detect_compression(object_path)
    if compression:
        click.echo(Fore.BLUE + f"Detected {compression} compression, decompressing..." + Style.RESET_ALL)
        stream = decompress_stream(stream, compression)

    # Calculate how many rows to read including offset
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


def get_record_count(
    service: str,
    bucket: str,
    object_path: str,
    input_format: str,
    delimiter: Optional[str] = None
):
    """Get record count from a file.

    Args:
        service: Cloud service identifier.
        bucket: Bucket or container name.
        object_path: Object path.
        input_format: Data format.
        delimiter: CSV delimiter.

    Returns:
        Record count (int) or "Unknown" on failure.
    """
    # Detect compression from file path
    compression = detect_compression(object_path)

    if input_format == 'parquet' and HAS_PARQUET:
        # For Parquet, we can get count from metadata
        stream = get_stream(service, bucket, object_path)
        if compression:
            stream = decompress_stream(stream, compression)

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
            except OSError:
                pass  # Ignore cleanup errors
    else:
        # For CSV and JSON, we need to count the rows
        click.echo(Fore.YELLOW + "Counting records (this might take a while for large files)..." + Style.RESET_ALL)

        stream = get_stream(service, bucket, object_path)
        if compression:
            stream = decompress_stream(stream, compression)

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
                lines = [line.strip() for line in content_stripped.split('\n') if line.strip()]
                if len(lines) > 1 and all(line.startswith('{') for line in lines[:min(5, len(lines))]):
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
            import fastavro
            reader = fastavro.reader(stream)
            count = sum(1 for _ in reader)
            return count
        elif input_format == 'orc':
            if not HAS_ORC:
                return "Unknown (pyarrow ORC not installed)"
            import pyarrow.orc as orc
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_path = temp_file.name
            try:
                if hasattr(stream, 'read'):
                    with open(temp_path, 'wb') as f:
                        f.write(stream.read())
                orc_file = orc.ORCFile(temp_path)
                return orc_file.nrows
            finally:
                import os
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass  # Ignore cleanup errors
        elif input_format == 'text':
            content = stream.read()
            if isinstance(content, bytes):
                content = content.decode('utf-8')
            return len(content.splitlines())

        return "Unknown"


@click.command()
@click.version_option(version=__version__, prog_name='cloudcat')
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
