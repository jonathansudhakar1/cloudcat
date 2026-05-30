#!/usr/bin/env python
"""CloudCat CLI - Preview and analyze data files in cloud storage."""

import click
import pandas as pd
import os
import sys
import io
import json
import re
import tempfile
from typing import Optional, Tuple, List

from colorama import init, Fore, Style

# Import version
from . import __version__

# Import from modular components
from .config import cloud_config, SKIP_PATTERNS, FORMAT_EXTENSION_MAP
from .compression import (
    detect_compression,
    decompress_stream,
    strip_compression_extension,
    get_streaming_decompressor,
    supports_streaming_decompression,
)
from .filtering import parse_where_clause, apply_where_filter
from .formatters import colorize_json, format_table_with_colored_header
from .progress import start_progress, update_progress, stop_progress
from .storage import (
    parse_cloud_path,
    get_stream,
    list_directory,
    get_file_size,
)
from .storage.gcs import get_gcs_stream, list_gcs_directory
from .storage.s3 import get_s3_stream, list_s3_directory
from .storage.azure import get_azure_stream, list_azure_directory
from .readers import (
    read_csv_data,
    read_csv_data_streaming,
    read_json_data,
    read_json_data_streaming,
    read_parquet_data,
    read_parquet_data_streaming,
    read_avro_data,
    read_avro_data_streaming,
    read_orc_data,
    read_orc_data_streaming,
    read_text_data,
    read_text_data_streaming,
    HAS_PARQUET,
    HAS_AVRO,
    HAS_ORC,
)
from .streaming import (
    StreamingStats,
    format_bytes,
    get_pyarrow_filesystem,
    supports_pyarrow_fs,
)

# Import pyarrow for parquet metadata if available
try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None


def info(message: str) -> None:
    """Print a diagnostic/status message to stderr.

    Keeping non-data output off stdout means piping or redirecting cloudcat
    (e.g. ``cloudcat ... > out.csv``) produces a clean data file.
    """
    click.echo(message, err=True)


def _configure_color(no_color: bool) -> None:
    """Initialize colorama, stripping ANSI codes when output is not a terminal.

    Color is enabled only when stdout is a TTY, ``--no-color`` was not passed,
    and the NO_COLOR environment variable is unset (https://no-color.org).
    """
    use_color = (
        not no_color
        and sys.stdout.isatty()
        and os.environ.get('NO_COLOR') is None
    )
    # strip=True makes colorama remove escape codes from everything it wraps.
    init(strip=not use_color)


# Map of input format -> non-streaming reader (used for multi-file reads).
_READERS = {
    'csv': lambda stream, n, columns, delimiter: read_csv_data(stream, n, columns, delimiter),
    'json': lambda stream, n, columns, delimiter: read_json_data(stream, n, columns),
    'parquet': lambda stream, n, columns, delimiter: read_parquet_data(stream, n, columns),
    'avro': lambda stream, n, columns, delimiter: read_avro_data(stream, n, columns),
    'orc': lambda stream, n, columns, delimiter: read_orc_data(stream, n, columns),
    'text': lambda stream, n, columns, delimiter: read_text_data(stream, n, columns),
}


def _list_non_empty_files(service: str, bucket: str, prefix: str) -> List[Tuple[str, int]]:
    """List a directory and return its non-empty files, sorted by name.

    Raises:
        ValueError: If the directory has no files (or only empty ones).
    """
    files = list_directory(service, bucket, prefix)
    if not files:
        raise ValueError(f"No files found in {service}://{bucket}/{prefix}")

    non_empty_files = [f for f in files if f[1] > 0]
    if not non_empty_files:
        raise ValueError(f"No non-empty files found in {service}://{bucket}/{prefix}")

    non_empty_files.sort(key=lambda x: x[0])
    return non_empty_files


def _drop_metadata_files(files: List[Tuple[str, int]]) -> Tuple[List[Tuple[str, int]], bool]:
    """Drop common metadata/marker files (e.g. _SUCCESS, .crc).

    Returns:
        (files, only_metadata) where only_metadata is True if dropping would
        empty the list, in which case the original list is returned unchanged.
    """
    non_metadata = [f for f in files if not any(re.search(p, f[0]) for p in SKIP_PATTERNS)]
    if non_metadata:
        return non_metadata, False
    return files, True


def _filter_by_format(
    files: List[Tuple[str, int]], input_format: Optional[str]
) -> Tuple[List[Tuple[str, int]], bool]:
    """Filter files matching the requested format's extension.

    Returns:
        (files, matched) where matched is False when an explicit format was
        requested but nothing matched (the original list is returned so the
        caller can fall back).
    """
    if not input_format:
        return files, True
    format_regex = FORMAT_EXTENSION_MAP.get(input_format)
    if not format_regex:
        return files, True
    matching = [f for f in files if re.search(format_regex, f[0], re.IGNORECASE)]
    if matching:
        return matching, True
    return files, False


def get_files_for_multiread(
    service: str,
    bucket: str,
    prefix: str,
    input_format: Optional[str] = None,
    max_size_mb: int = 25,
    quiet: bool = False
) -> List[Tuple[str, int]]:
    """Get a list of files to read up to max_size_mb.

    Args:
        service: Cloud service identifier.
        bucket: Bucket or container name.
        prefix: Directory prefix.
        input_format: Optional format filter.
        max_size_mb: Maximum total size in MB.
        quiet: If True, suppress progress messages.

    Returns:
        List of (filename, size) tuples.

    Raises:
        ValueError: If no suitable files are found.
    """
    non_empty_files = _list_non_empty_files(service, bucket, prefix)

    filtered_files, only_metadata = _drop_metadata_files(non_empty_files)
    if only_metadata:
        info(Fore.YELLOW + "Only found metadata files, using all non-empty files." + Style.RESET_ALL)

    filtered_files, matched = _filter_by_format(filtered_files, input_format)
    if input_format and not matched:
        info(Fore.YELLOW + f"No files matching format '{input_format}' found in "
             f"{service}://{bucket}/{prefix}. Using all available files." + Style.RESET_ALL)

    # Sort by name for deterministic behavior
    filtered_files.sort(key=lambda x: x[0])

    # Select files up to max_size_mb
    max_size_bytes = max_size_mb * 1024 * 1024
    selected_files = []
    total_size = 0

    for file_name, file_size in filtered_files:
        # Always include at least the first file even if it exceeds the limit
        if selected_files and total_size + file_size > max_size_bytes:
            break

        selected_files.append((file_name, file_size))
        total_size += file_size

    if not selected_files:
        raise ValueError(f"No suitable files found in {service}://{bucket}/{prefix}")

    # Report on selected files
    if not quiet:
        total_mb = total_size / (1024 * 1024)
        info(Fore.BLUE + f"Reading {len(selected_files)} files totaling {total_mb:.2f} MB" + Style.RESET_ALL)

    return selected_files


def find_first_non_empty_file(
    service: str,
    bucket: str,
    prefix: str,
    input_format: Optional[str] = None,
    quiet: bool = False
) -> Tuple[str, int]:
    """Find the first non-empty file in a directory that matches the input format.

    Args:
        service: Cloud service identifier.
        bucket: Bucket or container name.
        prefix: Directory prefix.
        input_format: Optional format filter.
        quiet: If True, suppress output messages.

    Returns:
        Tuple of (file_path, file_size) for the first suitable file.

    Raises:
        ValueError: If no suitable files are found.
    """
    non_empty_files = _list_non_empty_files(service, bucket, prefix)

    # Filter by input format if specified
    if input_format:
        matching_files, matched = _filter_by_format(non_empty_files, input_format)
        if matched:
            selected_file = matching_files[0]
            if not quiet:
                info(Fore.BLUE + f"Selected file: {selected_file[0]} ({selected_file[1]} bytes)" + Style.RESET_ALL)
            return selected_file[0], selected_file[1]
        # Explicit format requested but nothing matched: warn instead of
        # silently picking a file that will fail to parse.
        info(Fore.YELLOW + f"No files matching format '{input_format}' found in "
             f"{service}://{bucket}/{prefix}. Using first available file." + Style.RESET_ALL)

    # Use the first non-empty, non-metadata file
    for file_name, file_size in non_empty_files:
        if not any(re.search(pattern, file_name) for pattern in SKIP_PATTERNS):
            if not quiet:
                info(Fore.BLUE + f"Selected file: {file_name} ({file_size} bytes)" + Style.RESET_ALL)
            return file_name, file_size

    # If all files are skipped, use the first non-empty file anyway
    selected_file = non_empty_files[0]
    if not quiet:
        info(Fore.YELLOW + f"Only found metadata files, using: {selected_file[0]} ({selected_file[1]} bytes)" + Style.RESET_ALL)
    return selected_file[0], selected_file[1]


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
    offset: int = 0,
    quiet: bool = False
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
        quiet: Suppress progress messages.

    Returns:
        Tuple of (DataFrame, schema, total_rows).
    """
    dfs = []
    schemas = []
    rows_read = 0
    rows_skipped = 0
    total_rows = 0

    def process_file(file_info, remaining_to_skip, remaining_to_read, file_index, total_files):
        file_name, file_size = file_info
        if not quiet:
            info(Fore.BLUE + f"Reading file: {file_name} ({file_size/1024:.1f} KB)" + Style.RESET_ALL)
        else:
            # Update progress indicator with current file
            short_name = file_name.split('/')[-1]
            update_progress(f"Reading file {file_index + 1}/{total_files}: {short_name}")

        stream = get_stream(service, bucket, file_name)

        # Check for compression and decompress if needed
        compression = detect_compression(file_name)
        if compression:
            if not quiet:
                info(Fore.BLUE + f"Detected {compression} compression, decompressing..." + Style.RESET_ALL)
            stream = decompress_stream(stream, compression)

        # Calculate how many rows to read from this file. When num_rows == 0
        # (read all), remaining_to_read is 0, which the readers treat as "all".
        # We still need offset + limit rows from each file when a limit is set.
        if remaining_to_read > 0:
            rows_to_read_from_file = remaining_to_skip + remaining_to_read
        else:
            rows_to_read_from_file = 0  # read all rows from this file

        reader = _READERS.get(input_format)
        if reader is None:
            raise ValueError(f"Unsupported format: {input_format}")
        df, schema = reader(stream, rows_to_read_from_file, columns, delimiter)

        return df, schema, len(df)

    # Process files in order until we have enough rows
    remaining_offset = offset
    remaining_rows = num_rows if num_rows > 0 else 0  # 0 == read all
    total_files = len(file_list)
    failures = []

    for file_index, file_info in enumerate(file_list):
        try:
            df, schema, file_rows = process_file(
                file_info, remaining_offset, remaining_rows, file_index, total_files
            )

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
            failures.append((file_info[0], e))
            info(Fore.YELLOW + f"Warning: Error reading file {file_info[0]}: {str(e)}" + Style.RESET_ALL)

    if not dfs:
        if rows_skipped > 0:
            info(Fore.YELLOW + f"Warning: Offset ({offset}) skipped all available rows." + Style.RESET_ALL)
            return pd.DataFrame(), pd.Series(dtype=object), total_rows
        if failures:
            # Surface the real cause instead of a generic message.
            first_name, first_exc = failures[0]
            raise ValueError(
                f"No data could be read from any of the {len(file_list)} files; "
                f"first error on '{first_name}': {first_exc}"
            ) from first_exc
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


def read_data_streaming(
    service: str,
    bucket: str,
    object_path: str,
    input_format: str,
    num_rows: int,
    columns: Optional[str] = None,
    delimiter: Optional[str] = None,
    offset: int = 0
) -> Tuple[pd.DataFrame, pd.Series, StreamingStats]:
    """Read data from cloud storage with streaming support.

    Uses PyArrow native filesystems for columnar formats (Parquet, ORC)
    to enable true column projection with range requests. Uses streaming
    decompression and chunked reading for row-based formats.

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
        Tuple of (DataFrame, schema, StreamingStats).
    """
    # Get file size for stats
    try:
        file_size = get_file_size(service, bucket, object_path)
    except Exception:
        file_size = 0

    # Initialize stats
    stats = StreamingStats(file_size=file_size, format_type=input_format)

    # Check for compression
    compression = detect_compression(object_path)
    stats.compression = compression

    # Calculate how many rows to read including offset
    rows_to_read = (offset + num_rows) if num_rows > 0 else 0

    # For columnar formats without external compression, try native PyArrow filesystem
    use_native_fs = input_format in ('parquet', 'orc') and compression is None
    if use_native_fs and not supports_pyarrow_fs():
        info(Fore.YELLOW + "Note: pyarrow.fs not available, downloading full file instead of streaming" + Style.RESET_ALL)
        use_native_fs = False

    if use_native_fs:
        try:
            pyarrow_fs, _ = get_pyarrow_filesystem(
                service,
                aws_profile=cloud_config.aws_profile,
                gcp_project=cloud_config.gcp_project,
                gcp_credentials=cloud_config.gcp_credentials,
                azure_account=cloud_config.azure_account,
                azure_access_key=cloud_config.azure_access_key
            )
            pyarrow_path = f"{bucket}/{object_path}"

            if input_format == 'parquet':
                df, schema, stats = read_parquet_data_streaming(
                    num_rows=rows_to_read,
                    columns=columns,
                    stats=stats,
                    pyarrow_fs=pyarrow_fs,
                    pyarrow_path=pyarrow_path
                )
            else:  # orc
                df, schema, stats = read_orc_data_streaming(
                    num_rows=rows_to_read,
                    columns=columns,
                    stats=stats,
                    pyarrow_fs=pyarrow_fs,
                    pyarrow_path=pyarrow_path
                )

            # Apply offset
            if offset > 0 and not df.empty:
                if offset >= len(df):
                    info(Fore.YELLOW + f"Warning: Offset ({offset}) >= total rows read ({len(df)}). No data to display." + Style.RESET_ALL)
                    df = df.iloc[0:0]
                else:
                    df = df.iloc[offset:].reset_index(drop=True)

            return df, schema, stats

        except Exception as e:
            # Fall back to stream-based approach
            info(Fore.YELLOW + f"Native filesystem unavailable, using stream: {str(e)}" + Style.RESET_ALL)

    # Get stream for non-native filesystem approach
    stream = get_stream(service, bucket, object_path)

    # Handle compression with streaming decompression where possible
    if compression:
        if supports_streaming_decompression(compression):
            info(Fore.BLUE + f"Detected {compression} compression, streaming decompression..." + Style.RESET_ALL)
            stream, is_streaming = get_streaming_decompressor(stream, compression)
            stats.is_streaming = is_streaming
        else:
            info(Fore.BLUE + f"Detected {compression} compression, decompressing..." + Style.RESET_ALL)
            stream = decompress_stream(stream, compression)
            stats.is_streaming = False

    # Read based on format using streaming readers
    if input_format == 'csv':
        df, schema, stats = read_csv_data_streaming(stream, rows_to_read, columns, delimiter, stats)
    elif input_format == 'json':
        df, schema, stats = read_json_data_streaming(stream, rows_to_read, columns, stats)
    elif input_format == 'parquet':
        df, schema, stats = read_parquet_data_streaming(stream=stream, num_rows=rows_to_read, columns=columns, stats=stats)
    elif input_format == 'avro':
        df, schema, stats = read_avro_data_streaming(stream, rows_to_read, columns, stats)
    elif input_format == 'orc':
        df, schema, stats = read_orc_data_streaming(stream=stream, num_rows=rows_to_read, columns=columns, stats=stats)
    elif input_format == 'text':
        df, schema, stats = read_text_data_streaming(stream, rows_to_read, columns, stats)
    else:
        raise ValueError(f"Unsupported format: {input_format}")

    # Apply offset - skip first N rows
    if offset > 0 and not df.empty:
        if offset >= len(df):
            info(Fore.YELLOW + f"Warning: Offset ({offset}) >= total rows read ({len(df)}). No data to display." + Style.RESET_ALL)
            df = df.iloc[0:0]
        else:
            df = df.iloc[offset:].reset_index(drop=True)

    return df, schema, stats


def get_record_count(
    service: str,
    bucket: str,
    object_path: str,
    input_format: str,
    delimiter: Optional[str] = None,
    quiet: bool = False
):
    """Get record count from a file.

    Args:
        service: Cloud service identifier.
        bucket: Bucket or container name.
        object_path: Object path.
        input_format: Data format.
        delimiter: CSV delimiter.
        quiet: If True, suppress progress messages.

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
        if not quiet:
            info(Fore.YELLOW + "Counting records (this might take a while for large files)..." + Style.RESET_ALL)

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


def get_record_count_multiple_files(
    service: str,
    bucket: str,
    file_list: List[Tuple[str, int]],
    input_format: str,
    delimiter: Optional[str] = None
):
    """Get total record count across multiple files.

    Args:
        service: Cloud service identifier.
        bucket: Bucket or container name.
        file_list: List of (filename, size) tuples.
        input_format: Data format.
        delimiter: CSV delimiter.

    Returns:
        Total record count (int) or "Unknown" on failure.
    """
    info(Fore.YELLOW + f"Counting records across {len(file_list)} files..." + Style.RESET_ALL)
    total_count = 0

    for file_name, file_size in file_list:
        try:
            count = get_record_count(service, bucket, file_name, input_format, delimiter, quiet=True)
            if isinstance(count, int):
                total_count += count
                info(Fore.BLUE + f"  {file_name}: {count:,} records" + Style.RESET_ALL)
            else:
                info(Fore.YELLOW + f"  {file_name}: {count}" + Style.RESET_ALL)
        except Exception as e:
            info(Fore.YELLOW + f"  {file_name}: Error - {str(e)}" + Style.RESET_ALL)

    return total_count


_ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')


def _strip_ansi(text: str) -> str:
    """Remove ANSI color escape codes from a string (for file output)."""
    return _ANSI_RE.sub('', text)


def _format_count(value) -> str:
    """Format a record count, leaving non-numeric 'Unknown ...' strings intact."""
    return f"{value:,}" if isinstance(value, int) else str(value)


def _render_data(df: pd.DataFrame, output_format: str) -> str:
    """Render a DataFrame to the requested output format string."""
    if output_format == 'table':
        return format_table_with_colored_header(df)
    elif output_format == 'jsonp':
        return colorize_json(df.to_json(orient='records'))
    elif output_format == 'json':
        return df.to_json(orient='records', lines=True)
    elif output_format == 'csv':
        return df.to_csv(index=False)
    raise ValueError(f"Unsupported output format: {output_format}")


@click.command()
@click.version_option(version=__version__, prog_name='cloudcat')
@click.option('--path', '-p', required=True,
              help='Path to the file or directory (gs://, gcs://, s3://, or abfss://)')
@click.option('--output-format', '-o', type=click.Choice(['json', 'jsonp', 'csv', 'table']), default='table',
              help='Output format (default: table)')
@click.option('--output-file', '-O', type=click.Path(dir_okay=False, writable=True),
              help='Write rendered data to this file instead of stdout')
@click.option('--input-format', '-i', type=click.Choice(['json', 'csv', 'parquet', 'avro', 'orc', 'text']),
              help='Input format (default: inferred from path)')
@click.option('--columns', '-c', help='Comma-separated list of columns to display (default: all)')
@click.option('--num-rows', '-n', default=10, type=click.IntRange(min=0),
              help='Number of rows to display, 0 = all (default: 10)')
@click.option('--offset', default=0, type=click.IntRange(min=0), help='Skip first N rows (default: 0)')
@click.option('--where', '-w',
              help='Filter rows; scans the file (e.g., "status=active", "age>30", "name contains john")')
@click.option('--schema', '-s', type=click.Choice(['show', 'dont_show', 'schema_only']), default='show',
              help='Schema display option (default: show)')
@click.option('--count', is_flag=True, help='Show total record count (requires scanning entire file)')
@click.option('--multi-file-mode', '-m', type=click.Choice(['first', 'auto', 'all']), default='auto',
              help='How to handle directories with multiple files (default: auto)')
@click.option('--max-size-mb', default=25, type=click.IntRange(min=0),
              help='Maximum size in MB to read when reading multiple files (default: 25)')
@click.option('--delimiter', '-d', help='Delimiter to use for CSV files (use "\\t" for tab)')
@click.option('--no-color', is_flag=True, help='Disable colored output (also honors the NO_COLOR env var)')
@click.option('--profile', help='AWS profile name (for S3 access)')
@click.option('--project', help='GCP project ID (for GCS access)')
@click.option('--credentials', help='Path to GCP service account JSON file')
@click.option('--az-access-key', help='Azure storage account access key')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompts (for scripting)')
def main(path, output_format, output_file, input_format, columns, num_rows, offset, where, schema, count,
         multi_file_mode, max_size_mb, delimiter, no_color, profile, project, credentials, az_access_key, yes):
    """Display data from files in Google Cloud Storage, AWS S3, or Azure Data Lake Storage Gen2.

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
    # Read from Azure Data Lake Storage Gen2
    cloudcat --path abfss://container@account.dfs.core.windows.net/data.json --output-format jsonp

    \b
    # Read Avro files from Kafka exports
    cloudcat --path s3://my-bucket/kafka-export.avro

    \b
    # Read ORC files from Hive
    cloudcat --path gcs://my-bucket/hive-table.orc

    \b
    # Read log files as plain text
    cloudcat --path abfss://logs@account.dfs.core.windows.net/app.log --input-format text

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
    # Filter rows with WHERE clause (scans the file for matching rows)
    cloudcat --path s3://bucket/users.parquet --where "status=active"
    cloudcat --path s3://bucket/events.json --where "age>30"
    cloudcat --path gcs://bucket/logs.csv --where "message contains error"

    \b
    # Export rows to a local file (clean data, no diagnostics)
    cloudcat --path s3://bucket/data.parquet --output-format csv --output-file out.csv

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
    # Use Azure storage account access key
    cloudcat --path abfss://container@account.dfs.core.windows.net/data.csv --az-access-key YOUR_KEY
    """
    # Enable color only for an interactive stdout; writing to a file is never
    # colored. This must run before any colored output is produced.
    _configure_color(no_color or bool(output_file))

    # When filtering, the display window (num_rows) must not limit the read
    # window, or we would filter only the first num_rows and miss matches.
    # Reading all rows lets us return up to num_rows *matching* rows.
    read_rows = 0 if where else num_rows

    try:
        # Configure cloud credentials from CLI options
        if profile:
            cloud_config.aws_profile = profile
        if project:
            cloud_config.gcp_project = project
        if credentials:
            cloud_config.gcp_credentials = credentials
        if az_access_key:
            cloud_config.azure_access_key = az_access_key

        # Handle special characters in delimiter
        if delimiter == "\\t":
            delimiter = "\t"

        # Parse the path
        service, bucket, object_path = parse_cloud_path(path)

        # Check if path is a directory (ends with '/' or is empty = bucket root)
        is_directory = object_path.endswith('/') or object_path == ''

        # Initialize streaming stats
        streaming_stats = None
        multi_file_list = None  # For directory reads with --count

        # Handle directory paths based on multi-file-mode
        if is_directory:
            if multi_file_mode == 'first' or (multi_file_mode == 'auto' and max_size_mb <= 0):
                # Use a single file
                start_progress("Listing files...")

                # Find first non-empty file (quiet during progress)
                object_path, file_size = find_first_non_empty_file(service, bucket, object_path, input_format, quiet=True)
                stop_progress()

                # Show file selection info
                info(Fore.BLUE + f"Selected file: {object_path} ({file_size} bytes)" + Style.RESET_ALL)

                # Determine input format if not specified
                if not input_format:
                    input_format = detect_format_from_path(object_path)
                info(Fore.BLUE + f"Inferred input format: {input_format}" + Style.RESET_ALL)

                # Get file name for display
                file_name = object_path.split('/')[-1]
                start_progress(f"Reading {file_name}...")

                # Read the data from the single file with streaming
                df, full_schema, streaming_stats = read_data_streaming(service, bucket, object_path, input_format, read_rows, columns, delimiter, offset)
                total_record_count = None  # Will be computed later if needed

                # Stop progress
                stop_progress()
            else:
                # Read from multiple files
                start_progress("Listing files...")

                # Get files to read for preview (limited by max_size_mb)
                # First, determine input format if not specified (use the first file to infer)
                if not input_format:
                    stop_progress()
                    first_file, _ = find_first_non_empty_file(service, bucket, object_path, quiet=True)
                    input_format = detect_format_from_path(first_file)
                    start_progress(f"Selecting {input_format} files...")
                else:
                    update_progress(f"Selecting {input_format} files...")

                file_list = get_files_for_multiread(service, bucket, object_path, input_format, max_size_mb, quiet=True)

                # For a single file, use streaming read for efficiency
                if len(file_list) == 1:
                    single_file_path = file_list[0][0]
                    file_name = single_file_path.split('/')[-1]
                    update_progress(f"Reading {file_name}...")

                    # Use streaming read for all formats
                    df, full_schema, streaming_stats = read_data_streaming(
                        service, bucket, single_file_path, input_format, read_rows, columns, delimiter, offset
                    )
                    stop_progress()

                    info(Fore.BLUE + f"Inferred input format: {input_format}" + Style.RESET_ALL)
                    multi_file_list = file_list
                    total_record_count = None
                elif len(file_list) > 1:
                    # Read data from multiple files with progress updates
                    update_progress(f"Reading {len(file_list)} files...")
                    df, full_schema, rows_in_files = read_data_from_multiple_files(
                        service, bucket, file_list, input_format, read_rows, columns, delimiter, offset, quiet=True
                    )

                    # Stop progress before any output
                    stop_progress()

                    # Calculate total size for stats
                    total_size = sum(f[1] for f in file_list)
                    streaming_stats = StreamingStats(file_size=total_size, bytes_read=total_size, format_type=input_format)

                    info(Fore.BLUE + f"Inferred input format: {input_format}" + Style.RESET_ALL)

                    # total_record_count will be computed later if --count is specified
                    total_record_count = None
                    multi_file_list = file_list

                # For --count, get ALL files (not limited by max_size_mb)
                # so we can count records across the entire directory
                if count:
                    all_files = get_files_for_multiread(service, bucket, object_path, input_format, max_size_mb=999999, quiet=True)
                    all_files_size = sum(f[1] for f in all_files)
                    all_files_size_mb = all_files_size / (1024 * 1024)

                    # Warn user about counting all files in directory
                    if not yes:
                        info(Fore.YELLOW + f"\nWarning: --count will scan {len(all_files)} files ({all_files_size_mb:.1f} MB total)." + Style.RESET_ALL)
                        if not click.confirm("Continue?", default=True, err=True):
                            info("Aborted.")
                            return

                    multi_file_list = all_files
                    # Update stats to reflect all files
                    streaming_stats.file_size = all_files_size

                # Update object_path for display/logging purposes
                num_files_display = len(multi_file_list) if count else len(file_list)
                object_path = f"{object_path} ({num_files_display} files)"
        else:
            # Single file path
            # Determine input format if not specified
            if not input_format:
                input_format = detect_format_from_path(object_path)
                info(Fore.BLUE + f"Inferred input format: {input_format}" + Style.RESET_ALL)

            # Get file name for display
            file_name = object_path.split('/')[-1]
            start_progress(f"Reading {file_name}...")

            # Read the data with streaming
            df, full_schema, streaming_stats = read_data_streaming(service, bucket, object_path, input_format, read_rows, columns, delimiter, offset)
            total_record_count = None  # Will be computed later if needed

            stop_progress()

        # Apply WHERE filter if specified. Because we read the full file when
        # filtering, this matches across the whole file; then we truncate to the
        # requested display window.
        if where and not df.empty:
            original_count = len(df)
            df = apply_where_filter(df, where)
            if num_rows > 0 and len(df) > num_rows:
                df = df.head(num_rows)
            filtered_count = len(df)
            info(Fore.BLUE + f"Filtered: {filtered_count} of {original_count} rows match '{where}'" + Style.RESET_ALL)

        def emit_count(prefix=""):
            """Compute (if needed) and print the total record count to stderr."""
            nonlocal total_record_count
            try:
                if total_record_count is None:
                    if multi_file_list:
                        total_record_count = get_record_count_multiple_files(
                            service, bucket, multi_file_list, input_format, delimiter
                        )
                    else:
                        total_record_count = get_record_count(service, bucket, object_path, input_format, delimiter)
                    # Reflect that the full file was scanned for counting.
                    if streaming_stats:
                        streaming_stats.bytes_read = streaming_stats.file_size
                info(Fore.CYAN + f"{prefix}Total records: {_format_count(total_record_count)}" + Style.RESET_ALL)
            except Exception as e:
                info(Fore.YELLOW + f"{prefix}Could not count records: {str(e)}" + Style.RESET_ALL)

        # Display schema if requested. In schema_only mode the schema IS the
        # requested output, so it goes to stdout; otherwise it is supplementary
        # context and goes to stderr to keep stdout clean for piping.
        if schema in ['show', 'schema_only']:
            schema_lines = [Fore.GREEN + "Schema:" + Style.RESET_ALL]
            schema_lines += [f"  {col}: {dtype}" for col, dtype in full_schema.items()]
            schema_text = '\n'.join(schema_lines)
            if schema == 'schema_only':
                click.echo(schema_text)
            else:
                info(schema_text)
                info("")

        # Exit if only schema was requested
        if schema == 'schema_only':
            if count:
                emit_count()
            return

        # Render the data and write it to the output file or stdout.
        rendered = _render_data(df, output_format)
        if output_file:
            with open(output_file, 'w', encoding='utf-8', newline='') as f:
                f.write(_strip_ansi(rendered))
                if not rendered.endswith('\n'):
                    f.write('\n')
            info(Fore.GREEN + f"Wrote output to {output_file}" + Style.RESET_ALL)
        else:
            click.echo(rendered)

        # Show record count only if --count flag is specified
        if count:
            emit_count(prefix="\n")

        # Display streaming stats footer (to stderr, supplementary info)
        if streaming_stats and streaming_stats.file_size > 0:
            info(Fore.BLUE + f"\n{streaming_stats.format_report()}" + Style.RESET_ALL)

    except Exception as e:
        stop_progress()  # Make sure progress is stopped on error
        click.echo(Fore.RED + f"Error: {str(e)}" + Style.RESET_ALL, err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
