"""Parquet data reader."""

from typing import Optional, Tuple, Union, BinaryIO, Any
import io
import os
import sys
import tempfile
import pandas as pd
from colorama import Fore, Style

from ..streaming import StreamingStats

# Try to import Parquet support
try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    HAS_PARQUET = True
except ImportError:
    pq = None
    pa = None
    HAS_PARQUET = False


def read_parquet_data(
    stream: Union[BinaryIO, str],
    num_rows: int,
    columns: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series]:
    """Read Parquet data from a stream (legacy interface).

    Args:
        stream: File-like object or file path containing Parquet data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.

    Returns:
        Tuple of (DataFrame, schema Series).

    Raises:
        SystemExit: If pyarrow is not installed.
    """
    df, schema, _ = read_parquet_data_streaming(
        stream=stream,
        num_rows=num_rows,
        columns=columns,
        stats=None
    )
    return df, schema


def read_parquet_data_streaming(
    stream: Union[BinaryIO, str, None] = None,
    num_rows: int = 0,
    columns: Optional[str] = None,
    stats: Optional[StreamingStats] = None,
    pyarrow_fs: Optional[Any] = None,
    pyarrow_path: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series, Optional[StreamingStats]]:
    """Read Parquet data with streaming support.

    When pyarrow_fs and pyarrow_path are provided, uses PyArrow's native
    cloud filesystem for true streaming with range requests. This enables
    column projection to only fetch required column chunks.

    Args:
        stream: File-like object or file path containing Parquet data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.
        stats: StreamingStats instance for tracking bytes read.
        pyarrow_fs: PyArrow filesystem for native cloud access.
        pyarrow_path: Path within the PyArrow filesystem.

    Returns:
        Tuple of (DataFrame, schema Series, StreamingStats).

    Raises:
        SystemExit: If pyarrow is not installed.
    """
    if not HAS_PARQUET:
        sys.stderr.write(
            Fore.RED + "Error: pyarrow package is required for Parquet support.\n" +
            "Install it with: pip install pyarrow\n" + Style.RESET_ALL
        )
        sys.exit(1)

    if stats is None:
        stats = StreamingStats()

    stats.format_type = 'parquet'
    stats.rows_requested = num_rows if num_rows > 0 else None

    col_names = [c.strip() for c in columns.split(',')] if columns else None
    stats.columns_requested = col_names

    # Use native PyArrow filesystem if available (true streaming)
    if pyarrow_fs is not None and pyarrow_path is not None:
        return _read_with_native_fs(
            pyarrow_fs, pyarrow_path, num_rows, col_names, stats
        )

    # Fallback: use stream with temp file approach
    return _read_with_stream(stream, num_rows, col_names, stats)


def _read_with_native_fs(
    filesystem: Any,
    path: str,
    num_rows: int,
    col_names: Optional[list],
    stats: StreamingStats
) -> Tuple[pd.DataFrame, pd.Series, StreamingStats]:
    """Read Parquet using PyArrow native filesystem (true streaming).

    This approach uses range requests to only fetch required column chunks,
    significantly reducing data transfer for column-projected queries.
    """
    stats.used_native_fs = True
    stats.is_streaming = True

    # Open file with native filesystem
    parquet_file = pq.ParquetFile(path, filesystem=filesystem)
    metadata = parquet_file.metadata

    # Read data with column projection
    if num_rows > 0:
        tables = []
        rows_read = 0

        for i in range(parquet_file.num_row_groups):
            if rows_read >= num_rows:
                break

            # This issues range requests for only the specified columns
            table = parquet_file.read_row_group(i, columns=col_names)

            if rows_read + table.num_rows > num_rows:
                table = table.slice(0, num_rows - rows_read)

            tables.append(table)
            rows_read += table.num_rows

        if tables:
            result_table = pa.concat_tables(tables)
            df = result_table.to_pandas()
        else:
            df = pd.DataFrame()
    else:
        # Read all data with column filtering
        table = parquet_file.read(columns=col_names)
        df = table.to_pandas()

    # Get full schema
    full_schema = _get_schema_from_metadata(parquet_file)

    # Estimate bytes read from metadata
    stats.bytes_read = _estimate_bytes_read(
        metadata, col_names, num_rows if num_rows > 0 else metadata.num_rows
    )

    return df, full_schema, stats


def _read_with_stream(
    stream: Union[BinaryIO, str],
    num_rows: int,
    col_names: Optional[list],
    stats: StreamingStats
) -> Tuple[pd.DataFrame, pd.Series, StreamingStats]:
    """Read Parquet from a stream (fallback for compressed files)."""
    stats.used_native_fs = False
    stats.is_streaming = False

    # For Parquet, we need a temporary file to properly read the metadata
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_path = temp_file.name

    try:
        # If stream is a file-like object, copy to temp file
        if hasattr(stream, 'read'):
            data = stream.read()
            stats.bytes_read = len(data)
            with open(temp_path, 'wb') as f:
                f.write(data)
        else:
            # Assume it's already a path
            temp_path = stream
            stats.bytes_read = os.path.getsize(temp_path) if os.path.exists(temp_path) else 0

        parquet_file = pq.ParquetFile(temp_path)

        # Read the data efficiently
        if num_rows > 0:
            tables = []
            rows_read = 0

            for i in range(parquet_file.num_row_groups):
                if rows_read >= num_rows:
                    break

                table = parquet_file.read_row_group(i, columns=col_names)

                if rows_read + table.num_rows > num_rows:
                    table = table.slice(0, num_rows - rows_read)

                tables.append(table)
                rows_read += table.num_rows

            if tables:
                result_table = pa.concat_tables(tables)
                df = result_table.to_pandas()
            else:
                df = pd.DataFrame()
        else:
            table = parquet_file.read(columns=col_names)
            df = table.to_pandas()

        full_schema = _get_schema_from_metadata(parquet_file)
        return df, full_schema, stats

    finally:
        try:
            if hasattr(stream, 'read'):
                os.unlink(temp_path)
        except OSError:
            pass


def _get_schema_from_metadata(parquet_file) -> pd.Series:
    """Extract full schema from Parquet file."""
    if parquet_file.num_row_groups > 0:
        sample_table = parquet_file.read_row_group(0)
        if sample_table.num_rows > 1:
            sample_table = sample_table.slice(0, 1)
        full_df = sample_table.to_pandas()
    else:
        full_df = pd.DataFrame()
    return full_df.dtypes


def _estimate_bytes_read(
    metadata,
    col_names: Optional[list],
    rows_read: int
) -> int:
    """Estimate bytes read based on Parquet metadata.

    This provides an approximation of actual bytes transferred based on
    column chunk sizes in the metadata.
    """
    if metadata.num_rows == 0:
        return 0

    total_bytes = 0

    # Get column indices we care about
    schema = metadata.schema
    num_columns = len(schema)
    if col_names:
        col_indices = set()
        for i in range(num_columns):
            if schema[i].name in col_names:
                col_indices.add(i)
    else:
        col_indices = set(range(num_columns))

    # Sum up column chunk sizes for row groups we read
    rows_counted = 0
    for rg_idx in range(metadata.num_row_groups):
        if rows_counted >= rows_read:
            break

        row_group = metadata.row_group(rg_idx)

        for col_idx in range(row_group.num_columns):
            col = row_group.column(col_idx)
            # Check if this column is one we requested
            if col_idx in col_indices or not col_names:
                total_bytes += col.total_compressed_size

        rows_counted += row_group.num_rows

    return total_bytes
