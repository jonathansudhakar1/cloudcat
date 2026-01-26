"""CSV data reader."""

from typing import Optional, Tuple, Union, BinaryIO
import io
import pandas as pd
import click
from colorama import Fore, Style

from ..streaming import StreamingStats


def read_csv_data(
    stream: Union[BinaryIO, io.StringIO],
    num_rows: int,
    columns: Optional[str] = None,
    delimiter: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series]:
    """Read CSV data from a stream (legacy interface).

    Args:
        stream: File-like object containing CSV data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.
        delimiter: Custom delimiter character.

    Returns:
        Tuple of (DataFrame, schema Series).
    """
    df, schema, _ = read_csv_data_streaming(
        stream=stream,
        num_rows=num_rows,
        columns=columns,
        delimiter=delimiter,
        stats=None
    )
    return df, schema


def read_csv_data_streaming(
    stream: Union[BinaryIO, io.StringIO],
    num_rows: int,
    columns: Optional[str] = None,
    delimiter: Optional[str] = None,
    stats: Optional[StreamingStats] = None
) -> Tuple[pd.DataFrame, pd.Series, Optional[StreamingStats]]:
    """Read CSV data with streaming support.

    Uses chunked reading to enable early termination when row limit is reached,
    reducing data transfer for row-limited queries.

    Args:
        stream: File-like object containing CSV data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.
        delimiter: Custom delimiter character.
        stats: StreamingStats instance for tracking bytes read.

    Returns:
        Tuple of (DataFrame, schema Series, StreamingStats).
    """
    if stats is None:
        stats = StreamingStats()

    stats.format_type = 'csv'
    stats.rows_requested = num_rows if num_rows > 0 else None
    stats.is_streaming = True

    col_names = [c.strip() for c in columns.split(',')] if columns else None
    stats.columns_requested = col_names

    pd_args = {}
    if delimiter:
        pd_args['delimiter'] = delimiter

    # Use chunked reading for streaming when we have a row limit
    if num_rows > 0:
        # Use smaller chunks for better streaming efficiency
        chunk_size = min(1000, num_rows)
        pd_args['chunksize'] = chunk_size

        chunks = []
        rows_collected = 0
        bytes_read = 0

        try:
            reader = pd.read_csv(stream, **pd_args)

            for chunk in reader:
                # Track approximate bytes (chunk size * avg row size estimate)
                chunk_bytes = chunk.memory_usage(deep=True).sum()
                bytes_read += chunk_bytes

                remaining = num_rows - rows_collected
                if len(chunk) > remaining:
                    chunk = chunk.head(remaining)

                chunks.append(chunk)
                rows_collected += len(chunk)

                if rows_collected >= num_rows:
                    break

        except Exception as e:
            # If chunked reading fails, fall back to regular read
            if hasattr(stream, 'seek'):
                stream.seek(0)
            pd_args.pop('chunksize', None)
            pd_args['nrows'] = num_rows
            full_df = pd.read_csv(stream, **pd_args)
            stats.bytes_read = full_df.memory_usage(deep=True).sum()
            stats.is_streaming = False
            return _apply_column_filter(full_df, col_names, stats)

        if chunks:
            full_df = pd.concat(chunks, ignore_index=True)
        else:
            full_df = pd.DataFrame()

        stats.bytes_read = bytes_read
    else:
        # No row limit - read all data
        full_df = pd.read_csv(stream, **pd_args)
        stats.bytes_read = full_df.memory_usage(deep=True).sum()
        stats.is_streaming = False

    return _apply_column_filter(full_df, col_names, stats)


def _apply_column_filter(
    full_df: pd.DataFrame,
    col_names: Optional[list],
    stats: StreamingStats
) -> Tuple[pd.DataFrame, pd.Series, StreamingStats]:
    """Apply column filtering and return results."""
    # Store the full schema
    full_schema = full_df.dtypes

    # Apply column filtering if specified
    if col_names:
        valid_cols = [c for c in col_names if c in full_df.columns]
        if len(valid_cols) != len(col_names):
            missing = set(col_names) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        if not valid_cols:
            raise ValueError(f"None of the requested columns exist. Available: {', '.join(full_df.columns)}")
        df = full_df[valid_cols]
    else:
        df = full_df

    return df, full_schema, stats
