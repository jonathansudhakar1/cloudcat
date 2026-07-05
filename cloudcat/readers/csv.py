"""CSV data reader."""

from typing import Optional, Tuple, Union, BinaryIO
import io
import pandas as pd
import click
from colorama import Fore, Style

from ..streaming import StreamingStats, BytesTrackingStream


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
    stats: Optional[StreamingStats] = None,
    where: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series, Optional[StreamingStats]]:
    """Read CSV data with streaming support.

    Uses chunked reading to enable early termination when row limit is reached,
    reducing data transfer for row-limited queries. When ``where`` is given,
    chunks are filtered as they stream and reading stops as soon as
    ``num_rows`` matching rows are collected — without materializing the file.

    Args:
        stream: File-like object containing CSV data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.
        delimiter: Custom delimiter character.
        stats: StreamingStats instance for tracking bytes read.
        where: Optional WHERE expression applied while streaming.

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

    # Wrap the source in a tracking stream so bytes_read reflects the bytes
    # actually pulled from the source, not the in-memory DataFrame footprint.
    stats.bytes_read = 0
    tracked = BytesTrackingStream(stream, stats) if hasattr(stream, 'read') else stream

    pd_args = {}
    if delimiter:
        pd_args['delimiter'] = delimiter

    if where:
        # Filter-as-you-stream: scan chunks, keep only matching rows, stop
        # once num_rows matches are collected (num_rows == 0 scans the whole
        # file but memory stays bounded by the matches).
        from ..filtering import apply_where_filter
        pd_args['chunksize'] = 1000

        matched_chunks = []
        first_chunk = None
        matched = 0
        scanned = 0

        for chunk in pd.read_csv(tracked, **pd_args):
            if first_chunk is None:
                first_chunk = chunk
            scanned += len(chunk)
            hits = apply_where_filter(chunk, where)
            if not hits.empty:
                if num_rows > 0 and matched + len(hits) > num_rows:
                    hits = hits.head(num_rows - matched)
                matched_chunks.append(hits)
                matched += len(hits)
            if num_rows > 0 and matched >= num_rows:
                break

        if matched_chunks:
            full_df = pd.concat(matched_chunks, ignore_index=True)
        elif first_chunk is not None:
            full_df = first_chunk.head(0)  # empty but correctly typed
        else:
            full_df = pd.DataFrame()

        stats.where_applied = True
        stats.rows_scanned = scanned
        return _apply_column_filter(full_df, col_names, stats)

    # Use chunked reading for streaming when we have a row limit
    if num_rows > 0:
        # Use smaller chunks for better streaming efficiency
        chunk_size = min(1000, num_rows)
        pd_args['chunksize'] = chunk_size

        chunks = []
        rows_collected = 0

        try:
            reader = pd.read_csv(tracked, **pd_args)

            for chunk in reader:
                remaining = num_rows - rows_collected
                if len(chunk) > remaining:
                    chunk = chunk.head(remaining)

                chunks.append(chunk)
                rows_collected += len(chunk)

                if rows_collected >= num_rows:
                    break

        except Exception as e:
            # If chunked reading fails, fall back to a regular read — but only
            # if we can rewind the stream. Non-seekable streams (e.g. a zstd
            # stream_reader) raise on seek(0); in that case re-raise the original
            # parse error rather than masking it with a confusing seek error.
            if hasattr(stream, 'seek') and getattr(stream, 'seekable', lambda: True)():
                try:
                    stream.seek(0)
                    stats.bytes_read = 0
                except (OSError, ValueError):
                    raise e
            else:
                raise e
            pd_args.pop('chunksize', None)
            pd_args['nrows'] = num_rows
            full_df = pd.read_csv(tracked, **pd_args)
            stats.is_streaming = False
            return _apply_column_filter(full_df, col_names, stats)

        if chunks:
            full_df = pd.concat(chunks, ignore_index=True)
        else:
            full_df = pd.DataFrame()
    else:
        # No row limit - read all data
        full_df = pd.read_csv(tracked, **pd_args)
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
