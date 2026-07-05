"""Plain text data reader."""

from typing import Optional, Tuple, Union, BinaryIO
import pandas as pd
import click
from colorama import Fore, Style

from ..streaming import StreamingStats


def read_text_data(
    stream: Union[BinaryIO, str],
    num_rows: int,
    columns: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series]:
    """Read plain text data from a stream (legacy interface).

    Args:
        stream: File-like object or file path containing text data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.

    Returns:
        Tuple of (DataFrame, schema Series).
    """
    df, schema, _ = read_text_data_streaming(
        stream=stream,
        num_rows=num_rows,
        columns=columns,
        stats=None
    )
    return df, schema


def read_text_data_streaming(
    stream: Union[BinaryIO, str],
    num_rows: int,
    columns: Optional[str] = None,
    stats: Optional[StreamingStats] = None,
    where: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series, Optional[StreamingStats]]:
    """Read plain text data with streaming support.

    Uses line-by-line reading to enable early termination when row limit
    is reached, reducing data transfer for row-limited queries. With
    ``where``, lines are filtered in batches as they stream (line_number
    reflects the position in the file, not the filtered output) and reading
    stops at ``num_rows`` matches.

    Args:
        stream: File-like object or file path containing text data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.
        stats: StreamingStats instance for tracking bytes read.
        where: Optional WHERE expression applied while streaming.

    Returns:
        Tuple of (DataFrame, schema Series, StreamingStats).
    """
    if stats is None:
        stats = StreamingStats()

    stats.format_type = 'text'
    stats.rows_requested = num_rows if num_rows > 0 else None

    col_names = [c.strip() for c in columns.split(',')] if columns else None
    stats.columns_requested = col_names

    if where and hasattr(stream, 'read'):
        return _read_text_filtered(stream, num_rows, col_names, stats, where)

    lines = []
    bytes_read = 0

    # Stream line-by-line for row limiting
    if hasattr(stream, 'read'):
        if num_rows > 0:
            # Streaming approach - read line by line
            stats.is_streaming = True
            for line_bytes in stream:
                if isinstance(line_bytes, bytes):
                    bytes_read += len(line_bytes)
                    line = line_bytes.decode('utf-8', errors='replace').rstrip('\n\r')
                else:
                    bytes_read += len(line_bytes.encode('utf-8'))
                    line = line_bytes.rstrip('\n\r')

                lines.append(line)

                if len(lines) >= num_rows:
                    break
        else:
            # No row limit - read all
            stats.is_streaming = False
            content = stream.read()
            if isinstance(content, bytes):
                bytes_read = len(content)
                content = content.decode('utf-8', errors='replace')
            else:
                bytes_read = len(content.encode('utf-8'))
            lines = content.splitlines()
    else:
        # File path - read all (binary read so non-UTF-8 bytes degrade gracefully)
        stats.is_streaming = False
        with open(stream, 'rb') as f:
            raw = f.read()
            bytes_read = len(raw)
            content = raw.decode('utf-8', errors='replace')
            lines = content.splitlines()

        if num_rows > 0:
            lines = lines[:num_rows]

    stats.bytes_read = bytes_read

    # Create DataFrame with a single 'line' column
    full_df = pd.DataFrame({'line': lines, 'line_number': range(1, len(lines) + 1)})

    # Store the full schema
    full_schema = full_df.dtypes

    # Apply column filtering if specified
    if col_names:
        valid_cols = [c for c in col_names if c in full_df.columns]
        if len(valid_cols) != len(col_names):
            missing = set(col_names) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        df = full_df[valid_cols]
    else:
        df = full_df

    return df, full_schema, stats


def _read_text_filtered(
    stream: BinaryIO,
    num_rows: int,
    col_names: Optional[list],
    stats: StreamingStats,
    where: str
) -> Tuple[pd.DataFrame, pd.Series, StreamingStats]:
    """Filter-as-you-stream over text lines, stopping at num_rows matches.

    line_number reflects the position in the file, so filtered output stays
    traceable back to the source (like grep -n).
    """
    from ..filtering import apply_where_filter

    stats.is_streaming = True
    batch_size = 1000
    matched_frames = []
    batch_lines = []
    batch_start = 1
    matched = 0
    scanned = 0
    bytes_read = 0

    def _flush():
        nonlocal matched, batch_start
        frame = pd.DataFrame({
            'line': batch_lines,
            'line_number': range(batch_start, batch_start + len(batch_lines)),
        })
        batch_start += len(batch_lines)
        hits = apply_where_filter(frame, where)
        if not hits.empty:
            if num_rows > 0 and matched + len(hits) > num_rows:
                hits = hits.head(num_rows - matched)
            matched_frames.append(hits)
            matched += len(hits)

    for line_bytes in stream:
        if isinstance(line_bytes, bytes):
            bytes_read += len(line_bytes)
            line = line_bytes.decode('utf-8', errors='replace').rstrip('\n\r')
        else:
            bytes_read += len(line_bytes.encode('utf-8'))
            line = line_bytes.rstrip('\n\r')
        batch_lines.append(line)
        scanned += 1

        if len(batch_lines) >= batch_size:
            _flush()
            batch_lines = []
            if num_rows > 0 and matched >= num_rows:
                break
    if batch_lines and (num_rows <= 0 or matched < num_rows):
        _flush()

    stats.bytes_read = bytes_read
    stats.rows_scanned = scanned
    stats.where_applied = True

    if matched_frames:
        full_df = pd.concat(matched_frames, ignore_index=True)
    else:
        full_df = pd.DataFrame({'line': [], 'line_number': []})

    full_schema = full_df.dtypes
    if col_names:
        valid_cols = [c for c in col_names if c in full_df.columns]
        if len(valid_cols) != len(col_names):
            missing = set(col_names) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        full_df = full_df[valid_cols]

    return full_df, full_schema, stats
