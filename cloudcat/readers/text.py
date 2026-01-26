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
    stats: Optional[StreamingStats] = None
) -> Tuple[pd.DataFrame, pd.Series, Optional[StreamingStats]]:
    """Read plain text data with streaming support.

    Uses line-by-line reading to enable early termination when row limit
    is reached, reducing data transfer for row-limited queries.

    Args:
        stream: File-like object or file path containing text data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.
        stats: StreamingStats instance for tracking bytes read.

    Returns:
        Tuple of (DataFrame, schema Series, StreamingStats).
    """
    if stats is None:
        stats = StreamingStats()

    stats.format_type = 'text'
    stats.rows_requested = num_rows if num_rows > 0 else None

    col_names = [c.strip() for c in columns.split(',')] if columns else None
    stats.columns_requested = col_names

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
                    line = line_bytes.decode('utf-8').rstrip('\n\r')
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
                content = content.decode('utf-8')
            else:
                bytes_read = len(content.encode('utf-8'))
            lines = content.splitlines()
    else:
        # File path - read all
        stats.is_streaming = False
        with open(stream, 'r') as f:
            content = f.read()
            bytes_read = len(content.encode('utf-8'))
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
