"""JSON data reader."""

from typing import Optional, Tuple, Union, BinaryIO
import io
import json
import pandas as pd
import click
from colorama import Fore, Style

from ..streaming import StreamingStats


def read_json_data(
    stream: Union[BinaryIO, io.StringIO, str],
    num_rows: int,
    columns: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series]:
    """Read JSON data from a stream (legacy interface).

    Args:
        stream: File-like object or string containing JSON data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.

    Returns:
        Tuple of (DataFrame, schema Series).
    """
    df, schema, _ = read_json_data_streaming(
        stream=stream,
        num_rows=num_rows,
        columns=columns,
        stats=None
    )
    return df, schema


def read_json_data_streaming(
    stream: Union[BinaryIO, io.StringIO, str],
    num_rows: int,
    columns: Optional[str] = None,
    stats: Optional[StreamingStats] = None
) -> Tuple[pd.DataFrame, pd.Series, Optional[StreamingStats]]:
    """Read JSON data with streaming support.

    For JSON Lines format, uses line-by-line reading for true streaming.
    For JSON arrays, must read the entire content (cannot stream).

    Args:
        stream: File-like object or string containing JSON data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.
        stats: StreamingStats instance for tracking bytes read.

    Returns:
        Tuple of (DataFrame, schema Series, StreamingStats).
    """
    if stats is None:
        stats = StreamingStats()

    stats.format_type = 'json'
    stats.rows_requested = num_rows if num_rows > 0 else None

    col_names = [c.strip() for c in columns.split(',')] if columns else None
    stats.columns_requested = col_names

    # Try streaming approach for JSON Lines first
    if hasattr(stream, 'read'):
        # Peek at first character to detect format
        first_bytes = stream.read(1)
        if isinstance(first_bytes, bytes):
            first_char = first_bytes.decode('utf-8', errors='replace') if first_bytes else ''
        else:
            first_char = first_bytes

        if first_char == '{':
            # Likely JSON Lines - try line-by-line streaming
            df, full_schema = _read_json_lines_streaming(stream, first_bytes, num_rows, stats)
            return _apply_column_filter(df, full_schema, col_names, stats)
        elif first_char == '[':
            # JSON array - must read fully
            stats.is_streaming = False
            rest = stream.read()
            if isinstance(rest, bytes):
                content = first_bytes + rest
                content = content.decode('utf-8', errors='replace')
            else:
                content = first_char + rest
            stats.bytes_read = len(content.encode('utf-8'))
            df, full_schema = _read_json_array(content, num_rows)
            return _apply_column_filter(df, full_schema, col_names, stats)
        else:
            # Empty or unknown format
            stats.is_streaming = False
            rest = stream.read()
            if isinstance(rest, bytes):
                content = (first_bytes + rest).decode('utf-8', errors='replace')
            else:
                content = first_char + rest
            stats.bytes_read = len(content.encode('utf-8'))
            df, full_schema = _read_json_fallback(content, num_rows)
            return _apply_column_filter(df, full_schema, col_names, stats)
    else:
        # String content - cannot stream
        stats.is_streaming = False
        content = stream
        stats.bytes_read = len(content.encode('utf-8'))
        df, full_schema = _read_json_fallback(content, num_rows)
        return _apply_column_filter(df, full_schema, col_names, stats)


def _read_json_lines_streaming(
    stream: BinaryIO,
    first_bytes: bytes,
    num_rows: int,
    stats: StreamingStats
) -> Tuple[pd.DataFrame, pd.Series]:
    """Read JSON that begins with '{'.

    This handles two distinct shapes that both start with '{':
      * JSON Lines (one complete object per line) — read line-by-line so we can
        stop early once ``num_rows`` rows are collected (true streaming).
      * A single pretty-printed (multi-line) object — the per-line parse fails,
        so we read the whole document and parse it as one record.

    The shape is decided from the *first complete line*: if it parses as
    standalone JSON it is treated as JSON Lines, otherwise as a single document.
    """
    stats.is_streaming = True
    bytes_read = len(first_bytes) if first_bytes else 0

    def _to_bytes(chunk):
        return chunk.encode('utf-8') if isinstance(chunk, str) else chunk

    # Read the first complete line (the rest of the line after first_bytes).
    first_line = _to_bytes(first_bytes) if first_bytes else b''
    for line_bytes in stream:
        bytes_read += len(line_bytes)
        first_line += _to_bytes(line_bytes)
        if first_line.endswith(b'\n'):
            break

    first_line_str = first_line.decode('utf-8', errors='replace').strip()

    def _is_standalone_json(text: str) -> bool:
        try:
            json.loads(text)
            return True
        except (json.JSONDecodeError, ValueError):
            return False

    if not _is_standalone_json(first_line_str):
        # Not JSON Lines — a multi-line document. Read the remainder and parse
        # the whole thing as a single JSON document.
        rest = stream.read()
        bytes_read += len(rest) if rest else 0
        content = first_line + _to_bytes(rest if rest else b'')
        stats.is_streaming = False
        stats.bytes_read = bytes_read
        df, _ = _read_json_fallback(content.decode('utf-8', errors='replace'), num_rows)
        return df, df.dtypes

    # JSON Lines: collect the first line, then continue streaming.
    lines = [first_line_str]
    rows_collected = 1
    current_line = b''

    if num_rows <= 0 or rows_collected < num_rows:
        for line_bytes in stream:
            bytes_read += len(line_bytes)
            current_line += _to_bytes(line_bytes)

            if current_line.endswith(b'\n'):
                line_str = current_line.decode('utf-8', errors='replace').strip()
                if line_str:
                    lines.append(line_str)
                    rows_collected += 1
                current_line = b''

                if num_rows > 0 and rows_collected >= num_rows:
                    break

    # Handle a trailing line without a newline.
    if current_line:
        line_str = current_line.decode('utf-8', errors='replace').strip()
        if line_str and (num_rows <= 0 or rows_collected < num_rows):
            lines.append(line_str)

    stats.bytes_read = bytes_read

    content = '\n'.join(lines)
    try:
        df = pd.read_json(io.StringIO(content), lines=True)
    except Exception:
        records = []
        for line in lines:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        df = pd.DataFrame(records) if records else pd.DataFrame()

    return df, df.dtypes


def _read_json_array(content: str, num_rows: int) -> Tuple[pd.DataFrame, pd.Series]:
    """Read a JSON array."""
    parsed = json.loads(content)

    if isinstance(parsed, list):
        df = pd.DataFrame(parsed)
    elif isinstance(parsed, dict):
        df = pd.DataFrame([parsed])
    else:
        raise ValueError("JSON must be an array or object")

    if num_rows > 0 and len(df) > num_rows:
        df = df.head(num_rows)

    return df, df.dtypes


def _read_json_fallback(content: str, num_rows: int) -> Tuple[pd.DataFrame, pd.Series]:
    """Fallback JSON reading for mixed formats."""
    content_stripped = content.strip()

    if not content_stripped:
        return pd.DataFrame(), pd.Series(dtype=object)

    first_char = content_stripped[0]

    try:
        if first_char == '{':
            # Could be JSON Lines or single object
            lines = [line.strip() for line in content_stripped.split('\n') if line.strip()]
            if len(lines) > 1 and all(line.startswith('{') for line in lines[:min(5, len(lines))]):
                # JSON Lines
                if num_rows > 0:
                    df = pd.read_json(io.StringIO(content), lines=True, nrows=num_rows)
                else:
                    df = pd.read_json(io.StringIO(content), lines=True)
            else:
                # Single JSON object
                parsed = json.loads(content)
                df = pd.DataFrame([parsed]) if isinstance(parsed, dict) else pd.DataFrame(parsed)
                if num_rows > 0 and len(df) > num_rows:
                    df = df.head(num_rows)
        elif first_char == '[':
            return _read_json_array(content, num_rows)
        else:
            # Try JSON Lines as fallback
            if num_rows > 0:
                df = pd.read_json(io.StringIO(content), lines=True, nrows=num_rows)
            else:
                df = pd.read_json(io.StringIO(content), lines=True)
    except (json.JSONDecodeError, ValueError):
        # Last resort - try JSON Lines
        try:
            if num_rows > 0:
                df = pd.read_json(io.StringIO(content), lines=True, nrows=num_rows)
            else:
                df = pd.read_json(io.StringIO(content), lines=True)
        except Exception:
            df = pd.DataFrame()

    return df, df.dtypes


def _apply_column_filter(
    df: pd.DataFrame,
    full_schema: pd.Series,
    col_names: Optional[list],
    stats: StreamingStats
) -> Tuple[pd.DataFrame, pd.Series, StreamingStats]:
    """Apply column filtering and return results."""
    if col_names:
        valid_cols = [c for c in col_names if c in df.columns]
        if len(valid_cols) != len(col_names):
            missing = set(col_names) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        if not valid_cols:
            raise ValueError(f"None of the requested columns exist. Available: {', '.join(df.columns)}")
        df = df[valid_cols]

    return df, full_schema, stats
