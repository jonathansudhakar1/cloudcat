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


def _filter_after_full_read(df: pd.DataFrame, num_rows: int, where: str, stats: StreamingStats):
    """Apply a WHERE filter to a fully-read frame (array/document JSON).

    These shapes cannot stream, but marking the filter as applied keeps the
    caller from filtering twice and lets it report scan counts accurately.
    """
    from ..filtering import apply_where_filter
    stats.rows_scanned = len(df)
    stats.where_applied = True
    df = apply_where_filter(df, where)
    if num_rows > 0 and len(df) > num_rows:
        df = df.head(num_rows)
    return df


def read_json_data_streaming(
    stream: Union[BinaryIO, io.StringIO, str],
    num_rows: int,
    columns: Optional[str] = None,
    stats: Optional[StreamingStats] = None,
    where: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series, Optional[StreamingStats]]:
    """Read JSON data with streaming support.

    For JSON Lines format, uses line-by-line reading for true streaming;
    with ``where``, lines are filtered in batches as they stream and reading
    stops at ``num_rows`` matches. JSON arrays/documents must be read fully,
    then filtered.

    Args:
        stream: File-like object or string containing JSON data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.
        stats: StreamingStats instance for tracking bytes read.
        where: Optional WHERE expression applied while streaming.

    Returns:
        Tuple of (DataFrame, schema Series, StreamingStats).
    """
    if stats is None:
        stats = StreamingStats()

    stats.format_type = 'json'
    stats.rows_requested = num_rows if num_rows > 0 else None

    col_names = [c.strip() for c in columns.split(',')] if columns else None
    stats.columns_requested = col_names

    # A full-read shape reads everything first; with a filter, the row limit
    # must not truncate before filtering.
    full_read_rows = 0 if where else num_rows

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
            df, full_schema = _read_json_lines_streaming(stream, first_bytes, num_rows, stats, where)
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
            df, full_schema = _read_json_array(content, full_read_rows)
            if where:
                df = _filter_after_full_read(df, num_rows, where, stats)
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
            df, full_schema = _read_json_fallback(content, full_read_rows)
            if where:
                df = _filter_after_full_read(df, num_rows, where, stats)
            return _apply_column_filter(df, full_schema, col_names, stats)
    else:
        # String content - cannot stream
        stats.is_streaming = False
        content = stream
        stats.bytes_read = len(content.encode('utf-8'))
        df, full_schema = _read_json_fallback(content, full_read_rows)
        if where:
            df = _filter_after_full_read(df, num_rows, where, stats)
        return _apply_column_filter(df, full_schema, col_names, stats)


def _parse_json_lines(lines: list) -> pd.DataFrame:
    """Parse a batch of JSONL strings into a DataFrame (lenient fallback)."""
    content = '\n'.join(lines)
    try:
        return pd.read_json(io.StringIO(content), lines=True)
    except Exception:
        records = []
        for line in lines:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        return pd.DataFrame(records) if records else pd.DataFrame()


def _read_json_lines_streaming(
    stream: BinaryIO,
    first_bytes: bytes,
    num_rows: int,
    stats: StreamingStats,
    where: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series]:
    """Read JSON that begins with '{'.

    This handles two distinct shapes that both start with '{':
      * JSON Lines (one complete object per line) — read line-by-line so we can
        stop early once ``num_rows`` rows are collected (true streaming). With
        ``where``, lines are parsed and filtered in batches and reading stops
        at ``num_rows`` *matches* instead.
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
        df, _ = _read_json_fallback(
            content.decode('utf-8', errors='replace'), 0 if where else num_rows
        )
        if where:
            df = _filter_after_full_read(df, num_rows, where, stats)
        return df, df.dtypes

    def _iter_lines():
        """Yield complete, non-empty JSONL strings (starting with line 1)."""
        nonlocal bytes_read
        yield first_line_str
        current = b''
        for line_bytes in stream:
            bytes_read += len(line_bytes)
            current += _to_bytes(line_bytes)
            if current.endswith(b'\n'):
                text = current.decode('utf-8', errors='replace').strip()
                current = b''
                if text:
                    yield text
        if current:
            text = current.decode('utf-8', errors='replace').strip()
            if text:
                yield text

    if where:
        # Filter-as-you-stream: parse and filter in batches, stop at
        # num_rows matches. Memory stays bounded by matches + one batch.
        from ..filtering import apply_where_filter
        batch_size = 1000
        matched_frames = []
        first_frame = None
        batch = []
        matched = 0
        scanned = 0

        def _flush(batch_lines):
            nonlocal matched, first_frame
            frame = _parse_json_lines(batch_lines)
            if first_frame is None:
                first_frame = frame
            hits = apply_where_filter(frame, where)
            if not hits.empty:
                if num_rows > 0 and matched + len(hits) > num_rows:
                    hits = hits.head(num_rows - matched)
                matched_frames.append(hits)
                matched += len(hits)

        for line in _iter_lines():
            batch.append(line)
            scanned += 1
            if len(batch) >= batch_size:
                _flush(batch)
                batch = []
                if num_rows > 0 and matched >= num_rows:
                    break
        if batch and (num_rows <= 0 or matched < num_rows):
            _flush(batch)

        stats.bytes_read = bytes_read
        stats.rows_scanned = scanned
        stats.where_applied = True

        if matched_frames:
            df = pd.concat(matched_frames, ignore_index=True)
        elif first_frame is not None:
            df = first_frame.head(0)
        else:
            df = pd.DataFrame()
        return df, df.dtypes

    # No filter: collect up to num_rows lines and parse once.
    lines = []
    for line in _iter_lines():
        lines.append(line)
        if num_rows > 0 and len(lines) >= num_rows:
            break

    stats.bytes_read = bytes_read
    df = _parse_json_lines(lines) if lines else pd.DataFrame()
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
    # Strip a UTF-8 BOM (common in files from Windows tooling). str.strip()
    # does not remove it, and a leading BOM defeats every parse branch below,
    # silently yielding an empty frame for a perfectly valid file.
    content = content.lstrip('\ufeff')
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
