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
    pyarrow_path: Optional[str] = None,
    where: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series, Optional[StreamingStats]]:
    """Read Parquet data with streaming support.

    When pyarrow_fs and pyarrow_path are provided, uses PyArrow's native
    cloud filesystem for true streaming with range requests. This enables
    column projection to only fetch required column chunks. With ``where``,
    row groups whose min/max statistics cannot match are skipped without
    being fetched, remaining groups are filtered as they are read, and
    reading stops at ``num_rows`` matches.

    Args:
        stream: File-like object or file path containing Parquet data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.
        stats: StreamingStats instance for tracking bytes read.
        pyarrow_fs: PyArrow filesystem for native cloud access.
        pyarrow_path: Path within the PyArrow filesystem.
        where: Optional WHERE expression applied while reading.

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
            pyarrow_fs, pyarrow_path, num_rows, col_names, stats, where
        )

    # Fallback: use stream with temp file approach
    return _read_with_stream(stream, num_rows, col_names, stats, where)


def _convert_stat_value(value: str, reference) -> Optional[Any]:
    """Convert a WHERE value string to the type of a statistics min/max.

    Returns None when the types are incomparable (caller must not prune).
    """
    if isinstance(reference, bool):
        return str(value).lower() in ('true', '1', 'yes')
    if isinstance(reference, (int, float)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if isinstance(reference, bytes):
        return str(value).encode('utf-8')
    if isinstance(reference, str):
        return str(value)
    return None


def _leaf_may_match(stat_min, stat_max, op: str, value: str) -> bool:
    """Can any row in [stat_min, stat_max] satisfy (op, value)?

    Conservative: any doubt returns True (never skip rows that might match).
    """
    converted = _convert_stat_value(value, stat_min)
    if converted is None:
        return True
    lo = float(stat_min) if isinstance(stat_min, (int, float)) and not isinstance(stat_min, bool) else stat_min
    hi = float(stat_max) if isinstance(stat_max, (int, float)) and not isinstance(stat_max, bool) else stat_max

    if op == '=':
        return lo <= converted <= hi
    if op == '!=':
        return not (lo == hi == converted)
    if op == '<':
        return lo < converted
    if op == '>':
        return hi > converted
    if op == '<=':
        return lo <= converted
    if op == '>=':
        return hi >= converted
    # contains / startswith / endswith and friends can't use min/max
    return True


def _row_group_may_match(metadata, rg_idx: int, or_groups, name_to_index) -> bool:
    """Decide from statistics whether a row group could contain matches.

    or_groups is parse_where_expression output (OR of AND-leaves). Missing
    or unusable statistics never prune.
    """
    try:
        row_group = metadata.row_group(rg_idx)
        for and_group in or_groups:
            group_possible = True
            for column, op, value in and_group:
                col_idx = name_to_index.get(column)
                if col_idx is None:
                    continue  # unknown column; the real read will error clearly
                statistics = row_group.column(col_idx).statistics
                if statistics is None or not statistics.has_min_max:
                    continue
                if not _leaf_may_match(statistics.min, statistics.max, op, value):
                    group_possible = False
                    break
            if group_possible:
                return True
        return False
    except Exception:
        return True  # never let a statistics quirk skip real data


def _collect_row_groups(
    parquet_file,
    num_rows: int,
    col_names: Optional[list],
    stats: StreamingStats,
    where: Optional[str]
) -> Tuple[pd.DataFrame, list]:
    """Read row groups (skipping non-matching ones) into a DataFrame.

    Returns (df, read_group_indices). With ``where``, groups are filtered as
    they are read and reading stops at ``num_rows`` matches; without it, the
    original early-stop-by-row-count behavior applies.
    """
    metadata = parquet_file.metadata
    or_groups = None
    name_to_index = {}
    if where:
        from ..filtering import parse_where_expression, apply_where_filter
        or_groups = parse_where_expression(where)
        schema = metadata.schema
        name_to_index = {schema.column(i).name: i for i in range(metadata.num_columns)}

    frames = []
    tables = []
    read_groups = []
    first_table = None
    matched = 0
    scanned = 0
    rows_read = 0

    for i in range(parquet_file.num_row_groups):
        if num_rows > 0 and (matched if where else rows_read) >= num_rows:
            break

        if or_groups is not None and not _row_group_may_match(metadata, i, or_groups, name_to_index):
            stats.row_groups_skipped += 1
            continue

        table = parquet_file.read_row_group(i, columns=col_names)
        read_groups.append(i)

        if where:
            from ..filtering import apply_where_filter
            frame = table.to_pandas()
            if first_table is None:
                first_table = frame
            scanned += len(frame)
            hits = apply_where_filter(frame, where)
            if not hits.empty:
                if num_rows > 0 and matched + len(hits) > num_rows:
                    hits = hits.head(num_rows - matched)
                frames.append(hits)
                matched += len(hits)
        else:
            if num_rows > 0 and rows_read + table.num_rows > num_rows:
                table = table.slice(0, num_rows - rows_read)
            tables.append(table)
            rows_read += table.num_rows

    if where:
        stats.rows_scanned = scanned
        stats.where_applied = True
        if frames:
            df = pd.concat(frames, ignore_index=True)
        elif first_table is not None:
            df = first_table.head(0)
        else:
            df = parquet_file.schema_arrow.empty_table().to_pandas()
            if col_names:
                df = df[[c for c in col_names if c in df.columns]]
        return df, read_groups

    if tables:
        df = pa.concat_tables(tables).to_pandas()
    else:
        df = pd.DataFrame()
    return df, read_groups


def _read_with_native_fs(
    filesystem: Any,
    path: str,
    num_rows: int,
    col_names: Optional[list],
    stats: StreamingStats,
    where: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series, StreamingStats]:
    """Read Parquet using PyArrow native filesystem (true streaming).

    This approach uses range requests to only fetch required column chunks,
    significantly reducing data transfer for column-projected queries. With
    a WHERE expression, row groups pruned by statistics are never fetched.
    """
    stats.used_native_fs = True
    stats.is_streaming = True

    # Open file with native filesystem
    parquet_file = pq.ParquetFile(path, filesystem=filesystem)
    metadata = parquet_file.metadata

    df, read_groups = _collect_row_groups(parquet_file, num_rows, col_names, stats, where)

    # Get full schema
    full_schema = _get_schema_from_metadata(parquet_file)

    # Estimate bytes read from metadata (only the groups actually fetched)
    stats.bytes_read = _estimate_bytes_read(metadata, col_names, read_groups)

    return df, full_schema, stats


def _read_with_stream(
    stream: Union[BinaryIO, str],
    num_rows: int,
    col_names: Optional[list],
    stats: StreamingStats,
    where: Optional[str] = None
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

        df, _read_groups = _collect_row_groups(parquet_file, num_rows, col_names, stats, where)

        full_schema = _get_schema_from_metadata(parquet_file)
        return df, full_schema, stats

    finally:
        try:
            if hasattr(stream, 'read'):
                os.unlink(temp_path)
        except OSError:
            pass


def _get_schema_from_metadata(parquet_file) -> pd.Series:
    """Extract the full schema from Parquet metadata without reading any data.

    Uses the Arrow schema (already loaded with the file footer) rather than
    reading row group 0, which would transfer real column data just to learn
    the dtypes.
    """
    return parquet_file.schema_arrow.empty_table().to_pandas().dtypes


def _estimate_bytes_read(
    metadata,
    col_names: Optional[list],
    read_groups: list
) -> int:
    """Estimate bytes read based on Parquet metadata.

    Sums compressed column-chunk sizes for the row groups that were actually
    fetched (read_groups), restricted to the projected columns.
    """
    if metadata.num_rows == 0 or not read_groups:
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

    # Sum up column chunk sizes for the row groups actually read
    for rg_idx in read_groups:
        row_group = metadata.row_group(rg_idx)
        for col_idx in range(row_group.num_columns):
            col = row_group.column(col_idx)
            if col_idx in col_indices or not col_names:
                total_bytes += col.total_compressed_size

    return total_bytes
