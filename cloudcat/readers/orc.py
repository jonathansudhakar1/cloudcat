"""ORC data reader."""

from typing import Optional, Tuple, Union, BinaryIO, Any
import os
import sys
import tempfile
import pandas as pd
from colorama import Fore, Style

from ..streaming import StreamingStats

# Try to import ORC support
try:
    import pyarrow.orc as orc
    HAS_ORC = True
except ImportError:
    orc = None
    HAS_ORC = False


def read_orc_data(
    stream: Union[BinaryIO, str],
    num_rows: int,
    columns: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series]:
    """Read ORC data from a stream (legacy interface).

    Args:
        stream: File-like object or file path containing ORC data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.

    Returns:
        Tuple of (DataFrame, schema Series).

    Raises:
        SystemExit: If pyarrow with ORC support is not installed.
    """
    df, schema, _ = read_orc_data_streaming(
        stream=stream,
        num_rows=num_rows,
        columns=columns,
        stats=None
    )
    return df, schema


def read_orc_data_streaming(
    stream: Union[BinaryIO, str, None] = None,
    num_rows: int = 0,
    columns: Optional[str] = None,
    stats: Optional[StreamingStats] = None,
    pyarrow_fs: Optional[Any] = None,
    pyarrow_path: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series, Optional[StreamingStats]]:
    """Read ORC data with streaming support.

    When pyarrow_fs and pyarrow_path are provided, uses PyArrow's native
    cloud filesystem for true streaming with range requests.

    Args:
        stream: File-like object or file path containing ORC data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.
        stats: StreamingStats instance for tracking bytes read.
        pyarrow_fs: PyArrow filesystem for native cloud access.
        pyarrow_path: Path within the PyArrow filesystem.

    Returns:
        Tuple of (DataFrame, schema Series, StreamingStats).

    Raises:
        SystemExit: If pyarrow with ORC support is not installed.
    """
    if not HAS_ORC:
        sys.stderr.write(
            Fore.RED + "Error: pyarrow with ORC support is required.\n" +
            "Install it with: pip install pyarrow\n" + Style.RESET_ALL
        )
        sys.exit(1)

    if stats is None:
        stats = StreamingStats()

    stats.format_type = 'orc'
    stats.rows_requested = num_rows if num_rows > 0 else None

    col_names = [c.strip() for c in columns.split(',')] if columns else None
    stats.columns_requested = col_names

    # Use native PyArrow filesystem if available
    if pyarrow_fs is not None and pyarrow_path is not None:
        return _read_with_native_fs(
            pyarrow_fs, pyarrow_path, num_rows, col_names, stats
        )

    # Fallback: use stream with temp file approach
    return _read_with_stream(stream, num_rows, col_names, stats)


def _read_orc_rows(orc_file, num_rows: int, col_names: Optional[list]) -> pd.DataFrame:
    """Read up to num_rows from an open ORCFile, stopping early by stripe.

    Reading stripe-by-stripe avoids materializing the entire table when only a
    small preview is requested.
    """
    if num_rows <= 0:
        return orc_file.read(columns=col_names).to_pandas()

    import pyarrow as pa
    batches = []  # list of pyarrow.RecordBatch
    rows_read = 0
    for i in range(orc_file.nstripes):
        if rows_read >= num_rows:
            break
        stripe = orc_file.read_stripe(i, columns=col_names)
        if rows_read + stripe.num_rows > num_rows:
            stripe = stripe.slice(0, num_rows - rows_read)
        batches.append(stripe)
        rows_read += stripe.num_rows

    if not batches:
        # Empty file: return a correctly-typed empty frame.
        return orc_file.read(columns=col_names).to_pandas().head(0)
    return pa.Table.from_batches(batches).to_pandas()


def _full_schema_dtypes(orc_file) -> pd.Series:
    """Derive the full (all-column) schema dtypes without reading any data."""
    return orc_file.schema.empty_table().to_pandas().dtypes


def _read_with_native_fs(
    filesystem: Any,
    path: str,
    num_rows: int,
    col_names: Optional[list],
    stats: StreamingStats
) -> Tuple[pd.DataFrame, pd.Series, StreamingStats]:
    """Read ORC using PyArrow native filesystem."""
    stats.used_native_fs = True
    stats.is_streaming = True

    # Open with native filesystem
    with filesystem.open_input_file(path) as f:
        orc_file = orc.ORCFile(f)

        # Read data with column projection, stopping early by stripe.
        full_df = _read_orc_rows(orc_file, num_rows, col_names)

        # Full schema is derived from metadata only (no extra data read).
        full_schema = _full_schema_dtypes(orc_file)

        # Estimate bytes read (ORC metadata access is limited)
        # Use file size from metadata if available
        stats.bytes_read = orc_file.nbytes if hasattr(orc_file, 'nbytes') else stats.file_size

    return full_df, full_schema, stats


def _read_with_stream(
    stream: Union[BinaryIO, str],
    num_rows: int,
    col_names: Optional[list],
    stats: StreamingStats
) -> Tuple[pd.DataFrame, pd.Series, StreamingStats]:
    """Read ORC from a stream (fallback for compressed files)."""
    stats.used_native_fs = False
    stats.is_streaming = False

    # For ORC, we need a temporary file
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
            temp_path = stream
            stats.bytes_read = os.path.getsize(temp_path) if os.path.exists(temp_path) else 0

        # Read the ORC file
        orc_file = orc.ORCFile(temp_path)

        # Read data with column projection, stopping early by stripe.
        full_df = _read_orc_rows(orc_file, num_rows, col_names)

        # Full schema is derived from metadata only (no extra data read).
        full_schema = _full_schema_dtypes(orc_file)

        return full_df, full_schema, stats

    finally:
        try:
            if hasattr(stream, 'read'):
                os.unlink(temp_path)
        except OSError:
            pass
