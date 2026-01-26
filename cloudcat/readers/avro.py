"""Avro data reader."""

from typing import Optional, Tuple, Union, BinaryIO
import sys
import pandas as pd
import click
from colorama import Fore, Style

from ..streaming import StreamingStats

# Try to import Avro support
try:
    import fastavro
    HAS_AVRO = True
except ImportError:
    fastavro = None
    HAS_AVRO = False


def read_avro_data(
    stream: Union[BinaryIO, str],
    num_rows: int,
    columns: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series]:
    """Read Avro data from a stream (legacy interface).

    Args:
        stream: File-like object or file path containing Avro data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.

    Returns:
        Tuple of (DataFrame, schema Series).

    Raises:
        SystemExit: If fastavro is not installed.
    """
    df, schema, _ = read_avro_data_streaming(
        stream=stream,
        num_rows=num_rows,
        columns=columns,
        stats=None
    )
    return df, schema


def read_avro_data_streaming(
    stream: Union[BinaryIO, str],
    num_rows: int,
    columns: Optional[str] = None,
    stats: Optional[StreamingStats] = None
) -> Tuple[pd.DataFrame, pd.Series, Optional[StreamingStats]]:
    """Read Avro data with streaming support.

    Avro naturally supports streaming via fastavro.reader() iterator.
    Column filtering is applied at the record level for efficiency.

    Args:
        stream: File-like object or file path containing Avro data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.
        stats: StreamingStats instance for tracking bytes read.

    Returns:
        Tuple of (DataFrame, schema Series, StreamingStats).

    Raises:
        SystemExit: If fastavro is not installed.
    """
    if not HAS_AVRO:
        sys.stderr.write(
            Fore.RED + "Error: fastavro package is required for Avro support.\n" +
            "Install it with: pip install fastavro\n" + Style.RESET_ALL
        )
        sys.exit(1)

    if stats is None:
        stats = StreamingStats()

    stats.format_type = 'avro'
    stats.rows_requested = num_rows if num_rows > 0 else None
    stats.is_streaming = True  # Avro reader is naturally streaming

    col_names = [c.strip() for c in columns.split(',')] if columns else None
    stats.columns_requested = col_names

    # Read the Avro file
    bytes_read = 0
    if hasattr(stream, 'read'):
        # Track initial position if possible
        start_pos = stream.tell() if hasattr(stream, 'tell') else 0
        reader = fastavro.reader(stream)
    else:
        with open(stream, 'rb') as f:
            reader = fastavro.reader(f)

    # Read records into a list, optionally filtering columns at record level
    records = []
    full_schema_record = None

    for i, record in enumerate(reader):
        if num_rows > 0 and i >= num_rows:
            break

        # Store first full record for schema
        if full_schema_record is None:
            full_schema_record = record.copy()

        # Apply column filtering at record level for efficiency
        if col_names:
            filtered_record = {k: v for k, v in record.items() if k in col_names}
            records.append(filtered_record)
        else:
            records.append(record)

    # Try to get bytes read from stream position
    if hasattr(stream, 'tell'):
        try:
            bytes_read = stream.tell() - start_pos
        except Exception:
            bytes_read = 0

    stats.bytes_read = bytes_read

    # Convert to DataFrame
    if records:
        df = pd.DataFrame(records)
    else:
        df = pd.DataFrame()

    # Get full schema from the first complete record
    if full_schema_record:
        full_schema = pd.DataFrame([full_schema_record]).dtypes
    else:
        full_schema = df.dtypes

    # Validate columns if filtering was requested
    if col_names and not df.empty:
        valid_cols = [c for c in col_names if c in df.columns]
        if len(valid_cols) != len(col_names):
            missing = set(col_names) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)

    return df, full_schema, stats
