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
    stats: Optional[StreamingStats] = None,
    where: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series, Optional[StreamingStats]]:
    """Read Avro data with streaming support.

    Avro naturally supports streaming via fastavro.reader() iterator.
    Column filtering is applied at the record level for efficiency. With
    ``where``, records are filtered in batches as they stream and reading
    stops at ``num_rows`` matches.

    Args:
        stream: File-like object or file path containing Avro data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.
        stats: StreamingStats instance for tracking bytes read.
        where: Optional WHERE expression applied while streaming.

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

    def _consume_filtered(reader):
        """Stream records through the WHERE filter, stopping at num_rows matches."""
        from ..filtering import apply_where_filter
        batch_size = 1000
        matched_frames = []
        batch = []
        full_schema_record = None
        matched = 0
        scanned = 0

        def _flush():
            nonlocal matched
            frame = pd.DataFrame(batch)
            hits = apply_where_filter(frame, where)
            if not hits.empty:
                if num_rows > 0 and matched + len(hits) > num_rows:
                    hits = hits.head(num_rows - matched)
                matched_frames.append(hits)
                matched += len(hits)

        for record in reader:
            if full_schema_record is None:
                full_schema_record = record.copy()
            batch.append({k: v for k, v in record.items() if k in col_names}
                         if col_names else record)
            scanned += 1
            if len(batch) >= batch_size:
                _flush()
                batch = []
                if num_rows > 0 and matched >= num_rows:
                    break
        if batch and (num_rows <= 0 or matched < num_rows):
            _flush()

        stats.rows_scanned = scanned
        stats.where_applied = True
        if matched_frames:
            frame = pd.concat(matched_frames, ignore_index=True)
        else:
            frame = pd.DataFrame(columns=col_names) if col_names else pd.DataFrame()
        return frame, full_schema_record

    def _consume(reader):
        """Materialize records (and the first full record for schema)."""
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
                records.append({k: v for k, v in record.items() if k in col_names})
            else:
                records.append(record)
        return records, full_schema_record

    consume = _consume_filtered if where else _consume

    # Read the Avro file. Records must be consumed while the underlying file
    # handle is still open, so the path branch iterates inside the `with`.
    bytes_read = 0
    if hasattr(stream, 'read'):
        start_pos = stream.tell() if hasattr(stream, 'tell') else 0
        result, full_schema_record = consume(fastavro.reader(stream))
        # Try to get bytes read from stream position
        if hasattr(stream, 'tell'):
            try:
                bytes_read = stream.tell() - start_pos
            except Exception:
                bytes_read = 0
    else:
        with open(stream, 'rb') as f:
            result, full_schema_record = consume(fastavro.reader(f))

    stats.bytes_read = bytes_read

    # Validate requested columns against the actual record fields — the
    # filtered frame can't be used for this: when every requested column is
    # missing, the records all filter to {} and the frame looks "empty",
    # which previously suppressed both the warning and the error.
    if col_names and full_schema_record is not None:
        available = list(full_schema_record)
        valid_cols = [c for c in col_names if c in available]
        if len(valid_cols) != len(col_names):
            missing = set(col_names) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        if not valid_cols:
            raise ValueError(f"None of the requested columns exist. Available: {', '.join(available)}")

    # Convert to DataFrame (the filtered consumer already returns one)
    if isinstance(result, pd.DataFrame):
        df = result
    elif result:
        df = pd.DataFrame(result)
    else:
        df = pd.DataFrame()

    # Get full schema from the first complete record
    if full_schema_record:
        full_schema = pd.DataFrame([full_schema_record]).dtypes
    else:
        full_schema = df.dtypes

    return df, full_schema, stats
