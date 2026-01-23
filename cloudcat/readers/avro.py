"""Avro data reader."""

from typing import Optional, Tuple, Union, BinaryIO
import sys
import pandas as pd
import click
from colorama import Fore, Style

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
    """Read Avro data from a stream.

    Args:
        stream: File-like object or file path containing Avro data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.

    Returns:
        Tuple of (DataFrame, schema Series).

    Raises:
        SystemExit: If fastavro is not installed.
    """
    if not HAS_AVRO:
        sys.stderr.write(
            Fore.RED + "Error: fastavro package is required for Avro support.\n" +
            "Install it with: pip install fastavro\n" + Style.RESET_ALL
        )
        sys.exit(1)

    # Read the Avro file
    if hasattr(stream, 'read'):
        reader = fastavro.reader(stream)
    else:
        with open(stream, 'rb') as f:
            reader = fastavro.reader(f)

    # Read records into a list
    records = []
    for i, record in enumerate(reader):
        if num_rows > 0 and i >= num_rows:
            break
        records.append(record)

    # Convert to DataFrame
    if records:
        full_df = pd.DataFrame(records)
    else:
        full_df = pd.DataFrame()

    # Store the full schema for later use
    full_schema = full_df.dtypes

    # Apply column filtering if specified
    if columns and not full_df.empty:
        cols = [c.strip() for c in columns.split(',')]
        valid_cols = [c for c in cols if c in full_df.columns]
        if len(valid_cols) != len(cols):
            missing = set(cols) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        df = full_df[valid_cols]
    else:
        df = full_df

    return df, full_schema
