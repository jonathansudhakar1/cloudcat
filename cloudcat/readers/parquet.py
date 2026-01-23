"""Parquet data reader."""

from typing import Optional, Tuple, Union, BinaryIO
import io
import os
import sys
import tempfile
import pandas as pd
from colorama import Fore, Style

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
    """Read Parquet data from a stream.

    Args:
        stream: File-like object or file path containing Parquet data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.

    Returns:
        Tuple of (DataFrame, schema Series).

    Raises:
        SystemExit: If pyarrow is not installed.
    """
    if not HAS_PARQUET:
        sys.stderr.write(
            Fore.RED + "Error: pyarrow package is required for Parquet support.\n" +
            "Install it with: pip install pyarrow\n" + Style.RESET_ALL
        )
        sys.exit(1)

    # For Parquet, we need a temporary file to properly read the metadata
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_path = temp_file.name

    try:
        # If stream is a file-like object, copy to temp file
        if hasattr(stream, 'read'):
            with open(temp_path, 'wb') as f:
                f.write(stream.read())
        else:
            # Assume it's already a path
            temp_path = stream

        parquet_file = pq.ParquetFile(temp_path)

        # Extract columns if specified for filtering
        col_names = columns.split(',') if columns else None

        # Read the data efficiently
        if num_rows > 0:
            tables = []
            rows_read = 0

            for i in range(parquet_file.num_row_groups):
                if rows_read >= num_rows:
                    break

                table = parquet_file.read_row_group(i, columns=col_names)

                # Limit rows if needed for the last batch
                if rows_read + table.num_rows > num_rows:
                    table = table.slice(0, num_rows - rows_read)

                tables.append(table)
                rows_read += min(table.num_rows, num_rows - rows_read)

            if tables:
                result_table = pa.concat_tables(tables)
                df = result_table.to_pandas()
            else:
                df = pd.DataFrame()
        else:
            # Read all data (potentially with column filtering)
            table = parquet_file.read(columns=col_names)
            df = table.to_pandas()

        # Get the full schema as a pandas Series for consistency with other formats
        if parquet_file.num_row_groups > 0:
            # Read just first row group
            sample_table = parquet_file.read_row_group(0)
            if sample_table.num_rows > 1:
                # Slice to just the first row if needed
                sample_table = sample_table.slice(0, 1)
            full_df = sample_table.to_pandas()
        else:
            # If no row groups, create empty DataFrame with correct schema
            full_df = df.iloc[0:0] if not df.empty else pd.DataFrame()

        full_schema = full_df.dtypes

        return df, full_schema

    finally:
        # Clean up the temporary file
        try:
            if hasattr(stream, 'read'):  # Only delete if we created temp file
                os.unlink(temp_path)
        except OSError:
            pass  # Ignore cleanup errors
