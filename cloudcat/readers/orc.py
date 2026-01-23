"""ORC data reader."""

from typing import Optional, Tuple, Union, BinaryIO
import os
import sys
import tempfile
import pandas as pd
from colorama import Fore, Style

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
    """Read ORC data from a stream.

    Args:
        stream: File-like object or file path containing ORC data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.

    Returns:
        Tuple of (DataFrame, schema Series).

    Raises:
        SystemExit: If pyarrow with ORC support is not installed.
    """
    if not HAS_ORC:
        sys.stderr.write(
            Fore.RED + "Error: pyarrow with ORC support is required.\n" +
            "Install it with: pip install pyarrow\n" + Style.RESET_ALL
        )
        sys.exit(1)

    # For ORC, we need a temporary file
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_path = temp_file.name

    try:
        # If stream is a file-like object, copy to temp file
        if hasattr(stream, 'read'):
            with open(temp_path, 'wb') as f:
                f.write(stream.read())
        else:
            temp_path = stream

        # Read the ORC file
        orc_file = orc.ORCFile(temp_path)

        # Extract columns if specified
        col_names = [c.strip() for c in columns.split(',')] if columns else None

        # Read data
        table = orc_file.read(columns=col_names)

        # Convert to pandas
        full_df = table.to_pandas()

        # Apply num_rows limit
        if num_rows > 0 and len(full_df) > num_rows:
            full_df = full_df.head(num_rows)

        # Get full schema (read without column filter for schema)
        if columns:
            full_table = orc_file.read()
            full_schema = full_table.to_pandas().dtypes
        else:
            full_schema = full_df.dtypes

        return full_df, full_schema

    finally:
        try:
            if hasattr(stream, 'read'):
                os.unlink(temp_path)
        except OSError:
            pass  # Ignore cleanup errors
