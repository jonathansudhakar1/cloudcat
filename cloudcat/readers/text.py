"""Plain text data reader."""

from typing import Optional, Tuple, Union, BinaryIO
import pandas as pd
import click
from colorama import Fore, Style


def read_text_data(
    stream: Union[BinaryIO, str],
    num_rows: int,
    columns: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series]:
    """Read plain text data from a stream, treating each line as a row.

    Args:
        stream: File-like object or file path containing text data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.

    Returns:
        Tuple of (DataFrame, schema Series).
    """
    # Read the content
    if hasattr(stream, 'read'):
        content = stream.read()
        if isinstance(content, bytes):
            content = content.decode('utf-8')
    else:
        with open(stream, 'r') as f:
            content = f.read()

    # Split into lines
    lines = content.splitlines()

    # Apply num_rows limit
    if num_rows > 0:
        lines = lines[:num_rows]

    # Create DataFrame with a single 'line' column
    full_df = pd.DataFrame({'line': lines, 'line_number': range(1, len(lines) + 1)})

    # Store the full schema
    full_schema = full_df.dtypes

    # Apply column filtering if specified
    if columns:
        cols = [c.strip() for c in columns.split(',')]
        valid_cols = [c for c in cols if c in full_df.columns]
        if len(valid_cols) != len(cols):
            missing = set(cols) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        df = full_df[valid_cols]
    else:
        df = full_df

    return df, full_schema
