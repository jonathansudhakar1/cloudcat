"""CSV data reader."""

from typing import Optional, Tuple, Union, BinaryIO
import io
import pandas as pd
import click
from colorama import Fore, Style


def read_csv_data(
    stream: Union[BinaryIO, io.StringIO],
    num_rows: int,
    columns: Optional[str] = None,
    delimiter: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series]:
    """Read CSV data from a stream.

    Args:
        stream: File-like object containing CSV data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.
        delimiter: Custom delimiter character.

    Returns:
        Tuple of (DataFrame, schema Series).
    """
    # First read the data without column filtering to get full schema
    pd_args = {'nrows': num_rows} if num_rows > 0 else {}

    # Add delimiter if specified
    if delimiter:
        pd_args['delimiter'] = delimiter

    full_df = pd.read_csv(stream, **pd_args)

    # Store the full schema for later use
    full_schema = full_df.dtypes

    # Apply column filtering if specified
    if columns:
        cols = [c.strip() for c in columns.split(',')]
        valid_cols = [c for c in cols if c in full_df.columns]
        if len(valid_cols) != len(cols):
            missing = set(cols) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        if not valid_cols:
            raise ValueError(f"None of the requested columns exist. Available: {', '.join(full_df.columns)}")
        df = full_df[valid_cols]
    else:
        df = full_df

    # Return both the filtered dataframe and the full schema
    return df, full_schema
