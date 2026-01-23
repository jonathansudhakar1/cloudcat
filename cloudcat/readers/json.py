"""JSON data reader."""

from typing import Optional, Tuple, Union, BinaryIO
import io
import json
import pandas as pd
import click
from colorama import Fore, Style


def read_json_data(
    stream: Union[BinaryIO, io.StringIO, str],
    num_rows: int,
    columns: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.Series]:
    """Read JSON data from a stream. Supports both JSON Lines and regular JSON formats.

    Args:
        stream: File-like object or string containing JSON data.
        num_rows: Maximum number of rows to read (0 for all).
        columns: Comma-separated list of columns to select.

    Returns:
        Tuple of (DataFrame, schema Series).
    """
    # Read the raw content to determine the format
    if hasattr(stream, 'read'):
        content = stream.read()
        if isinstance(content, bytes):
            content = content.decode('utf-8')
    else:
        content = stream

    # Try to detect if it's JSON Lines or regular JSON
    content_stripped = content.strip()
    is_json_lines = False

    if content_stripped:
        # JSON Lines starts with { or [ on each line, not a single array/object
        first_char = content_stripped[0]
        if first_char == '{':
            # Could be JSON Lines or a single JSON object
            lines = [line.strip() for line in content_stripped.split('\n') if line.strip()]
            if len(lines) > 1 and all(line.startswith('{') for line in lines[:min(5, len(lines))]):
                is_json_lines = True
        elif first_char == '[':
            # Regular JSON array
            is_json_lines = False

    # Create a new stream from the content for pandas
    content_stream = io.StringIO(content)

    try:
        if is_json_lines:
            # JSON Lines format
            if num_rows > 0:
                full_df = pd.read_json(content_stream, lines=True, nrows=num_rows)
            else:
                full_df = pd.read_json(content_stream, lines=True)
        else:
            # Regular JSON (array or object)
            parsed = json.loads(content)

            # Handle different JSON structures
            if isinstance(parsed, list):
                # JSON array - convert to dataframe
                full_df = pd.DataFrame(parsed)
            elif isinstance(parsed, dict):
                # Single JSON object - treat as single row
                full_df = pd.DataFrame([parsed])
            else:
                raise ValueError("JSON must be an array or object")

            # Apply num_rows limit
            if num_rows > 0 and len(full_df) > num_rows:
                full_df = full_df.head(num_rows)
    except json.JSONDecodeError:
        # Fall back to trying JSON Lines if regular JSON parsing fails
        content_stream = io.StringIO(content)
        if num_rows > 0:
            full_df = pd.read_json(content_stream, lines=True, nrows=num_rows)
        else:
            full_df = pd.read_json(content_stream, lines=True)

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
