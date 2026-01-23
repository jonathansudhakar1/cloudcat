"""Output formatting utilities."""

import json
import pandas as pd
from tabulate import tabulate
from colorama import Fore, Style


def _colorize_value(value) -> str:
    """Colorize a single JSON value based on its type."""
    # Check bool before int since bool is subclass of int
    if isinstance(value, str):
        return f'{Fore.GREEN}"{value}"{Style.RESET_ALL}'
    elif isinstance(value, bool):
        return f'{Fore.YELLOW}{str(value).lower()}{Style.RESET_ALL}'
    elif isinstance(value, (int, float)):
        return f'{Fore.CYAN}{value}{Style.RESET_ALL}'
    elif value is None:
        return f'{Fore.RED}null{Style.RESET_ALL}'
    else:
        # For complex types (lists, nested dicts), just convert to string
        return f'{json.dumps(value)}'


def colorize_json(json_str: str) -> str:
    """Add colors to JSON for better readability.

    Args:
        json_str: JSON string to colorize.

    Returns:
        Colorized JSON string for terminal display.
    """
    parsed = json.loads(json_str)

    # Handle non-list JSON (single object or primitive)
    if not isinstance(parsed, list):
        if isinstance(parsed, dict):
            return _colorize_dict(parsed)
        return _colorize_value(parsed)

    result = []
    for item in parsed:
        # Handle non-dict items in the array
        if not isinstance(item, dict):
            result.append(_colorize_value(item))
            continue

        result.append(_colorize_dict(item))

    return '\n'.join(result)


def _colorize_dict(item: dict) -> str:
    """Colorize a single dictionary."""
    item_parts = []
    item_parts.append('{')

    for i, (key, value) in enumerate(item.items()):
        # Format key
        key_str = f'  {Fore.BLUE}"{key}"{Style.RESET_ALL}: '
        val_str = _colorize_value(value)

        # Add comma if not the last item
        if i < len(item) - 1:
            item_parts.append(f"{key_str}{val_str},")
        else:
            item_parts.append(f"{key_str}{val_str}")

    item_parts.append('}')
    return '\n'.join(item_parts)


def format_table_with_colored_header(df: pd.DataFrame) -> str:
    """Format a dataframe as a table with colored and bold headers.

    Args:
        df: DataFrame to format.

    Returns:
        Formatted table string with colored headers.
    """
    if df.empty:
        return "Empty dataset"

    # Get the column headers and format them
    headers = [f"{Fore.CYAN}{Style.BRIGHT}{col}{Style.RESET_ALL}" for col in df.columns]

    # Convert the dataframe to a list of lists for tabulate
    data = df.values.tolist()

    # Use tabulate with the formatted headers
    return tabulate(data, headers, tablefmt='psql')
