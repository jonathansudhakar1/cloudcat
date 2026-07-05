"""Output formatting utilities.

Rendering is type-aware and colorized for terminals. Colors are emitted
unconditionally here; the CLI initializes colorama to strip them when output is
piped, when --no-color is set, or when writing to a file.
"""

import json
import pandas as pd
from tabulate import tabulate
from colorama import Fore, Style

# --- Palette ---------------------------------------------------------------
_HEADER = Fore.CYAN + Style.BRIGHT   # table headers / json keys (bold)
_KEY = Fore.BLUE                     # json object keys
_NUM = Fore.CYAN                     # numbers
_STR = Fore.GREEN                    # strings (json)
_TRUE = Fore.GREEN                   # boolean true
_FALSE = Fore.RED                    # boolean false
_NULL = Style.DIM                    # null / missing
_RESET = Style.RESET_ALL

_NULL_MARKER = "∘"  # ∘  shown for missing table cells


# --- Shared helpers --------------------------------------------------------
def _is_null(value) -> bool:
    """True if a scalar value is null/NaN. Safe for non-scalars (lists/dicts)."""
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _num_to_str(value) -> str:
    """Render a number faithfully (no forced decimals, no precision loss)."""
    return str(value)


# --- Table -----------------------------------------------------------------
def _column_kind(dtype) -> str:
    """Classify a column dtype as 'bool', 'num', or 'other'."""
    # bool must be checked first: is_numeric_dtype is True for bool too.
    if pd.api.types.is_bool_dtype(dtype):
        return "bool"
    if pd.api.types.is_numeric_dtype(dtype):
        return "num"
    return "other"


def _format_table_cell(value, kind: str) -> str:
    """Colorize a single table cell based on its column kind / value type."""
    if _is_null(value):
        return f"{_NULL}{_NULL_MARKER}{_RESET}"

    if kind == "bool" or isinstance(value, bool):
        return f"{_TRUE}true{_RESET}" if value else f"{_FALSE}false{_RESET}"

    if kind == "num" or (isinstance(value, (int, float)) and not isinstance(value, bool)):
        return f"{_NUM}{_num_to_str(value)}{_RESET}"

    # Strings and everything else: default terminal color.
    return str(value)


def format_table_with_colored_header(df: pd.DataFrame) -> str:
    """Format a DataFrame as a rounded, type-colored table.

    Headers are bold cyan; numbers are cyan and right-aligned; booleans render
    as colored true/false; missing values render as a dim ∘. Cells are
    pre-formatted (and numbers explicitly right-aligned) with number parsing
    disabled so the embedded color codes never disturb column alignment.

    Args:
        df: DataFrame to format.

    Returns:
        Formatted table string.
    """
    if df.empty:
        return "Empty dataset"

    headers = [f"{_HEADER}{col}{_RESET}" for col in df.columns]
    # Iterate dtypes positionally: df[col] on a duplicated column name (legal
    # in Parquet) returns a DataFrame, which has no .dtype and would crash.
    kinds = [_column_kind(dtype) for dtype in df.dtypes]
    colalign = ["right" if kind == "num" else "left" for kind in kinds]

    rows = [
        [_format_table_cell(value, kind) for value, kind in zip(row, kinds)]
        for row in df.itertuples(index=False, name=None)
    ]

    return tabulate(
        rows,
        headers,
        tablefmt="rounded_outline",
        colalign=colalign,
        disable_numparse=True,
    )


# --- JSON (pretty) ---------------------------------------------------------
def _colorize_scalar(value) -> str:
    """Colorize a single JSON scalar value."""
    # Check bool before int/float since bool is a subclass of int.
    if isinstance(value, bool):
        return f"{_TRUE}true{_RESET}" if value else f"{_FALSE}false{_RESET}"
    if value is None:
        return f"{_NULL}null{_RESET}"
    if isinstance(value, (int, float)):
        return f"{_NUM}{json.dumps(value)}{_RESET}"
    if isinstance(value, str):
        return f"{_STR}{json.dumps(value)}{_RESET}"
    # Fallback for anything unexpected.
    return json.dumps(value)


def _render_json(value, depth: int) -> str:
    """Recursively render a parsed JSON value with indentation and color."""
    indent = "  " * (depth + 1)
    closing_indent = "  " * depth

    if isinstance(value, dict):
        if not value:
            return "{}"
        items = ",\n".join(
            f"{indent}{_KEY}{json.dumps(key)}{_RESET}: {_render_json(val, depth + 1)}"
            for key, val in value.items()
        )
        return "{\n" + items + "\n" + closing_indent + "}"

    if isinstance(value, list):
        if not value:
            return "[]"
        items = ",\n".join(
            f"{indent}{_render_json(item, depth + 1)}" for item in value
        )
        return "[\n" + items + "\n" + closing_indent + "]"

    return _colorize_scalar(value)


def colorize_json(json_str: str) -> str:
    """Pretty-print and colorize a JSON string for terminal display.

    Nested arrays and objects are indented and colorized recursively.

    Args:
        json_str: JSON string to colorize.

    Returns:
        Indented, colorized JSON string.
    """
    try:
        parsed = json.loads(json_str)
    except (json.JSONDecodeError, ValueError):
        return json_str
    return _render_json(parsed, 0)
