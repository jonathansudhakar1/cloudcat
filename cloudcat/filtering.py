"""WHERE clause parsing and filtering utilities."""

from typing import Tuple, Any
import pandas as pd


def parse_where_clause(where_clause: str) -> Tuple[str, str, str]:
    """Parse a simple WHERE clause into column, operator, and value.

    Supports: =, !=, <, >, <=, >=, contains, startswith, endswith, not contains

    Args:
        where_clause: WHERE clause string (e.g., "status=active", "age>30").

    Returns:
        Tuple of (column, operator, value).

    Raises:
        ValueError: If the WHERE clause format is invalid.

    Examples:
        >>> parse_where_clause("status=active")
        ('status', '=', 'active')
        >>> parse_where_clause("age>30")
        ('age', '>', '30')
        >>> parse_where_clause("name contains john")
        ('name', 'contains', 'john')
    """
    lower_clause = where_clause.lower()

    # Locate the earliest comparison operator (=, !=, <, >, <=, >=), preferring
    # the longest match at that position (so '<=' beats '<').
    cmp_candidates = [
        (where_clause.find(op), -len(op), op)
        for op in ['!=', '<=', '>=', '=', '<', '>']
        if op in where_clause
    ]
    first_cmp = min(cmp_candidates) if cmp_candidates else None

    # Locate the earliest word operator (contains, startswith, ...), preferring
    # the longest match (so ' not contains ' beats ' contains ').
    word_candidates = [
        (lower_clause.find(f' {op} '), -len(op), op)
        for op in ['not contains', 'contains', 'startswith', 'endswith']
        if f' {op} ' in lower_clause
    ]
    first_word = min(word_candidates) if word_candidates else None

    # Pick whichever operator appears first in the clause. This keeps a word
    # operator inside an equality's value (e.g. "note=this contains that")
    # from hijacking the parse.
    column = op = value = None
    if first_cmp is not None and (first_word is None or first_cmp[0] < first_word[0]):
        idx, _, op = first_cmp
        column = where_clause[:idx].strip()
        value = where_clause[idx + len(op):].strip()
    elif first_word is not None:
        idx, _, op = first_word
        column = where_clause[:idx].strip()
        value = where_clause[idx + len(op) + 2:].strip()
    else:
        raise ValueError(
            f"Invalid WHERE clause: {where_clause}. "
            "Use format: column=value, column>value, column contains value, etc."
        )

    # Remove quotes if present; a quoted value is taken verbatim.
    quoted = False
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        value = value[1:-1]
        quoted = True

    # Compound conditions are not supported and would otherwise mis-parse
    # silently (AND -> "column not found"; OR -> empty result). Fail clearly —
    # but only for unquoted text: a quoted value may legitimately contain
    # the words "and"/"or" (e.g. title='Alice and Bob').
    def _has_compound(text: str) -> bool:
        padded = f' {text.lower()} '
        return ' and ' in padded or ' or ' in padded

    if _has_compound(column) or (not quoted and _has_compound(value)):
        raise ValueError(
            "Compound conditions with AND/OR are not supported. "
            "Use a single condition, e.g. column=value. "
            "If your value contains the word 'and'/'or', quote it: col='a and b'."
        )

    return column, op, value


def apply_where_filter(df: pd.DataFrame, where_clause: str) -> pd.DataFrame:
    """Apply a WHERE filter to a DataFrame.

    Args:
        df: DataFrame to filter.
        where_clause: WHERE clause string.

    Returns:
        Filtered DataFrame.

    Raises:
        ValueError: If column not found or operator is unsupported.
    """
    if not where_clause or df.empty:
        return df

    column, op, value = parse_where_clause(where_clause)

    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found. Available columns: {', '.join(df.columns)}")

    # Try to convert value to the column's type for comparison
    col_dtype = df[column].dtype
    converted_value: Any = value
    try:
        # Check bool first since is_numeric_dtype returns True for bool
        if pd.api.types.is_bool_dtype(col_dtype):
            converted_value = str(value).lower() in ('true', '1', 'yes')
        elif pd.api.types.is_numeric_dtype(col_dtype):
            converted_value = float(value) if '.' in str(value) else int(value)
    except (ValueError, TypeError):
        pass  # Keep as string

    # Apply the filter
    if op == '=':
        mask = df[column] == converted_value
    elif op == '!=':
        # Exclude nulls explicitly: pandas' NaN != x is always True, which
        # would make missing values match. SQL semantics (and the '='/string
        # operators here) all exclude NULL, so '!=' must too.
        mask = (df[column] != converted_value) & df[column].notna()
    elif op == '<':
        mask = df[column] < converted_value
    elif op == '>':
        mask = df[column] > converted_value
    elif op == '<=':
        mask = df[column] <= converted_value
    elif op == '>=':
        mask = df[column] >= converted_value
    elif op == 'contains':
        mask = df[column].astype(str).str.contains(str(value), case=False, na=False)
    elif op == 'not contains':
        mask = ~df[column].astype(str).str.contains(str(value), case=False, na=False)
    elif op == 'startswith':
        mask = df[column].astype(str).str.lower().str.startswith(str(value).lower(), na=False)
    elif op == 'endswith':
        mask = df[column].astype(str).str.lower().str.endswith(str(value).lower(), na=False)
    else:
        raise ValueError(f"Unsupported operator: {op}")

    return df[mask]
