"""WHERE clause parsing and filtering utilities."""

from typing import Tuple, List, Any
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

    # This parser handles a SINGLE condition. AND/OR splitting happens in
    # parse_where_expression before leaves reach here, so stray unquoted
    # and/or here means a malformed expression (e.g. "a=1 and and b=2") —
    # fail clearly instead of mis-parsing. A quoted value may legitimately
    # contain the words "and"/"or" (e.g. title='Alice and Bob').
    def _has_compound(text: str) -> bool:
        padded = f' {text.lower()} '
        return ' and ' in padded or ' or ' in padded

    if _has_compound(column) or (not quoted and _has_compound(value)):
        raise ValueError(
            f"Malformed condition: {where_clause!r}. "
            "Combine conditions with AND/OR between complete conditions "
            "(e.g. \"status=active AND age>30\"); quote values containing "
            "the words and/or: col='a and b'."
        )

    return column, op, value


def _split_outside_quotes(text: str, keyword: str) -> List[str]:
    """Split text on ' keyword ' (case-insensitive) occurring outside quotes.

    Quoted sections ('...' or "...") are opaque, so values like
    'Alice and Bob' never split.
    """
    parts = []
    current = []
    lower = text.lower()
    token = f' {keyword} '
    in_quote = None
    i = 0
    while i < len(text):
        ch = text[i]
        if in_quote:
            if ch == in_quote:
                in_quote = None
            current.append(ch)
            i += 1
        elif ch in ('"', "'"):
            in_quote = ch
            current.append(ch)
            i += 1
        elif lower.startswith(token, i):
            parts.append(''.join(current))
            current = []
            i += len(token)
        else:
            current.append(ch)
            i += 1
    parts.append(''.join(current))
    return parts


def parse_where_expression(where_clause: str) -> List[List[Tuple[str, str, str]]]:
    """Parse a WHERE expression with optional AND/OR into condition groups.

    AND binds tighter than OR (SQL precedence); parentheses are not supported.
    The result is OR-groups of AND-leaves: ``a=1 AND b=2 OR c=3`` becomes
    ``[[(a,=,1),(b,=,2)], [(c,=,3)]]``. Quoted values may contain the words
    and/or without being treated as connectives.

    Raises:
        ValueError: If any single condition is malformed.
    """
    or_groups = []
    for group_text in _split_outside_quotes(where_clause, 'or'):
        leaves = [
            parse_where_clause(leaf.strip())
            for leaf in _split_outside_quotes(group_text, 'and')
        ]
        or_groups.append(leaves)
    return or_groups


def where_columns(where_clause: str) -> List[str]:
    """Return every column referenced by a WHERE expression (deduplicated)."""
    seen = []
    for group in parse_where_expression(where_clause):
        for column, _op, _value in group:
            if column not in seen:
                seen.append(column)
    return seen


def _leaf_mask(df: pd.DataFrame, column: str, op: str, value: str) -> pd.Series:
    """Build the boolean mask for a single (column, op, value) condition."""
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

    if op == '=':
        return df[column] == converted_value
    elif op == '!=':
        # Exclude nulls explicitly: pandas' NaN != x is always True, which
        # would make missing values match. SQL semantics (and the '='/string
        # operators here) all exclude NULL, so '!=' must too.
        return (df[column] != converted_value) & df[column].notna()
    elif op == '<':
        return df[column] < converted_value
    elif op == '>':
        return df[column] > converted_value
    elif op == '<=':
        return df[column] <= converted_value
    elif op == '>=':
        return df[column] >= converted_value
    elif op == 'contains':
        return df[column].astype(str).str.contains(str(value), case=False, na=False)
    elif op == 'not contains':
        return ~df[column].astype(str).str.contains(str(value), case=False, na=False)
    elif op == 'startswith':
        return df[column].astype(str).str.lower().str.startswith(str(value).lower(), na=False)
    elif op == 'endswith':
        return df[column].astype(str).str.lower().str.endswith(str(value).lower(), na=False)
    raise ValueError(f"Unsupported operator: {op}")


def apply_where_filter(df: pd.DataFrame, where_clause: str) -> pd.DataFrame:
    """Apply a WHERE filter (with optional AND/OR) to a DataFrame.

    Args:
        df: DataFrame to filter.
        where_clause: WHERE expression, e.g. "status=active AND age>30".

    Returns:
        Filtered DataFrame.

    Raises:
        ValueError: If a column is not found or an operator is unsupported.
    """
    if not where_clause or df.empty:
        return df

    or_mask = None
    for group in parse_where_expression(where_clause):
        and_mask = None
        for column, op, value in group:
            leaf = _leaf_mask(df, column, op, value)
            and_mask = leaf if and_mask is None else (and_mask & leaf)
        or_mask = and_mask if or_mask is None else (or_mask | and_mask)

    return df[or_mask]
