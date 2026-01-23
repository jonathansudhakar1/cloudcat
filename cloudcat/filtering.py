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
    # Handle multi-word operators first (longer ones first to avoid partial matches)
    lower_clause = where_clause.lower()
    for op in ['not contains', 'contains', 'startswith', 'endswith']:
        if f' {op} ' in lower_clause:
            # Find the operator position in the lowercase string
            idx = lower_clause.find(f' {op} ')
            # Extract column and value using the index (preserves original case)
            column = where_clause[:idx].strip()
            value = where_clause[idx + len(op) + 2:].strip()
            # Remove quotes if present
            if (value.startswith('"') and value.endswith('"')) or \
               (value.startswith("'") and value.endswith("'")):
                value = value[1:-1]
            return column, op, value

    # Handle comparison operators
    for op in ['!=', '<=', '>=', '=', '<', '>']:
        if op in where_clause:
            parts = where_clause.split(op, 1)
            if len(parts) == 2:
                column = parts[0].strip()
                value = parts[1].strip()
                # Remove quotes if present
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
                return column, op, value

    raise ValueError(
        f"Invalid WHERE clause: {where_clause}. "
        "Use format: column=value, column>value, column contains value, etc."
    )


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
        mask = df[column] != converted_value
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
