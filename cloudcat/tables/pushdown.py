"""Shared WHERE-pushdown helpers for the table adapters.

Translation is all-or-nothing: if every leaf of the WHERE expression maps to
the engine's filter language, the engine prunes files/rows for us; otherwise
the adapter reads (column-projected) data and cloudcat's own filter does the
work. Either way apply_where_filter runs locally afterwards, so the final
row semantics are always cloudcat's — engine filters only *reduce* data.
"""

from typing import Any, Optional, Tuple

import pandas as pd

from ..filtering import parse_where_expression, apply_where_filter
from ..streaming import StreamingStats

# Leaf operators the engines can evaluate with semantics equivalent to ours
# (comparisons exclude NULLs on both sides; string equality is case-sensitive).
PUSHABLE_OPS = ('=', '!=', '<', '>', '<=', '>=')


def convert_value(value: str, arrow_type) -> Any:
    """Convert a WHERE literal to a Python value matching an Arrow type.

    Raises ValueError when the literal can't represent the type — callers
    treat that as "not translatable" and fall back to a local filter.
    """
    import pyarrow.types as pat

    if pat.is_boolean(arrow_type):
        return str(value).lower() in ('true', '1', 'yes')
    if pat.is_integer(arrow_type):
        return int(value)
    if pat.is_floating(arrow_type) or pat.is_decimal(arrow_type):
        return float(value)
    if pat.is_string(arrow_type) or pat.is_large_string(arrow_type):
        return str(value)
    raise ValueError(f"cannot push literal for arrow type {arrow_type}")


def to_arrow_expression(where: Optional[str], arrow_schema) -> Optional[Any]:
    """Translate a WHERE expression to a pyarrow.dataset filter, or None.

    None means "not fully translatable" — the adapter must read without an
    engine filter and let cloudcat filter locally.
    """
    if not where:
        return None
    import pyarrow.dataset as ds

    try:
        or_expr = None
        for group in parse_where_expression(where):
            and_expr = None
            for column, op, value in group:
                if op not in PUSHABLE_OPS:
                    return None
                field = arrow_schema.field(column)  # KeyError -> not translatable
                literal = convert_value(value, field.type)
                ref = ds.field(column)
                leaf = {
                    '=': ref == literal,
                    '!=': ref != literal,
                    '<': ref < literal,
                    '>': ref > literal,
                    '<=': ref <= literal,
                    '>=': ref >= literal,
                }[op]
                and_expr = leaf if and_expr is None else (and_expr & leaf)
            or_expr = and_expr if or_expr is None else (or_expr | and_expr)
        return or_expr
    except (KeyError, ValueError, TypeError):
        return None


def finalize(
    df: pd.DataFrame,
    num_rows: int,
    offset: int,
    where: Optional[str],
    stats: StreamingStats,
) -> Tuple[pd.DataFrame, StreamingStats]:
    """Apply cloudcat's filter/offset/limit semantics to an engine result.

    The engine may have pre-filtered and pre-limited; this pass is the
    source of truth (identical coercion rules to the plain-file paths) and
    handles offset-pages-through-matches.
    """
    if where:
        scanned = len(df)
        df = apply_where_filter(df, where)
        stats.where_applied = True
        if stats.rows_scanned is None:
            stats.rows_scanned = scanned

    target = (offset + num_rows) if num_rows > 0 else 0
    if target and len(df) > target:
        df = df.head(target)
    if offset > 0:
        df = df.iloc[offset:].reset_index(drop=True)

    return df, stats
