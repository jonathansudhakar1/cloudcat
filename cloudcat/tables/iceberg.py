"""Apache Iceberg table adapter (via the `pyiceberg` package).

Reads catalog-less: the newest metadata.json under <table>/metadata/ is
resolved by listing, then loaded as a StaticTable over PyArrow IO — no
catalog service, no fsspec extras.
"""

from typing import List, Optional, Tuple

import click
import pandas as pd
from colorama import Fore, Style

from ..streaming import StreamingStats
from . import table_uri, latest_iceberg_metadata
from .pushdown import PUSHABLE_OPS, convert_value, finalize
from ..filtering import parse_where_expression

try:
    from pyiceberg.table import StaticTable
    HAS_ICEBERG = True
except ImportError:
    StaticTable = None
    HAS_ICEBERG = False

_INSTALL_HINT = (
    "Iceberg support requires the pyiceberg package. "
    "Install with: pip install 'cloudcat[iceberg]'"
)

def _io_properties() -> dict:
    """PyArrow IO for catalog-less reads, plus any custom S3 endpoint."""
    from ..config import cloud_config
    properties = {"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"}
    endpoint = cloud_config.resolve_s3_endpoint()
    if endpoint:
        properties["s3.endpoint"] = endpoint
        properties["s3.region"] = "auto"
    return properties


def _require():
    if not HAS_ICEBERG:
        raise ValueError(_INSTALL_HINT)


def _open(service: str, bucket: str, prefix: str):
    _require()
    metadata_key = latest_iceberg_metadata(service, bucket, prefix)
    metadata_uri = table_uri(service, bucket, metadata_key)
    if service == 'local':
        metadata_uri = 'file://' + metadata_uri
    return StaticTable.from_metadata(metadata_uri, properties=_io_properties())


def _to_iceberg_expression(where: Optional[str], arrow_schema):
    """Translate WHERE to a pyiceberg expression, or None if not fully pushable."""
    if not where:
        return None
    from pyiceberg.expressions import (
        And, Or, EqualTo, NotEqualTo, LessThan, GreaterThan,
        LessThanOrEqual, GreaterThanOrEqual,
    )
    builders = {
        '=': EqualTo, '!=': NotEqualTo, '<': LessThan, '>': GreaterThan,
        '<=': LessThanOrEqual, '>=': GreaterThanOrEqual,
    }
    try:
        or_expr = None
        for group in parse_where_expression(where):
            and_expr = None
            for column, op, value in group:
                if op not in PUSHABLE_OPS:
                    return None
                field = arrow_schema.field(column)  # KeyError -> not pushable
                leaf = builders[op](column, convert_value(value, field.type))
                and_expr = leaf if and_expr is None else And(and_expr, leaf)
            or_expr = and_expr if or_expr is None else Or(or_expr, and_expr)
        return or_expr
    except (KeyError, ValueError, TypeError):
        return None


def _validate_columns(requested: Optional[str], arrow_schema) -> Optional[List[str]]:
    if not requested:
        return None
    names = [c.strip() for c in requested.split(',')]
    available = set(arrow_schema.names)
    valid = [c for c in names if c in available]
    if len(valid) != len(names):
        missing = set(names) - available
        click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL, err=True)
    if not valid:
        raise ValueError(f"None of the requested columns exist. Available: {', '.join(arrow_schema.names)}")
    return valid


def read(
    service: str,
    bucket: str,
    prefix: str,
    num_rows: int,
    columns: Optional[str] = None,
    offset: int = 0,
    where: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.Series, StreamingStats]:
    """Preview the current snapshot of an Iceberg table."""
    table = _open(service, bucket, prefix)
    arrow_schema = table.schema().as_arrow()
    cols = _validate_columns(columns, arrow_schema)

    stats = StreamingStats(format_type='iceberg', is_streaming=True, used_native_fs=True)

    snapshot = table.current_snapshot()
    if snapshot is not None:
        summary = snapshot.summary or {}
        files = summary.get('total-data-files', '?')
        click.echo(
            Fore.BLUE + f"Iceberg table: snapshot {snapshot.snapshot_id} · {files} data file(s)"
            + Style.RESET_ALL, err=True,
        )

    expression = _to_iceberg_expression(where, arrow_schema)
    target = (offset + num_rows) if num_rows > 0 else 0

    scan_kwargs = {}
    if cols:
        scan_kwargs['selected_fields'] = tuple(cols)
    if expression is not None:
        scan_kwargs['row_filter'] = expression
        if target:
            scan_kwargs['limit'] = target
    elif not where and target:
        # No filter at all: a plain limited preview.
        scan_kwargs['limit'] = target
    # where present but not pushable: no limit — read all, filter locally.

    df = table.scan(**scan_kwargs).to_arrow().to_pandas()
    full_schema = arrow_schema.empty_table().to_pandas().dtypes

    df, stats = finalize(df, num_rows, offset, where, stats)
    return df, full_schema, stats


def row_count(service: str, bucket: str, prefix: str) -> int:
    """Exact row count of the current snapshot (from snapshot metadata)."""
    table = _open(service, bucket, prefix)
    snapshot = table.current_snapshot()
    if snapshot is None:
        return 0
    summary = snapshot.summary or {}
    total = summary.get('total-records')
    if total is not None:
        return int(total)
    # Rare: no summary counts — fall back to an actual scan.
    return len(table.scan().to_arrow())
