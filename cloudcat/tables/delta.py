"""Delta Lake table adapter (via the `deltalake` package)."""

import os
from typing import List, Optional, Tuple

import click
import pandas as pd
from colorama import Fore, Style

from ..streaming import StreamingStats
from . import table_uri
from .pushdown import to_arrow_expression, finalize

try:
    from deltalake import DeltaTable
    HAS_DELTA = True
except ImportError:
    DeltaTable = None
    HAS_DELTA = False

_INSTALL_HINT = (
    "Delta Lake support requires the deltalake package. "
    "Install with: pip install 'cloudcat[delta]'"
)


def _require():
    if not HAS_DELTA:
        raise ValueError(_INSTALL_HINT)


def _storage_env():
    """Best-effort: surface cloudcat's credential flags to delta-rs via env."""
    from ..config import cloud_config
    if cloud_config.aws_profile:
        os.environ.setdefault('AWS_PROFILE', cloud_config.aws_profile)
    if cloud_config.gcp_credentials:
        os.environ.setdefault('GOOGLE_APPLICATION_CREDENTIALS', cloud_config.gcp_credentials)
    if cloud_config.azure_access_key:
        os.environ.setdefault('AZURE_STORAGE_ACCOUNT_KEY', cloud_config.azure_access_key)


def _open(service: str, bucket: str, prefix: str):
    _require()
    _storage_env()
    return DeltaTable(table_uri(service, bucket, prefix))


def _validate_columns(requested: Optional[str], schema) -> Optional[List[str]]:
    """Parse/validate a comma-separated column list against an Arrow schema."""
    if not requested:
        return None
    names = [c.strip() for c in requested.split(',')]
    available = set(schema.names)
    valid = [c for c in names if c in available]
    if len(valid) != len(names):
        missing = set(names) - available
        click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL, err=True)
    if not valid:
        raise ValueError(f"None of the requested columns exist. Available: {', '.join(schema.names)}")
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
    """Preview the current snapshot of a Delta table."""
    dt = _open(service, bucket, prefix)
    dataset = dt.to_pyarrow_dataset()
    cols = _validate_columns(columns, dataset.schema)

    stats = StreamingStats(format_type='delta', is_streaming=True, used_native_fs=True)

    click.echo(
        Fore.BLUE + f"Delta table: version {dt.version()} · {len(dt.file_uris())} live data file(s)"
        + Style.RESET_ALL, err=True,
    )

    # Engine pushdown when the whole WHERE translates; otherwise read
    # (column-projected) fully and let cloudcat's filter decide.
    expression = to_arrow_expression(where, dataset.schema)
    target = (offset + num_rows) if num_rows > 0 else 0

    if where and expression is None:
        table = dataset.to_table(columns=cols)
    elif target:
        table = dataset.head(target, columns=cols, filter=expression)
    else:
        table = dataset.to_table(columns=cols, filter=expression)

    df = table.to_pandas()
    full_schema = dataset.schema.empty_table().to_pandas().dtypes

    df, stats = finalize(df, num_rows, offset, where, stats)
    return df, full_schema, stats


def row_count(service: str, bucket: str, prefix: str) -> int:
    """Exact row count of the current snapshot.

    Prefers per-file record counts from the transaction log; falls back to
    Parquet footer counting (still metadata-only, no row data read).
    """
    dt = _open(service, bucket, prefix)
    try:
        actions = dt.get_add_actions(flatten=True)
        column = actions.column('num_records')
        total = 0
        for value in column:
            if value.as_py() is None:  # stats-less writer: fall back
                raise ValueError
            total += value.as_py()
        return total
    except Exception:
        return dt.to_pyarrow_dataset().count_rows()
