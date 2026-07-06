"""Lakehouse table-format support (Delta Lake, Apache Iceberg).

A *table* is a directory whose metadata layer decides which data files are
live. These adapters resolve the current snapshot — schema, live files,
row counts — and hand cloudcat a filtered/limited frame, so everything
downstream (rendering, --stats, NDJSON, WHERE semantics) is shared with the
plain-file paths.

Layout:
    detection + routing here; format specifics in delta.py / iceberg.py;
    shared filter-pushdown helpers in pushdown.py.
"""

import re
from typing import Optional, Tuple

import pandas as pd

from ..storage import list_directory
from ..streaming import StreamingStats

TABLE_FORMATS = ('delta', 'iceberg')

_ICEBERG_METADATA_RE = re.compile(r'(?:^|/)(?:v(\d+)|(\d+)-[^/]*)\.metadata\.json$')


def detect_table_format(service: str, bucket: str, prefix: str) -> Optional[str]:
    """Detect whether a directory is a Delta or Iceberg table.

    Cheap targeted listings (the marker directories are small) rather than a
    full table listing. Returns 'delta', 'iceberg', or None; never raises —
    on listing errors the caller proceeds with the plain-directory flow,
    where real errors surface with context.
    """
    base = prefix if (prefix.endswith('/') or prefix == '') else prefix + '/'
    try:
        if list_directory(service, bucket, base + '_delta_log/'):
            return 'delta'
    except Exception:
        pass
    try:
        entries = list_directory(service, bucket, base + 'metadata/')
        if any(_ICEBERG_METADATA_RE.search(name) for name, _size in entries):
            return 'iceberg'
    except Exception:
        pass
    return None


def table_uri(service: str, bucket: str, prefix: str) -> str:
    """Build the table URI the lakehouse libraries expect."""
    path = prefix.rstrip('/')
    if service == 'local':
        return path
    if service == 's3':
        return f"s3://{bucket}/{path}"
    if service == 'gcs':
        return f"gs://{bucket}/{path}"
    if service == 'azure':
        from ..config import cloud_config
        account = cloud_config.azure_account
        if not account:
            raise ValueError(
                "Azure table reads need the storage account; use an abfss:// URL."
            )
        return f"abfss://{bucket}@{account}.dfs.core.windows.net/{path}"
    raise ValueError(f"Unsupported service for table formats: {service}")


def latest_iceberg_metadata(service: str, bucket: str, prefix: str) -> str:
    """Find the newest *.metadata.json under <table>/metadata/.

    Handles both catalog naming (00003-<uuid>.metadata.json) and Hadoop
    naming (v3.metadata.json) by picking the highest version number.
    """
    base = prefix if (prefix.endswith('/') or prefix == '') else prefix + '/'
    entries = list_directory(service, bucket, base + 'metadata/')
    candidates = []
    for name, _size in entries:
        m = _ICEBERG_METADATA_RE.search(name)
        if m:
            version = int(m.group(1) or m.group(2))
            candidates.append((version, name))
    if not candidates:
        raise ValueError(f"No Iceberg metadata files found under {base}metadata/")
    _version, name = max(candidates)
    return name


def read_table_data(
    service: str,
    bucket: str,
    prefix: str,
    table_format: str,
    num_rows: int,
    columns: Optional[str] = None,
    offset: int = 0,
    where: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.Series, StreamingStats]:
    """Read a preview from a lakehouse table's current snapshot.

    Mirrors read_data_streaming's contract: returns (df, full_schema, stats)
    with WHERE already applied (stats.where_applied) and offset paging
    through matches.
    """
    if table_format == 'delta':
        from . import delta
        return delta.read(service, bucket, prefix, num_rows, columns, offset, where)
    if table_format == 'iceberg':
        from . import iceberg
        return iceberg.read(service, bucket, prefix, num_rows, columns, offset, where)
    raise ValueError(f"Unsupported table format: {table_format}")


def table_row_count(service: str, bucket: str, prefix: str, table_format: str) -> int:
    """Exact row count of the current snapshot (metadata-only where possible)."""
    if table_format == 'delta':
        from . import delta
        return delta.row_count(service, bucket, prefix)
    if table_format == 'iceberg':
        from . import iceberg
        return iceberg.row_count(service, bucket, prefix)
    raise ValueError(f"Unsupported table format: {table_format}")
