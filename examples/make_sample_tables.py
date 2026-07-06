#!/usr/bin/env python
"""Generate sample lakehouse tables for trying cloudcat locally.

Creates ./examples/lakehouse/ with a Delta Lake table and an Apache Iceberg
table (multiple snapshots each, nulls, several columns), then prints example
cloudcat commands to run against them.

The tables are generated rather than committed: Iceberg metadata embeds
absolute paths, so a checked-in table would only work on the machine that
wrote it.

Requires: pip install 'cloudcat[tables]'   (or deltalake + pyiceberg)
"""

import random
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa

HERE = Path(__file__).resolve().parent
OUT = HERE / "lakehouse"

random.seed(42)
N = 5_000

REGIONS = ["us-east", "eu-west", "ap-south"]
STATUSES = ["complete", "pending", "refunded"]


def sample_frame(start: int, count: int) -> pd.DataFrame:
    return pd.DataFrame({
        "order_id": range(start, start + count),
        "region": [random.choice(REGIONS) for _ in range(count)],
        "status": [random.choice(STATUSES) for _ in range(count)],
        "amount": [round(random.uniform(5, 500), 2) if random.random() > 0.1 else None
                   for _ in range(count)],
        "ts": pd.date_range("2026-01-01", periods=count, freq="min").astype(str),
    })


def make_delta(root: Path) -> Path:
    from deltalake import write_deltalake, DeltaTable
    path = root / "orders_delta"
    write_deltalake(str(path), sample_frame(0, N), partition_by=["region"])
    # Second commit: append a late batch so the log has history.
    write_deltalake(str(path), sample_frame(N, 500), mode="append")
    print(f"  delta:   {path}  (version {DeltaTable(str(path)).version()}, {N + 500} rows)")
    return path


def make_iceberg(root: Path) -> Path:
    from pyiceberg.catalog.sql import SqlCatalog
    catalog = SqlCatalog(
        "sample", uri=f"sqlite:///{root}/catalog.db", warehouse=f"file://{root}"
    )
    catalog.create_namespace("shop")
    schema = pa.schema([
        ("order_id", pa.int64()), ("region", pa.string()),
        ("status", pa.string()), ("amount", pa.float64()), ("ts", pa.string()),
    ])
    table = catalog.create_table("shop.orders", schema=schema)
    table.append(pa.Table.from_pandas(sample_frame(0, N), schema=schema))
    table.append(pa.Table.from_pandas(sample_frame(N, 500), schema=schema))
    location = Path(table.location()[len("file://"):])
    snapshot = table.current_snapshot().snapshot_id
    print(f"  iceberg: {location}  (snapshot {snapshot}, {N + 500} rows)")
    return location


def main():
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)
    print(f"Generating sample tables under {OUT} ...")
    delta_path = make_delta(OUT)
    iceberg_path = make_iceberg(OUT)

    print("\nTry these:")
    for cmd in [
        f"cloudcat {delta_path}/ -s schema_only -y",
        f"cloudcat {delta_path}/ --count -s schema_only -y",
        f"cloudcat {delta_path}/ -w \"status=refunded AND amount>400\" -n 5 -y",
        f"cloudcat {iceberg_path}/ --stats -n 0 -s dont_show -y",
        f"cloudcat {iceberg_path}/ -w \"region=eu-west\" -n 5 -o json -s dont_show -y",
    ]:
        print(f"  {cmd}")


if __name__ == "__main__":
    main()
