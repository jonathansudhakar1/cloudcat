## Lakehouse Tables

CloudCat reads **Delta Lake** and **Apache Iceberg** tables directly — point
it at the table directory and it resolves the *current snapshot* from the
table's metadata layer: only live data files are read, dead files from
overwrites and compactions are ignored.

```bash
# Auto-detected from the table's marker directory
cloudcat s3://lake/orders_delta/ -n 10
cloudcat gs://lake/warehouse/shop/orders/ -n 10        # Iceberg

# Or explicit
cloudcat s3://lake/orders/ -i delta
cloudcat s3://lake/orders/ -i iceberg
```

### Installation

The table formats are optional extras (kept out of `all` — `deltalake`
alone is a ~100 MB wheel):

```bash
pip install 'cloudcat[delta]'      # Delta Lake
pip install 'cloudcat[iceberg]'    # Apache Iceberg (~4 MB)
pip install 'cloudcat[tables]'     # both
```

### What works

Everything the plain-file paths support, snapshot-aware:

| Capability | Delta | Iceberg |
|------------|-------|---------|
| Auto-detection (`_delta_log/`, `metadata/*.metadata.json`) | ✅ | ✅ |
| Schema from table metadata (`-s schema_only`) | ✅ | ✅ |
| `--count` from snapshot metadata — no data read | ✅ | ✅ |
| `--where` with engine pushdown (files/row groups pruned) | ✅ | ✅ |
| Compound `AND`/`OR` filters | ✅ | ✅ |
| Column projection, `--stats`, `--offset`, all output formats | ✅ | ✅ |
| Catalog required | No | No — newest `metadata.json` resolved by listing |

Supported WHERE operators (`=`, `!=`, `<`, `>`, `<=`, `>=`, `AND`/`OR`)
push down into the engines; `contains`-style operators fall back to reading
the (column-projected) snapshot and filtering locally — same semantics
either way, because CloudCat's own filter always makes the final decision.

### Examples

```bash
# Structure and exact size of a table, metadata-only
cloudcat s3://lake/orders_delta/ -s schema_only --count -y

# Filtered sample: engine prunes non-matching files before download
cloudcat s3://lake/orders_delta/ -w "status=refunded AND amount>400" -n 10

# Column profile of the live snapshot
cloudcat gs://lake/warehouse/shop/orders/ --stats -n 0 -s dont_show -y

# Export matching rows to a local CSV
cloudcat s3://lake/orders_delta/ -w "region=eu-west" -n 0 -o csv -O eu.csv
```

### Try it locally

The repo ships a sample-data generator — no cloud account needed:

```bash
pip install 'cloudcat[tables]'
python examples/make_sample_tables.py
cloudcat examples/lakehouse/orders_delta/ --count -s schema_only -y
```

### Notes

- Reads target the **latest snapshot**; time travel is on the roadmap
- Deletion vectors, column mapping, and Iceberg v2 delete files are handled
  by the underlying engines (`deltalake`, `pyiceberg`), not re-implemented
- Cloud credentials follow the same ambient auth as everything else
  (`--profile`, `--project`/`--credentials`, `--az-access-key`)
- Apache Hudi is not supported yet (no mature Python reader)
