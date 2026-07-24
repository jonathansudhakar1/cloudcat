---
name: cloudcat
description: Use when you need to quickly inspect, preview, sample, filter, count, or profile a data file, directory, or lakehouse table (CSV, JSON/JSONL, Parquet, Avro, ORC, text, Delta Lake, Apache Iceberg; gzip/zstd/lz4/snappy/bz2 compressed) on S3, GCS, Azure, or local disk — instead of writing pandas/pyarrow code or downloading whole files.
---

# cloudcat — fast data scanning

## Overview

`cloudcat PATH` previews data without downloading whole files. PATH is
`s3://…`, `gs://…`, `abfss://container@account.dfs.core.windows.net/…`,
`r2://…` (Cloudflare R2 — add `--endpoint-url https://<accountid>.r2.cloudflarestorage.com`),
`file://…`, or a plain local path (no credentials needed for local).
Directories (Spark/Hive output like `s3://bucket/output/`) work directly —
part files are discovered and merged, `_SUCCESS`/metadata files skipped.
Delta Lake and Iceberg table directories are auto-detected and read
snapshot-aware (only live files; needs `pip install 'cloudcat[tables]'`).

## Setup

```bash
command -v cloudcat || pip install 'cloudcat[all]'   # all formats + compression
cloudcat --version                                    # verify
```

This reference covers the scanning surface; run `cloudcat --help` only for
flags beyond it (credentials, delimiters, config profiles).

## Rules for automation

- **Always pass `-y`.** Without it, `--count` on a directory prompts for
  confirmation and fails in a non-TTY.
- **Parse `-o json` (NDJSON, one object per line).** `-o jsonp` and the
  default table are for humans.
- **stdout is data only.** Schema, progress, warnings, and stats headers go
  to stderr; color auto-disables when piped. Exit code 0 = success.
- **Schema first, then filter.** WHERE column names must match the schema
  exactly — run the schema recipe before writing filters (recipe examples use
  placeholder column names). On a typo the error lists available columns.
- **Filter with `--where`, not in post.** It streams and stops at `-n`
  matches; on Parquet it skips whole row groups via min/max statistics.
  Fetching everything to filter yourself is strictly worse.

## Recipes

| Goal | Command |
|------|---------|
| Schema only (no data read) | `cloudcat PATH -s schema_only -y` |
| First 5 rows, parseable | `cloudcat PATH -n 5 -o json -s dont_show -y` |
| Filtered sample | `cloudcat PATH -w "type=purchase AND amount>250" -n 5 -o json -s dont_show -y` |
| Exact row count (Parquet/ORC: metadata-only, instant) | `cloudcat PATH --count -s schema_only -y` |
| Column profile: nulls, distinct, min/max | `cloudcat PATH --stats -n 0 -s dont_show -y` |
| Only some columns | add `-c col1,col2` |
| Paginate filtered matches | add `--offset 50 -n 50` |
| Export clean file | add `-o csv -O out.csv` |
| Force format when extension lies | add `-i parquet` (csv, json, avro, orc, text, delta, iceberg) |
| Delta/Iceberg table: schema + exact count, metadata-only | `cloudcat TABLE_DIR/ -s schema_only --count -y` |

## Examples

Explore an unknown Parquet directory:

```bash
$ cloudcat s3://lake/events/ -s schema_only -y
Schema:
  event_id: int64
  event_type: str
  amount: float64

$ cloudcat s3://lake/events/ --count -s schema_only -y
Total records: 50,000            # stderr; footer metadata only, no download

$ cloudcat s3://lake/events/ -w "event_type=purchase AND amount>250" -n 2 -o json -s dont_show -y
{"event_id":24,"event_type":"purchase","amount":271.63}
{"event_id":56,"event_type":"purchase","amount":280.4}
# stderr: Filtered: 2 matching rows (scanned 5,000 rows); skipped 9 row group(s) via column statistics
```

Find nulls in a local CSV (no credentials):

```bash
$ cloudcat ./export.csv --stats -n 0 -o json -s dont_show -y
{"column":"amount","dtype":"float64","non_null":35191,"nulls":14809,"distinct":25882,"min":0.0,"max":300.0}
```

Grep-like scan of compressed logs:

```bash
$ cloudcat gs://logs/app.log.gz -i text -w "line contains ERROR" -n 20 -o json -s dont_show -y
{"line":"2026-06-01 ERROR timeout on shard 3","line_number":48213}
```

## WHERE syntax

Operators: `=` `!=` `<` `>` `<=` `>=` `contains` `not contains`
`startswith` `endswith`. Combine with `AND`/`OR` (AND binds tighter; no
parentheses). Quote values containing spaces or the words and/or:
`-w "title='Alice and Bob'"`. String matching is case-insensitive; `!=`
excludes nulls (SQL semantics).

## Notes

- `-n 0` means all rows — use with `--stats`/`--count`/exports, not previews.
- `--stats` profiles the retrieved rows: pair with `-n 0` for the whole
  file, or with `-w`/`-n` to profile a slice.
- Cloud auth is ambient: `--profile` (AWS), `--project`/`--credentials`
  (GCP), `--az-access-key` (Azure); R2/MinIO need `--endpoint-url` (or
  AWS_ENDPOINT_URL_S3) with S3-style keys. Missing-credential errors arrive
  on stderr with exit 1.

## Common mistakes

| Mistake | Fix |
|---------|-----|
| `-o jsonp` for machine parsing | `-o json` (NDJSON) |
| Omitting `-y` | directory `--count` prompts → dies non-interactively |
| Downloading/reading whole file, filtering in pandas | `--where` streams with pushdown |
| Looking for schema on stdout | it's on stderr; only data rows are stdout |
| `--help` before trying a recipe | recipes above cover scanning; `--help` is for the rest |
