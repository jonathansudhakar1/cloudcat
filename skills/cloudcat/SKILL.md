---
name: cloudcat
description: Use when you need to quickly inspect, preview, sample, filter, count, or profile a data file or directory (CSV, JSON/JSONL, Parquet, Avro, ORC, text; gzip/zstd/lz4/snappy/bz2 compressed) on S3, GCS, Azure, or local disk — instead of writing pandas/pyarrow code or downloading whole files.
---

# cloudcat — fast data scanning

## Overview

`cloudcat PATH` previews data without downloading whole files. PATH is
`s3://…`, `gs://…`, `abfss://container@account.dfs.core.windows.net/…`,
`file://…`, or a plain local path (no credentials needed for local).
Directories (Spark/Hive output like `s3://bucket/output/`) work directly —
part files are discovered and merged, `_SUCCESS`/metadata files skipped.

## Rules for automation

- **Always pass `-y`.** Without it, `--count` on a directory prompts for
  confirmation and fails in a non-TTY.
- **Parse `-o json` (NDJSON, one object per line).** `-o jsonp` and the
  default table are for humans.
- **stdout is data only.** Schema, progress, warnings, and stats headers go
  to stderr; color auto-disables when piped. Exit code 0 = success.
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
| Force format when extension lies | add `-i parquet` (csv, json, avro, orc, text) |

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
  (GCP), `--az-access-key` (Azure). Missing-credential errors arrive on
  stderr with exit 1.

## Common mistakes

| Mistake | Fix |
|---------|-----|
| `-o jsonp` for machine parsing | `-o json` (NDJSON) |
| Omitting `-y` | directory `--count` prompts → dies non-interactively |
| Downloading/reading whole file, filtering in pandas | `--where` streams with pushdown |
| Looking for schema on stdout | it's on stderr; only data rows are stdout |
| Running `--help` to discover flags | this reference covers the scanning surface |
