## Command Reference

Complete reference for all CloudCat command-line options.

### Usage

```
cloudcat [OPTIONS]
```

### Required Options

| Option | Description |
|--------|-------------|
| `PATH` (positional) | `gs://`, `gcs://`, `s3://`, `abfss://`, `file://` URL, or a plain local path. `-p/--path` remains as a compatible alias |

### Output & Format Options

| Option | Default | Description |
|--------|---------|-------------|
| `-o, --output-format TEXT` | `table` | Output format: `table`, `json`, `jsonp`, `csv` |
| `-O, --output-file PATH` | none | Write rendered data to a file instead of stdout |
| `-i, --input-format TEXT` | auto-detect | Input format: `csv`, `json`, `parquet`, `avro`, `orc`, `text`, `delta`, `iceberg` (tables auto-detected for directories) |

### Data Selection Options

| Option | Default | Description |
|--------|---------|-------------|
| `-c, --columns TEXT` | all | Comma-separated list of columns to display |
| `-n, --num-rows INTEGER` | 10 | Number of rows to display (0 for all rows) |
| `--offset INTEGER` | 0 | Skip first N rows |

### Filtering & Schema Options

| Option | Default | Description |
|--------|---------|-------------|
| `-w, --where TEXT` | none | Filter rows with SQL-like conditions; combine with `AND`/`OR`. Streams and stops at `--num-rows` matches; Parquet skips row groups via statistics |
| `-s, --schema TEXT` | `show` | Schema display: `show`, `dont_show`, `schema_only` |
| `--count` | false | Show total record count (scans entire file) |

### Directory Handling Options

| Option | Default | Description |
|--------|---------|-------------|
| `-m, --multi-file-mode TEXT` | `auto` | Directory handling: `auto`, `first`, `all` |
| `--max-size-mb INTEGER` | 25 | Max data size for multi-file mode in MB |

### CSV Options

| Option | Default | Description |
|--------|---------|-------------|
| `-d, --delimiter TEXT` | comma | CSV delimiter (use `\t` for tab) |

### Cloud Provider Authentication

| Option | Description |
|--------|-------------|
| `--profile TEXT` | AWS profile name (for S3 access) |
| `--project TEXT` | GCP project ID (for GCS access) |
| `--credentials TEXT` | Path to GCP service account JSON file |
| `--az-access-key TEXT` | Azure storage account access key |
| `--config-profile TEXT` | Named profile from `~/.config/cloudcat/config.toml` |

### General Options

| Option | Description |
|--------|-------------|
| `--stats` | Show per-column statistics (nulls, distinct, min/max) over the retrieved rows |
| `--completion [bash\|zsh\|fish]` | Print the shell completion script and exit. With it enabled, TAB also completes PATH: local files natively, `s3://<TAB>` lists buckets, `s3://bucket/dir/<TAB>` lists immediate children (honors `--profile`/`--project` typed earlier; 1–2s network timeout, silent on failure). bash users: add `COMP_WORDBREAKS=${COMP_WORDBREAKS//:/}` so URLs don't split on `:` |
| `--no-color` | Disable colored output (also honors the `NO_COLOR` env var). Color is auto-disabled when output is piped. |
| `-y, --yes` | Skip confirmation prompts (for scripting) |
| `--help` | Show help message and exit |

### Examples

```bash
# Basic usage
cloudcat -p gcs://bucket/data.csv

# Select columns and limit rows
cloudcat -p s3://bucket/users.parquet -c id,name,email -n 20

# Filter with WHERE clause
cloudcat -p gcs://bucket/events.json --where "status=active"

# Output as JSON
cloudcat -p s3://bucket/data.csv -o json

# Read from Spark output directory
cloudcat -p s3://bucket/spark-output/ -i parquet -m all

# Use custom delimiter for TSV
cloudcat -p gcs://bucket/data.tsv -d "\t"

# Pagination
cloudcat -p s3://bucket/large.csv --offset 100 -n 10

# Schema only
cloudcat -p gcs://bucket/events.parquet -s schema_only

# With AWS profile
cloudcat -p s3://bucket/data.csv --profile production

# With GCP credentials
cloudcat -p gcs://bucket/data.csv --credentials /path/to/key.json

# Azure Data Lake Storage Gen2 with access key
cloudcat -p abfss://container@account.dfs.core.windows.net/data.parquet --az-access-key "YOUR_KEY"

# Azure with environment variable (no CLI option needed)
export AZURE_STORAGE_ACCESS_KEY="YOUR_KEY"
cloudcat -p abfss://container@account.dfs.core.windows.net/data.parquet
```
