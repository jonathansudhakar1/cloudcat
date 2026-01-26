<p align="center">
  <img src="https://raw.githubusercontent.com/jonathansudhakar1/cloudcat/main/assets/logo.png" alt="CloudCat Logo" width="200">
</p>

<h1 align="center">CloudCat</h1>

<p align="center">
  <strong>The Swiss Army knife for viewing cloud storage data from your terminal</strong>
</p>

<p align="center">
  <a href="https://pypi.org/project/cloudcat/"><img src="https://img.shields.io/pypi/v/cloudcat.svg?style=flat-square&logo=pypi&logoColor=white" alt="PyPI version"></a>
  <a href="https://pypi.org/project/cloudcat/"><img src="https://img.shields.io/pypi/pyversions/cloudcat.svg?style=flat-square&logo=python&logoColor=white" alt="Python versions"></a>
  <a href="https://pepy.tech/projects/cloudcat"><img src="https://static.pepy.tech/personalized-badge/cloudcat?period=total&units=international_system&left_color=black&right_color=green&left_text=downloads" alt="PyPI Downloads"></a>
  <a href="https://github.com/jonathansudhakar1/cloudcat/releases"><img src="https://img.shields.io/github/downloads/jonathansudhakar1/cloudcat/total.svg?style=flat-square&logo=homebrew&logoColor=white&label=homebrew" alt="Homebrew Downloads"></a>
  <a href="https://github.com/jonathansudhakar1/cloudcat/blob/main/LICENSE"><img src="https://img.shields.io/github/license/jonathansudhakar1/cloudcat.svg?style=flat-square" alt="License"></a>
</p>

<p align="center">
  <a href="https://cloudcatcli.com">Documentation</a> •
  <a href="#installation">Installation</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#features">Features</a> •
  <a href="#examples">Examples</a>
</p>

---

**CloudCat** is a powerful command-line tool that lets you instantly preview and analyze data files stored in **Google Cloud Storage (GCS)**, **Amazon S3**, and **Azure Blob Storage** — without downloading entire files. Think of it as `cat`, `head`, and `less` combined, but for cloud storage with built-in support for CSV, JSON, Parquet, Avro, ORC, and plain text formats.

## Why CloudCat?

- **No Downloads Required** — Stream and preview data directly from cloud storage
- **Format-Aware** — Intelligently handles CSV, JSON, Parquet, Avro, ORC, and plain text files
- **Directory Smart** — Automatically discovers data files in Spark/Hive/Kafka output directories
- **Beautiful Output** — Colorized tables, pretty-printed JSON, and schema visualization
- **Developer Friendly** — Simple CLI with sensible defaults and powerful options
- **Compression Support** — Automatic decompression of gzip, zstd, lz4, snappy, and bz2 files
- **SQL-like Filtering** — Filter rows with WHERE clauses (e.g., `--where "status=active"`)

## Installation

### Homebrew (macOS Apple Silicon)

The easiest way to install on Apple Silicon Macs (M1/M2/M3/M4) — no Python required:

```bash
brew tap jonathansudhakar1/cloudcat https://github.com/jonathansudhakar1/cloudcat.git && brew install cloudcat
```

This installs a self-contained binary that includes Python and all dependencies.

> **Intel Mac users:** Homebrew bottles are not available for Intel. Please use `pip install 'cloudcat[all]'` instead.

To upgrade:

```bash
brew update && brew upgrade cloudcat
```

> **Note:** On first run, macOS may block the app. Go to System Settings > Privacy & Security and click "Allow", or run:
> ```bash
> xattr -d com.apple.quarantine $(which cloudcat)
> ```

### pip (Python)

```bash
# Full installation with all formats and compression
pip install 'cloudcat[all]'

# Standard installation (includes GCS, S3, and Azure support)
pip install cloudcat

# With Parquet file support
pip install 'cloudcat[parquet]'

# With Avro file support
pip install 'cloudcat[avro]'

# With ORC file support (uses pyarrow)
pip install 'cloudcat[orc]'

# With compression support (zstd, lz4, snappy)
pip install 'cloudcat[compression]'
```

> **Note:** If using zsh (default on macOS), quotes around extras are required to prevent shell interpretation of brackets.

To upgrade:

```bash
pip install --upgrade 'cloudcat[all]'
```

### Requirements

- **Homebrew**: macOS (Apple Silicon only). Intel Mac users should use pip.
- **pip**: Python 3.7+ (all platforms)
- Cloud provider credentials configured (see [Authentication](#authentication))

## Quick Start

```bash
# Preview a CSV file from GCS
cloudcat -p gcs://my-bucket/data.csv

# Preview a Parquet file from S3
cloudcat -p s3://my-bucket/analytics/events.parquet

# Preview JSON data from Azure with pretty formatting
cloudcat -p abfss://my-container@account.dfs.core.windows.net/logs.json -o jsonp

# Read Avro files from Kafka
cloudcat -p s3://my-bucket/kafka-export.avro

# Read ORC files from Hive
cloudcat -p gcs://my-bucket/hive-table.orc

# Read log files as plain text
cloudcat -p abfss://logs@account.dfs.core.windows.net/app.log -i text

# Read from a Spark output directory
cloudcat -p s3://my-bucket/spark-output/ -i parquet

# Read compressed files (auto-detected)
cloudcat -p gcs://my-bucket/data.csv.gz

# Filter rows with WHERE clause
cloudcat -p s3://bucket/users.parquet --where "status=active"

# Skip first 100 rows (pagination)
cloudcat -p gcs://bucket/data.csv --offset 100 -n 10
```

## Features

### Cloud Storage Support

| Provider | URL Scheme | Status |
|----------|------------|--------|
| Google Cloud Storage | `gcs://` or `gs://` | ✅ Supported |
| Amazon S3 | `s3://` | ✅ Supported |
| Azure Data Lake Gen2 | `abfss://` | ✅ Supported |

### File Format Support

| Format | Auto-Detect | Use Case |
|--------|-------------|----------|
| CSV | ✅ | General data files |
| JSON | ✅ | API responses, configs |
| JSON Lines | ✅ | Log files, streaming data |
| Parquet | ✅ | Spark/analytics data |
| Avro | ✅ | Kafka, data pipelines |
| ORC | ✅ | Hive, Hadoop ecosystem |
| Text | ✅ | Log files, plain text |
| TSV | Via `--delimiter` | Tab-separated data |

### Streaming Efficiency

CloudCat uses intelligent streaming to minimize data transfer and egress costs:

| Format | Compression | Streams | Column Projection | Early Row Stop |
|--------|-------------|---------|-------------------|----------------|
| Parquet | None/Internal | ✅ | ✅ Range requests | ✅ |
| Parquet | External (.gz) | ❌ | ❌ | ❌ |
| ORC | None/Internal | ❌ | ❌ | ❌ |
| ORC | External (.gz) | ❌ | ❌ | ❌ |
| CSV | None | ✅ | ❌ | ✅ |
| CSV | gzip/zstd/lz4/bz2 | ✅ | ❌ | ✅ |
| CSV | snappy | ❌ | ❌ | ❌ |
| JSON Lines | None/streamable | ✅ | ❌ | ✅ |
| JSON Array | Any | ❌ | ❌ | ❌ |
| Avro | Any | ✅ | ✅ Record-level | ✅ |
| Text | Any streamable | ✅ | N/A | ✅ |

- **Streams**: Only reads data as needed, stops early when row limit is reached
- **Column Projection**: For Parquet, only fetches required column chunks via HTTP range requests
- **Early Row Stop**: Stops reading when `--num-rows` limit is reached

### Compression Support

| Format | Extension | Built-in | Use Case |
|--------|-----------|----------|----------|
| Gzip | `.gz`, `.gzip` | ✅ | Most common, universal |
| Bzip2 | `.bz2` | ✅ | High compression ratio |
| Zstandard | `.zst`, `.zstd` | Optional | Fast, modern compression |
| LZ4 | `.lz4` | Optional | Very fast decompression |
| Snappy | `.snappy` | Optional | Hadoop ecosystem |

CloudCat automatically detects and decompresses files based on extension (e.g., `data.csv.gz`, `logs.json.zst`).

### Output Formats

| Format | Flag | Description |
|--------|------|-------------|
| Table | `-o table` | Beautiful ASCII table with colored headers (default) |
| JSON | `-o json` | Standard JSON Lines output |
| Pretty JSON | `-o jsonp` | Syntax-highlighted, indented JSON |
| CSV | `-o csv` | Comma-separated values |

### Key Capabilities

- **Schema Inspection** — View column names and data types
- **Column Selection** — Display only the columns you need
- **Row Limiting** — Control how many rows to preview
- **Row Offset** — Skip first N rows for pagination/sampling
- **WHERE Filtering** — Filter rows with SQL-like conditions
- **Record Counting** — Get total record counts (with Parquet metadata optimization)
- **Multi-File Reading** — Combine data from multiple files in a directory
- **Custom Delimiters** — Support for tab, pipe, semicolon, and other delimiters
- **Auto Decompression** — Transparent handling of compressed files

## Examples

### Basic Usage

```bash
# Preview first 10 rows (default)
cloudcat -p gcs://bucket/data.csv

# Preview 50 rows
cloudcat -p s3://bucket/data.parquet -n 50

# Show only specific columns
cloudcat -p gcs://bucket/users.json -c id,name,email

# View schema only (no data)
cloudcat -p s3://bucket/events.parquet -s schema_only
```

### Working with Different Formats

```bash
# CSV with custom delimiter (tab-separated)
cloudcat -p gcs://bucket/data.tsv -d "\t"

# Pipe-delimited file
cloudcat -p s3://bucket/export.txt -d "|"

# Semicolon-delimited (common in European data)
cloudcat -p gcs://bucket/report.csv -d ";"

# JSON array file
cloudcat -p s3://bucket/config.json

# JSON Lines file (auto-detected)
cloudcat -p gcs://bucket/events.jsonl
```

### Filtering and Pagination

```bash
# Filter rows with WHERE clause
cloudcat -p s3://bucket/users.parquet --where "status=active"
cloudcat -p gcs://bucket/events.json --where "age>30"
cloudcat -p s3://bucket/logs.csv --where "level=ERROR"

# String matching filters
cloudcat -p gcs://bucket/data.csv --where "name contains john"
cloudcat -p s3://bucket/emails.json --where "email endswith @gmail.com"
cloudcat -p abfss://logs@account.dfs.core.windows.net/app.log --where "message startswith ERROR"

# Skip first N rows (pagination)
cloudcat -p gcs://bucket/data.csv --offset 100 -n 10

# Combine offset with filters
cloudcat -p s3://bucket/users.parquet --where "active=true" --offset 50 -n 20
```

### Compressed Files

```bash
# Gzip compressed (built-in)
cloudcat -p gcs://bucket/data.csv.gz
cloudcat -p s3://bucket/logs.json.gz

# Zstandard compressed (requires: pip install cloudcat[zstd])
cloudcat -p gcs://bucket/events.parquet.zst

# LZ4 compressed (requires: pip install cloudcat[lz4])
cloudcat -p s3://bucket/data.csv.lz4

# Bzip2 compressed (built-in)
cloudcat -p abfss://container@account.dfs.core.windows.net/archive.json.bz2
```

### Directory Operations

CloudCat intelligently handles directories containing multiple data files (common with Spark, Hive, and distributed processing outputs):

```bash
# Auto-detect and read first data file in directory
cloudcat -p gcs://bucket/spark-output/

# Read and combine multiple files (up to 25MB by default)
cloudcat -p s3://bucket/daily-logs/ -m all

# Read up to 100MB of data from multiple files
cloudcat -p gcs://bucket/events/ -m all --max-size-mb 100

# Force reading only the first file
cloudcat -p s3://bucket/output/ -m first
```

**CloudCat automatically:**
- Skips empty files
- Ignores metadata files (`_SUCCESS`, `_metadata`, `.crc`, etc.)
- Prioritizes files matching the specified format
- Reports which files were selected

### Output Format Examples

```bash
# Default table output (great for terminals)
cloudcat -p gcs://bucket/data.csv
# ┌────┬────────────┬─────────┐
# │ id │ name       │ value   │
# ├────┼────────────┼─────────┤
# │ 1  │ Alice      │ 100     │
# │ 2  │ Bob        │ 200     │
# └────┴────────────┴─────────┘

# Pretty JSON (great for nested data)
cloudcat -p s3://bucket/events.json -o jsonp
# {
#   "id": 1,
#   "name": "Alice",
#   "metadata": {
#     "created": "2024-01-15"
#   }
# }

# JSON Lines (great for piping to jq)
cloudcat -p gcs://bucket/data.parquet -o json | jq '.name'

# CSV (great for further processing)
cloudcat -p s3://bucket/data.json -o csv > output.csv
```

### Data Pipeline Examples

```bash
# Convert Parquet to CSV
cloudcat -p gcs://bucket/data.parquet -o csv -n 0 > data.csv

# Preview and filter with jq
cloudcat -p s3://bucket/events.json -o json | jq 'select(.status == "error")'

# Quick data validation
cloudcat -p gcs://bucket/import.csv -s schema_only

# Sample data from large dataset
cloudcat -p s3://bucket/big-table.parquet -n 100 -c user_id,event_type

# Export specific columns to CSV
cloudcat -p gcs://bucket/users.parquet -c email,created_at -o csv -n 0 > emails.csv
```

### Real-World Use Cases

#### Debugging Spark Jobs
```bash
# Check output of a Spark job
cloudcat -p gcs://data-lake/jobs/daily-etl/output/ -i parquet -n 20

# Verify schema matches expectations
cloudcat -p s3://analytics/spark-output/ -s schema_only
```

#### Log Analysis
```bash
# Preview recent logs
cloudcat -p gcs://logs/app/2024-01-15/ -m all -n 50

# Check error logs (combine with grep)
cloudcat -p s3://logs/errors/ -o json | grep "ERROR"
```

#### Data Validation
```bash
# Quick sanity check on data export
cloudcat -p gcs://exports/daily/users.csv -s show

# Verify record count
cloudcat -p s3://warehouse/transactions.parquet --count
```

#### Format Conversion
```bash
# Convert tab-separated to comma-separated
cloudcat -p gcs://imports/data.tsv -d "\t" -o csv > converted.csv

# Convert JSON to CSV for spreadsheet import
cloudcat -p s3://api-dumps/response.json -o csv > data.csv
```

## Command Reference

```
Usage: cloudcat [OPTIONS]

Options:
  -p, --path TEXT              Cloud storage path (required)
                               Format: gcs://bucket/path, s3://bucket/path,
                               or abfss://container@account.dfs.core.windows.net/path

  -o, --output-format TEXT     Output format: table, json, jsonp, csv
                               [default: table]

  -i, --input-format TEXT      Input format: csv, json, parquet, avro, orc, text
                               [default: auto-detect from extension]

  -c, --columns TEXT           Comma-separated list of columns to display
                               [default: all columns]

  -n, --num-rows INTEGER       Number of rows to display (0 for all)
                               [default: 10]

  --offset INTEGER             Skip first N rows
                               [default: 0]

  -w, --where TEXT             Filter rows with SQL-like conditions
                               Examples: "status=active", "age>30",
                               "name contains john", "email endswith @gmail.com"

  -s, --schema TEXT            Schema display: show, dont_show, schema_only
                               [default: show]

  --count                      Show total record count (scans entire file)

  -m, --multi-file-mode TEXT   Directory handling: auto, first, all
                               [default: auto]

  --max-size-mb INTEGER        Max data size for multi-file mode in MB
                               [default: 25]

  -d, --delimiter TEXT         CSV delimiter (use \t for tab)
                               [default: comma]

  --profile TEXT               AWS profile name (for S3 access)

  --project TEXT               GCP project ID (for GCS access)

  --credentials TEXT           Path to GCP service account JSON file

  --account TEXT               Azure storage account name

  --help                       Show this message and exit
```

### WHERE Clause Operators

| Operator | Example | Description |
|----------|---------|-------------|
| `=` | `status=active` | Exact match |
| `!=` | `type!=deleted` | Not equal |
| `>` | `age>30` | Greater than |
| `<` | `price<100` | Less than |
| `>=` | `count>=10` | Greater than or equal |
| `<=` | `score<=50` | Less than or equal |
| `contains` | `name contains john` | Case-insensitive substring match |
| `startswith` | `email startswith admin` | String prefix match |
| `endswith` | `file endswith .csv` | String suffix match |

## Authentication

### Google Cloud Storage

CloudCat uses [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials). Set up authentication using one of these methods:

```bash
# Option 1: User credentials (for development)
gcloud auth application-default login

# Option 2: Service account via environment variable
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Option 3: Service account via CLI option
cloudcat -p gcs://bucket/data.csv --credentials /path/to/service-account.json

# Option 4: Specify GCP project
cloudcat -p gcs://bucket/data.csv --project my-gcp-project
```

### Amazon S3

CloudCat uses the standard [AWS credential chain](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html):

```bash
# Option 1: Environment variables
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"

# Option 2: AWS credentials file (~/.aws/credentials)
aws configure

# Option 3: AWS named profile
cloudcat -p s3://bucket/data.csv --profile production

# Option 4: IAM role (for EC2/ECS/Lambda)
# Automatically detected
```

### Azure Data Lake Storage Gen2

CloudCat supports multiple authentication methods for Azure ADLS Gen2:

```bash
# Option 1: Access key (simplest)
cloudcat -p abfss://container@account.dfs.core.windows.net/data.csv --az-access-key "YOUR_KEY"

# Option 2: Access key via environment variable
export AZURE_STORAGE_ACCESS_KEY="YOUR_KEY"
cloudcat -p abfss://container@account.dfs.core.windows.net/data.csv

# Option 3: Azure CLI with DefaultAzureCredential
az login
cloudcat -p abfss://container@account.dfs.core.windows.net/data.csv
```

**Path format:** `abfss://container@account.dfs.core.windows.net/path/to/file`

## Performance Tips

1. **Counting is off by default** — use `--count` only when you need the total record count
2. **Prefer Parquet** format when possible — record counts are instant from metadata
3. **Use `--num-rows`** to limit data transfer for large files
4. **Use `--columns`** to select only needed columns (especially effective with Parquet)
5. **Use `-m first`** when you only need a sample from directories with many files

## Troubleshooting

### Common Issues

**"google-cloud-storage package is required"**
```bash
pip install cloudcat[gcs]
```

**"boto3 package is required"**
```bash
pip install cloudcat[s3]
```

**"pyarrow package is required"**
```bash
pip install cloudcat[parquet]
```

**"azure-storage-blob package is required"**
```bash
pip install cloudcat[azure]
```

**"fastavro package is required"**
```bash
pip install cloudcat[avro]
```

**"pyarrow with ORC support is required"**
```bash
pip install cloudcat[orc]
```

**"zstandard package is required for .zst files"**
```bash
pip install cloudcat[zstd]
# or for all compression formats:
pip install cloudcat[compression]
```

**"lz4 package is required for .lz4 files"**
```bash
pip install cloudcat[lz4]
```

**"python-snappy package is required for .snappy files"**
```bash
pip install cloudcat[snappy]
```

**Authentication errors**
- GCS: Run `gcloud auth application-default login`
- S3: Run `aws configure` or check your credentials
- Azure: Set `AZURE_STORAGE_CONNECTION_STRING` or `AZURE_STORAGE_ACCOUNT_URL` and run `az login`

**"Could not infer format from path"**
```bash
# Specify the format explicitly
cloudcat -p gcs://bucket/data -i parquet
```

## Contributing

Contributions are welcome! Here's how you can help:

1. **Report bugs** — Open an issue with reproduction steps
2. **Suggest features** — Open an issue describing the use case
3. **Submit PRs** — Fork, create a branch, and submit a pull request

### Development Setup

```bash
# Clone the repository
git clone https://github.com/jonathansudhakar1/cloudcat.git
cd cloudcat

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install in development mode with all dependencies
pip install -e ".[all]"

# Run tests
pytest
```

## Roadmap

- [x] Azure Blob Storage support
- [x] Avro format support
- [x] ORC format support
- [x] Plain text format support
- [x] SQL-like filtering (`--where` clause)
- [x] Compression support (gzip, zstd, lz4, snappy, bz2)
- [x] Row offset/pagination (`--offset`)
- [ ] Interactive mode with pagination
- [ ] Output to file with `--output-file`
- [ ] Configuration file support

## Related Projects

- [s3cmd](https://s3tools.org/s3cmd) — S3 command-line tool
- [gsutil](https://cloud.google.com/storage/docs/gsutil) — Google Cloud Storage CLI
- [aws-cli](https://aws.amazon.com/cli/) — AWS command-line interface
- [azcopy](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10) — Azure Storage data transfer tool
- [duckdb](https://duckdb.org/) — In-process SQL OLAP database

## License

MIT License — see [LICENSE](LICENSE) for details.


<p align="center">
  <a href="https://github.com/jonathansudhakar1/cloudcat/issues">Report Bug</a> •
  <a href="https://github.com/jonathansudhakar1/cloudcat/issues">Request Feature</a>
</p>
