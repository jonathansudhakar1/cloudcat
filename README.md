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
  <a href="https://pypi.org/project/cloudcat/"><img src="https://img.shields.io/pypi/dm/cloudcat.svg?style=flat-square&logo=pypi&logoColor=white" alt="Downloads"></a>
  <a href="https://github.com/jonathansudhakar1/cloudcat/blob/main/LICENSE"><img src="https://img.shields.io/github/license/jonathansudhakar1/cloudcat.svg?style=flat-square" alt="License"></a>
  <a href="https://github.com/jonathansudhakar1/cloudcat/stargazers"><img src="https://img.shields.io/github/stars/jonathansudhakar1/cloudcat.svg?style=flat-square&logo=github" alt="GitHub stars"></a>
</p>

<p align="center">
  <a href="#installation">Installation</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#features">Features</a> •
  <a href="#examples">Examples</a> •
  <a href="#documentation">Documentation</a>
</p>

---

**CloudCat** is a powerful command-line tool that lets you instantly preview and analyze data files stored in **Google Cloud Storage (GCS)** and **Amazon S3** — without downloading entire files. Think of it as `cat`, `head`, and `less` combined, but for cloud storage with built-in support for CSV, JSON, and Parquet formats.

## Why CloudCat?

- **No Downloads Required** — Stream and preview data directly from cloud storage
- **Format-Aware** — Intelligently handles CSV, JSON (including JSON Lines), and Parquet files
- **Directory Smart** — Automatically discovers data files in Spark/Hive output directories
- **Beautiful Output** — Colorized tables, pretty-printed JSON, and schema visualization
- **Developer Friendly** — Simple CLI with sensible defaults and powerful options

## Installation

```bash
# Basic installation
pip install cloudcat

# With Google Cloud Storage support
pip install cloudcat[gcs]

# With AWS S3 support
pip install cloudcat[s3]

# With Parquet file support
pip install cloudcat[parquet]

# Full installation (recommended)
pip install cloudcat[all]
```

### Requirements

- Python 3.7+
- Cloud provider credentials configured (see [Authentication](#authentication))

## Quick Start

```bash
# Preview a CSV file from GCS
cloudcat -p gcs://my-bucket/data.csv

# Preview a Parquet file from S3
cloudcat -p s3://my-bucket/analytics/events.parquet

# Preview JSON data with pretty formatting
cloudcat -p gcs://my-bucket/logs.json -o jsonp

# Read from a Spark output directory
cloudcat -p s3://my-bucket/spark-output/ -i parquet
```

## Features

### Cloud Storage Support

| Provider | URL Scheme | Status |
|----------|------------|--------|
| Google Cloud Storage | `gcs://` or `gs://` | ✅ Supported |
| Amazon S3 | `s3://` | ✅ Supported |
| Azure Blob Storage | `az://` | 🔜 Coming Soon |

### File Format Support

| Format | Read | Auto-Detect | Streaming |
|--------|------|-------------|-----------|
| CSV | ✅ | ✅ | ✅ |
| JSON | ✅ | ✅ | ✅ |
| JSON Lines | ✅ | ✅ | ✅ |
| Parquet | ✅ | ✅ | ✅ |
| TSV | ✅ | Via `--delimiter` | ✅ |

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
- **Record Counting** — Get total record counts (with Parquet metadata optimization)
- **Multi-File Reading** — Combine data from multiple files in a directory
- **Custom Delimiters** — Support for tab, pipe, semicolon, and other delimiters

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
cloudcat -p s3://warehouse/transactions.parquet --no-count
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
                               Format: gcs://bucket/path or s3://bucket/path

  -o, --output-format TEXT     Output format: table, json, jsonp, csv
                               [default: table]

  -i, --input-format TEXT      Input format: csv, json, parquet
                               [default: auto-detect from extension]

  -c, --columns TEXT           Comma-separated list of columns to display
                               [default: all columns]

  -n, --num-rows INTEGER       Number of rows to display (0 for all)
                               [default: 10]

  -s, --schema TEXT            Schema display: show, dont_show, schema_only
                               [default: show]

  --no-count                   Disable automatic record counting

  -m, --multi-file-mode TEXT   Directory handling: auto, first, all
                               [default: auto]

  --max-size-mb INTEGER        Max data size for multi-file mode in MB
                               [default: 25]

  -d, --delimiter TEXT         CSV delimiter (use \t for tab)
                               [default: comma]

  --help                       Show this message and exit
```

## Authentication

### Google Cloud Storage

CloudCat uses [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials). Set up authentication using one of these methods:

```bash
# Option 1: User credentials (for development)
gcloud auth application-default login

# Option 2: Service account (for production)
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
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

# Option 3: IAM role (for EC2/ECS/Lambda)
# Automatically detected
```

## Performance Tips

1. **Use `--no-count`** for large files when you don't need the total record count
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

**Authentication errors**
- GCS: Run `gcloud auth application-default login`
- S3: Run `aws configure` or check your credentials

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

- [ ] Azure Blob Storage support
- [ ] Avro format support
- [ ] ORC format support
- [ ] Interactive mode with pagination
- [ ] SQL-like filtering (`--where` clause)
- [ ] Output to file with `--output-file`
- [ ] Compression support (gzip, snappy, zstd)
- [ ] Configuration file support

## Related Projects

- [s3cmd](https://s3tools.org/s3cmd) — S3 command-line tool
- [gsutil](https://cloud.google.com/storage/docs/gsutil) — Google Cloud Storage CLI
- [aws-cli](https://aws.amazon.com/cli/) — AWS command-line interface
- [duckdb](https://duckdb.org/) — In-process SQL OLAP database

## License

MIT License — see [LICENSE](LICENSE) for details.

## Star History

If you find CloudCat useful, please consider giving it a star on GitHub!

[![Star History Chart](https://api.star-history.com/svg?repos=jonathansudhakar1/cloudcat&type=Date)](https://star-history.com/#jonathansudhakar1/cloudcat&Date)

---

<p align="center">
  Made with ❤️ by <a href="https://github.com/jonathansudhakar1">Jonathan Sudhakar</a>
</p>

<p align="center">
  <a href="https://github.com/jonathansudhakar1/cloudcat/issues">Report Bug</a> •
  <a href="https://github.com/jonathansudhakar1/cloudcat/issues">Request Feature</a>
</p>
