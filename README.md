# CloudCat

A command-line utility to read and display files from cloud storage (Google Cloud Storage and AWS S3).

## Installation

```bash
# Basic installation
pip install cloudcat

# With specific provider support
pip install cloudcat[gcs]  # For Google Cloud Storage
pip install cloudcat[s3]   # For AWS S3
pip install cloudcat[parquet]  # For Parquet file support

# Full installation with all dependencies
pip install cloudcat[all]
```

## Usage

```bash
# Basic usage
cloudcat --path gcs://bucket/file.csv

# With output format specified
cloudcat --path s3://bucket/file.json --output-format json

# Select specific columns
cloudcat --path gcs://bucket/file.parquet --columns id,name,timestamp

# Limit number of rows
cloudcat --path s3://bucket/file.csv --num-rows 50

# Show schema only
cloudcat --path gcs://bucket/file.parquet --schema schema_only

# Get record count
cloudcat --path s3://bucket/file.json --count
```

## Command-Line Options

- `--path`: Path to the file (required), format: `gcs://bucket/path` or `s3://bucket/path`
- `--output-format`: Output format, choices: `json`, `csv`, `table` (default)
- `--input-format`: Input format, choices: `json`, `csv`, `parquet` (default: inferred from path)
- `--columns`: Comma-separated list of columns to display (default: all)
- `--num-rows`, `-n`: Number of rows to display (default: 10)
- `--schema`: Schema display option, choices: `show` (default), `dont_show`, `schema_only`
- `--count`: Show record count at the end (flag)

## Authentication

CloudCat uses the default authentication mechanisms for each cloud provider:

- For GCS: Application Default Credentials (ADC)
- For S3: AWS credentials from environment, ~/.aws/credentials, etc.

Make sure you have the appropriate credentials configured before using the tool.

## Examples

### Reading a CSV from Google Cloud Storage

```bash
cloudcat --path gcs://my-data-bucket/sales/monthly.csv --num-rows 20
```

### Showing only the schema of a Parquet file in S3

```bash
cloudcat --path s3://analytics-bucket/events.parquet --schema schema_only
```

### Getting a JSON output from a CSV file

```bash
cloudcat --path gcs://data-bucket/users.csv --output-format json
```

### Counting records in a large JSON file

```bash
cloudcat --path s3://logs-bucket/app-logs.json --count --num-rows 0
```

## License

MIT