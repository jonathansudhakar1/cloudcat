# CloudCat

A command-line utility to read and display data from cloud storage (Google Cloud Storage and AWS S3) with advanced features for handling directories, multiple files, and formatting output.

## Features

- **Cloud Storage Support**: Read files from GCS (`gcs://`) and S3 (`s3://`)
- **Multiple File Formats**: Support for CSV, JSON, and Parquet
- **CSV Delimiter Support**: Handle tab-delimited and other custom-delimited files
- **Intelligent Directory Handling**: Automatically find and read data from directories
- **Multi-File Reading**: Combine data from multiple small files (up to configurable size)
- **Streaming**: Optimize to avoid downloading entire files when possible
- **Output Formatting**: Display as tables, JSON, pretty JSON with colors, or CSV
- **Schema Display**: View full schema information for all columns
- **Column Selection**: Filter to show only specific columns
- **Record Counting**: Automatically provides total record counts
- **Row Limiting**: Control how many rows to display

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

## Quick Start

```bash
# Basic file reading
cloudcat --path gcs://bucket/file.csv

# Reading tab-delimited files
cloudcat --path s3://bucket/data.csv --delimiter "\t"

# Reading from a directory (automatically finds data files)
cloudcat --path s3://bucket/spark-output/

# Reading multiple files (up to 25MB)
cloudcat --path gcs://bucket/logs/ --multi-file-mode all
```

## Command-Line Options

| Option | Short | Description |
|--------|-------|-------------|
| `--path` | `-p` | Path to the file or directory (required), format: `gcs://bucket/path` or `s3://bucket/path` |
| `--output-format` | `-o` | Output format: `table` (default), `json`, `jsonp` (pretty JSON), `csv` |
| `--input-format` | `-i` | Input format: `csv`, `json`, `parquet` (default: inferred from path) |
| `--columns` | `-c` | Comma-separated list of columns to display (default: all) |
| `--num-rows` | `-n` | Number of rows to display (default: 10) |
| `--schema` | `-s` | Schema display: `show` (default), `dont_show`, `schema_only` |
| `--no-count` | | Disable record count display (counts shown by default) |
| `--multi-file-mode` | `-m` | How to handle directories: `auto` (default), `first`, `all` |
| `--max-size-mb` | | Maximum size in MB to read for multi-file mode (default: 25) |
| `--delimiter` | `-d` | Delimiter to use for CSV files (use `\t` for tab) |

## Directory Handling

When pointing to a directory (path ending with `/`), CloudCat can operate in three modes:

- **first**: Read only the first suitable non-empty file
- **auto**: Automatically decide between single file and multi-file reading
- **all**: Always read multiple files up to the specified size limit

When reading directories, CloudCat automatically:
- Skips empty files
- Ignores metadata files like `_SUCCESS`, `.crc`, etc.
- Prioritizes files matching the specified format
- Provides a summary of selected files

## CSV Delimiter Support

CloudCat can handle various CSV delimiter types:

```bash
# Tab-delimited files
cloudcat --path gcs://bucket/data.tsv --delimiter "\t"

# Pipe-delimited files
cloudcat --path s3://data/extract.csv --delimiter "|"

# Semicolon-delimited files (common in Europe)
cloudcat --path gcs://reports/data.csv --delimiter ";"
```

## Output Formats

- **table**: Formatted table with colored headers (default)
- **json**: Standard JSON, one object per line
- **jsonp**: Pretty JSON with syntax highlighting and colors
- **csv**: Comma-separated values format

## Authentication

CloudCat uses the default authentication mechanisms for each cloud provider:

- For GCS: Application Default Credentials (ADC)
- For S3: AWS credentials from environment, ~/.aws/credentials, etc.

Make sure you have the appropriate credentials configured before using the tool.

## Example Use Cases

### Reading Tab-Delimited Files

```bash
# Read a tab-delimited file and show as table
cloudcat --path gcs://analytics/export.tsv --delimiter "\t"

# Convert tab-delimited to JSON
cloudcat --path s3://exports/data.tsv --delimiter "\t" --output-format jsonp
```

### Exploring a Spark Output Directory

```bash
# Automatically find and read data files, skipping metadata files
cloudcat --path gcs://analytics/spark-job-output/ --input-format parquet

# Read multiple files to get a more representative sample
cloudcat --path gcs://analytics/spark-job-output/ --multi-file-mode all --max-size-mb 50
```

### Quick Data Analysis

```bash
# See the schema of a dataset
cloudcat --path s3://data-lake/customers.parquet --schema schema_only

# Look at specific columns with pretty formatting
cloudcat --path gcs://analytics/events.json --columns user_id,event_type,timestamp --output-format jsonp
```

### Data Export

```bash
# Export data as CSV
cloudcat --path s3://exports/data.parquet --num-rows 1000 --output-format csv > exported_data.csv

# Convert tab-delimited to comma-delimited
cloudcat --path gcs://exports/data.tsv --delimiter "\t" --output-format csv > converted.csv
```

### Handling Small Files

```bash
# Read and combine data from a directory with many small files
cloudcat --path gcs://logs/daily/ --multi-file-mode all --max-size-mb 100
```

## License

MIT