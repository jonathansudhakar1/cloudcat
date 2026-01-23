## Features

CloudCat is designed to make previewing cloud data effortless. Here's what it offers:

### Cloud Storage Support

| Provider | URL Scheme | Status |
|----------|------------|--------|
| Google Cloud Storage | `gcs://` or `gs://` | Supported |
| Amazon S3 | `s3://` | Supported |
| Azure Blob Storage | `az://` or `azure://` | Supported |

### File Format Support

CloudCat automatically detects file formats from extensions and handles them appropriately:

| Format | Read | Auto-Detect | Streaming | Use Case |
|--------|------|-------------|-----------|----------|
| CSV | Yes | Yes | Yes | General data files |
| JSON | Yes | Yes | Yes | API responses, configs |
| JSON Lines | Yes | Yes | Yes | Log files, streaming data |
| Parquet | Yes | Yes | Yes | Spark/analytics data |
| Avro | Yes | Yes | Yes | Kafka, data pipelines |
| ORC | Yes | Yes | Yes | Hive, Hadoop ecosystem |
| Text | Yes | Yes | Yes | Log files, plain text |
| TSV | Yes | Via `--delimiter` | Yes | Tab-separated data |

### Compression Support

CloudCat automatically detects and decompresses files based on extension:

| Format | Extension | Built-in | Installation |
|--------|-----------|----------|--------------|
| Gzip | `.gz`, `.gzip` | Yes | Included |
| Bzip2 | `.bz2` | Yes | Included |
| Zstandard | `.zst`, `.zstd` | Optional | `pip install cloudcat[zstd]` |
| LZ4 | `.lz4` | Optional | `pip install cloudcat[lz4]` |
| Snappy | `.snappy` | Optional | `pip install cloudcat[snappy]` |

### Output Formats

| Format | Flag | Description |
|--------|------|-------------|
| Table | `-o table` | Beautiful ASCII table with colored headers (default) |
| JSON | `-o json` | Standard JSON Lines output (one record per line) |
| Pretty JSON | `-o jsonp` | Syntax-highlighted, indented JSON with colors |
| CSV | `-o csv` | Comma-separated values for further processing |

### Key Capabilities

- **Schema Inspection** - View column names and data types before previewing data
- **Column Selection** - Display only the columns you need with `--columns`
- **Row Limiting** - Control how many rows to preview with `--num-rows`
- **Row Offset** - Skip first N rows for pagination with `--offset`
- **WHERE Filtering** - Filter rows with SQL-like conditions using `--where`
- **Record Counting** - Get total record counts (instant for Parquet via metadata)
- **Multi-File Reading** - Combine data from multiple files in a directory
- **Custom Delimiters** - Support for tab, pipe, semicolon, and other delimiters
- **Auto Decompression** - Transparent handling of compressed files
- **Directory Intelligence** - Automatically discovers data files in Spark/Hive outputs
