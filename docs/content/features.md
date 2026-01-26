## Features

CloudCat is designed to make previewing cloud data effortless. Here's what it offers:

### Cloud Storage Support

| Provider | URL Scheme | Status |
|----------|------------|--------|
| Google Cloud Storage | `gcs://` or `gs://` | Supported |
| Amazon S3 | `s3://` | Supported |
| Azure Blob Storage | `az://` or `azure://` | Supported |
| Azure Data Lake Gen2 | `abfss://` | Supported |

### File Format Support

CloudCat automatically detects file formats from extensions and handles them appropriately:

| Format | Read | Auto-Detect | Use Case |
|--------|------|-------------|----------|
| CSV | Yes | Yes | General data files |
| JSON | Yes | Yes | API responses, configs |
| JSON Lines | Yes | Yes | Log files, streaming data |
| Parquet | Yes | Yes | Spark/analytics data |
| Avro | Yes | Yes | Kafka, data pipelines |
| ORC | Yes | Yes | Hive, Hadoop ecosystem |
| Text | Yes | Yes | Log files, plain text |
| TSV | Yes | Via `--delimiter` | Tab-separated data |

### Streaming Efficiency

CloudCat optimizes data transfer by streaming only necessary data when possible. This table shows what truly streams for reduced egress costs:

| Format | Compression | Streams | Column Projection | Early Row Stop |
|--------|-------------|---------|-------------------|----------------|
| Parquet | None/Internal | ✓ | ✓ (range requests) | ✓ |
| Parquet | External (.gz) | ✗ | ✗ | ✗ |
| ORC | None/Internal | ✗ | ✗ | ✗ |
| ORC | External (.gz) | ✗ | ✗ | ✗ |
| CSV | None | ✓ | ✗ | ✓ |
| CSV | gzip/zstd/lz4/bz2 | ✓ | ✗ | ✓ |
| CSV | snappy | ✗ | ✗ | ✗ |
| JSON Lines | None/streamable | ✓ | ✗ | ✓ |
| JSON Array | Any | ✗ | ✗ | ✗ |
| Avro | Any | ✓ | ✓ (record-level) | ✓ |
| Text | Any streamable | ✓ | N/A | ✓ |

**Key:**
- **Streams**: Data is read incrementally (low memory, stops early)
- **Column Projection**: Only reads selected columns from storage
- **Early Row Stop**: Stops reading when row limit (`-n`) is reached

**Read Statistics**: CloudCat shows file size vs data read at the bottom of output:
```
File size: 1.2 GB | Data read: 45.2 MB (3.7%)
```

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
