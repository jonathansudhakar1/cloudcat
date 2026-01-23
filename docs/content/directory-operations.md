## Directory Operations

CloudCat intelligently handles directories containing multiple data files, common with Spark, Hive, and distributed processing outputs.

### Multi-File Mode

Control how CloudCat handles directories with the `-m, --multi-file-mode` option:

| Mode | Description |
|------|-------------|
| `auto` | Smart selection based on directory contents (default) |
| `first` | Read only the first data file found |
| `all` | Combine data from all files in the directory |

### Auto Mode (Default)

In auto mode, CloudCat analyzes the directory and makes smart decisions:

```bash
cloudcat -p s3://bucket/spark-output/
```

- Scans directory for data files
- Ignores metadata files (`_SUCCESS`, `_metadata`, `.crc`, etc.)
- Selects appropriate files based on format
- Reports which files were selected

### First File Mode

Read only the first file for quick sampling:

```bash
cloudcat -p gcs://bucket/large-output/ -m first
```

Best for:
- Quick data validation
- Large directories with many files
- When you only need a sample

### All Files Mode

Combine data from multiple files:

```bash
cloudcat -p s3://bucket/daily-logs/ -m all
```

Best for:
- Aggregating partitioned data
- Reading complete datasets
- Directories with related files

### Size Limits

Control maximum data size when reading multiple files:

```bash
# Read up to 100MB of data
cloudcat -p gcs://bucket/events/ -m all --max-size-mb 100
```

Default is 25MB to prevent accidentally loading huge datasets.

### Automatic File Filtering

CloudCat automatically ignores common metadata files:

- `_SUCCESS` - Spark/Hadoop success markers
- `_metadata` - Parquet metadata files
- `_common_metadata` - Parquet common metadata
- `.crc` files - Checksum files
- `.committed` - Transaction markers
- `.pending` - Pending transaction files
- `_temporary` directories - Temporary files

### Examples

#### Spark Output Directory

```bash
# Typical Spark output structure:
# s3://bucket/output/
#   _SUCCESS
#   part-00000-abc.parquet
#   part-00001-def.parquet

cloudcat -p s3://bucket/output/ -i parquet
# Automatically reads part files, ignores _SUCCESS
```

#### Hive Partitioned Data

```bash
# Partitioned structure:
# gcs://bucket/events/
#   year=2024/month=01/data.parquet
#   year=2024/month=02/data.parquet

cloudcat -p gcs://bucket/events/ -m all -i parquet
```

#### Daily Log Files

```bash
# Log directory:
# s3://bucket/logs/
#   2024-01-15.json
#   2024-01-16.json
#   2024-01-17.json

cloudcat -p s3://bucket/logs/ -m all -n 100
```

#### Large Directory Sampling

```bash
# Quick preview of first file only
cloudcat -p gcs://bucket/huge-dataset/ -m first -n 20
```

### Format Detection in Directories

When reading from a directory, you may want to specify the format:

```bash
# Explicitly set format for directory
cloudcat -p s3://bucket/output/ -i parquet

# Auto-detect from first matching file
cloudcat -p gcs://bucket/data/
```

CloudCat examines file extensions to determine format when not specified.

### Tips

- Use `-m first` for quick validation of large directories
- Use `--max-size-mb` to control memory usage with `-m all`
- Specify `-i` format when directory contains mixed file types
- CloudCat preserves column order across multiple files
