## Quick Start

Get started with CloudCat in seconds. Here are the most common operations:

### Preview a CSV File

```bash
# From Google Cloud Storage
cloudcat -p gcs://my-bucket/data.csv

# From Amazon S3
cloudcat -p s3://my-bucket/data.csv

# From Azure Blob Storage
cloudcat -p az://my-container/data.csv
```

### Preview Parquet Files

```bash
# Preview first 10 rows (default)
cloudcat -p s3://my-bucket/analytics/events.parquet

# Preview 50 rows
cloudcat -p gcs://my-bucket/data.parquet -n 50
```

### Preview JSON Data

```bash
# Standard JSON
cloudcat -p s3://my-bucket/config.json

# JSON Lines (newline-delimited JSON)
cloudcat -p gcs://my-bucket/events.jsonl

# With pretty formatting
cloudcat -p az://my-container/logs.json -o jsonp
```

### Select Specific Columns

```bash
cloudcat -p gcs://bucket/users.json -c id,name,email
```

### Filter Rows

```bash
# Exact match
cloudcat -p s3://bucket/users.parquet --where "status=active"

# Numeric comparison
cloudcat -p gcs://bucket/events.json --where "age>30"

# String contains
cloudcat -p s3://bucket/logs.csv --where "message contains error"
```

### View Schema Only

```bash
cloudcat -p s3://bucket/events.parquet -s schema_only
```

### Read Compressed Files

CloudCat automatically decompresses files:

```bash
# Gzip
cloudcat -p gcs://bucket/data.csv.gz

# Zstandard
cloudcat -p s3://bucket/events.parquet.zst

# LZ4
cloudcat -p s3://bucket/data.csv.lz4
```

### Read from Spark Output Directory

```bash
cloudcat -p s3://my-bucket/spark-output/ -i parquet
```

CloudCat automatically discovers data files and ignores metadata files like `_SUCCESS`.

### Pagination

```bash
# Skip first 100 rows, show next 10
cloudcat -p gcs://bucket/data.csv --offset 100 -n 10
```

### Convert and Export

```bash
# Convert Parquet to CSV
cloudcat -p gcs://bucket/data.parquet -o csv -n 0 > data.csv

# Export specific columns
cloudcat -p s3://bucket/users.parquet -c email,created_at -o csv -n 0 > emails.csv

# Pipe to jq for JSON processing
cloudcat -p s3://bucket/events.json -o json | jq '.status'
```
