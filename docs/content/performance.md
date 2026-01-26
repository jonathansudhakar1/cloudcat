## Performance Tips

Optimize CloudCat for faster performance and lower data transfer costs.

### 1. Counting is Off by Default

CloudCat no longer counts total records by default, which speeds up operations and reduces data transfer. If you need the total count, use `--count`:

```bash
cloudcat -p s3://bucket/huge-file.csv --count
```

Note: Parquet files show instant record counts from metadata without extra scanning.

### 2. Prefer Parquet Format

Parquet files offer the best performance with CloudCat:

- **Instant record counts** from metadata (no file scan needed)
- **Column pruning** when using `--columns` (only reads selected columns)
- **Better compression** means less data transfer

```bash
# Record count is instant for Parquet
cloudcat -p gcs://bucket/data.parquet

# Column selection reads only needed columns
cloudcat -p s3://bucket/wide-table.parquet -c id,name,email
```

### 3. Limit Rows with --num-rows

Reduce data transfer by limiting rows:

```bash
# Preview only 20 rows instead of default 10
cloudcat -p gcs://bucket/data.csv -n 20

# Don't use -n 0 (all rows) unless you need everything
```

### 4. Select Only Needed Columns

With columnar formats (Parquet, ORC), column selection reduces data transfer:

```bash
# Reads only 3 columns instead of all 50
cloudcat -p s3://bucket/wide-table.parquet -c user_id,event_type,timestamp
```

### 5. Use First File Mode for Directories

When you only need a sample from a directory with many files:

```bash
# Read only the first file
cloudcat -p gcs://bucket/spark-output/ -m first

# Instead of reading all files
cloudcat -p gcs://bucket/spark-output/ -m all
```

### 6. Set Appropriate Size Limits

Control memory usage when reading multiple files:

```bash
# Limit to 10MB for quick preview
cloudcat -p s3://bucket/logs/ -m all --max-size-mb 10

# Increase for complete datasets
cloudcat -p s3://bucket/data/ -m all --max-size-mb 100
```

### 7. Use Schema-Only for Structure Checks

When you only need to check the schema:

```bash
# Instant - doesn't read data
cloudcat -p gcs://bucket/data.parquet -s schema_only
```

### 8. Compression Considerations

CloudCat handles compressed files efficiently:

- **Gzip/Bzip2** - Built-in, always available
- **Zstandard** - Fast decompression, good for large files
- **LZ4** - Fastest decompression
- **Snappy** - Good balance of speed and ratio

For best performance with large files, prefer zstd or lz4:

```bash
cloudcat -p s3://bucket/data.csv.zst -n 100
```

### 9. Network Considerations

CloudCat streams data, so network latency matters:

- Run CloudCat close to your data (same region)
- Use AWS EC2/GCP Compute/Azure VMs in the same region as your buckets
- For local development, expect slower performance due to network transfer

### 10. Memory Management

For very large previews, be mindful of memory:

```bash
# This loads all data into memory
cloudcat -p s3://bucket/huge.parquet -n 0

# Better: limit rows
cloudcat -p s3://bucket/huge.parquet -n 1000
```

### Performance Comparison

| Operation | CSV | JSON | Parquet |
|-----------|-----|------|---------|
| Record Count | Slow (full scan) | Slow (full scan) | Instant (metadata) |
| Column Selection | Full file read | Full file read | Reads only selected |
| First N Rows | Fast (stops early) | Fast (stops early) | Fast |
| Compression | Standard | Standard | Built-in, efficient |

### Quick Reference

| Goal | Recommendation |
|------|----------------|
| Fastest preview | `-n 10` (counting is off by default) |
| Check structure | `-s schema_only` |
| Large directories | `-m first` |
| Wide tables | `-c col1,col2,col3` |
| Memory efficiency | Set reasonable `-n` value |
