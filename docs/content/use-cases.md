## Use Cases

Real-world scenarios where CloudCat shines.

### Debugging Spark Jobs

Quickly validate Spark job output without downloading files:

```bash
# Check output of a Spark job
cloudcat -p gcs://data-lake/jobs/daily-etl/output/ -i parquet -n 20

# Verify schema matches expectations
cloudcat -p s3://analytics/spark-output/ -s schema_only

# Sample data from large output
cloudcat -p gcs://bucket/aggregations/ -m first -n 50
```

### Log Analysis

Preview and filter log files stored in cloud storage:

```bash
# Preview recent logs
cloudcat -p gcs://logs/app/2024-01-15/ -m all -n 50

# Filter for errors
cloudcat -p s3://logs/api/ --where "level=ERROR" -n 100

# Search log messages
cloudcat -p gcs://logs/app/ --where "message contains timeout"

# Export errors for analysis
cloudcat -p s3://logs/errors/ -o json -n 0 | jq 'select(.status >= 500)'
```

### Data Validation

Verify data quality and structure before processing:

```bash
# Quick sanity check on data export
cloudcat -p gcs://exports/daily/users.csv -s show

# Verify record count
cloudcat -p s3://warehouse/transactions.parquet

# Check for null values (preview and inspect)
cloudcat -p gcs://data/customers.parquet -n 100

# Validate schema before ETL
cloudcat -p s3://input/raw-data.json -s schema_only
```

### Format Conversion

Convert between data formats using CloudCat:

```bash
# Convert Parquet to CSV
cloudcat -p gcs://bucket/data.parquet -o csv -n 0 > data.csv

# Convert JSON to CSV for spreadsheet import
cloudcat -p s3://api-dumps/response.json -o csv > data.csv

# Convert tab-separated to comma-separated
cloudcat -p gcs://imports/data.tsv -d "\t" -o csv > converted.csv

# Export Avro as JSON Lines
cloudcat -p s3://kafka/events.avro -o json -n 0 > events.jsonl
```

### Data Exploration

Understand unfamiliar datasets quickly:

```bash
# View schema of unknown file
cloudcat -p s3://vendor-data/export.parquet -s schema_only

# Preview first few rows
cloudcat -p gcs://bucket/new-data.csv -n 5

# Check all columns
cloudcat -p s3://bucket/wide-table.parquet -n 3
```

### Data Sampling

Get representative samples from large datasets:

```bash
# Random-ish sample (use offset)
cloudcat -p gcs://bucket/huge-table.parquet --offset 10000 -n 100

# Sample from each partition
cloudcat -p s3://bucket/year=2024/month=01/ -m first -n 50
cloudcat -p s3://bucket/year=2024/month=02/ -m first -n 50

# Quick peek at different columns
cloudcat -p gcs://bucket/data.parquet -c user_id,event_type -n 20
cloudcat -p gcs://bucket/data.parquet -c timestamp,value -n 20
```

### Pipeline Debugging

Debug data pipeline issues:

```bash
# Check intermediate outputs
cloudcat -p s3://pipeline/stage1-output/ -i parquet -n 10
cloudcat -p s3://pipeline/stage2-output/ -i parquet -n 10

# Compare schemas between stages
cloudcat -p gcs://etl/raw/ -s schema_only
cloudcat -p gcs://etl/transformed/ -s schema_only

# Find records with specific IDs
cloudcat -p s3://data/users.parquet --where "user_id=12345"
```

### Kafka/Event Streaming

Preview data from Kafka exports:

```bash
# Read Avro files from Kafka Connect
cloudcat -p s3://kafka-exports/topic-name/ -i avro

# Filter events by type
cloudcat -p gcs://events/user-actions/ --where "event_type=purchase"

# Preview JSON events
cloudcat -p s3://kinesis/events.jsonl -o jsonp
```

### Multi-Cloud Data Access

Work with data across multiple cloud providers:

```bash
# Compare data between clouds
cloudcat -p gcs://source/data.parquet -c id,value -n 100
cloudcat -p s3://destination/data.parquet -c id,value -n 100

# Verify replication
cloudcat -p gcs://primary/users.csv
cloudcat -p abfss://backup@account.dfs.core.windows.net/users.csv
```

### Integration with Other Tools

Combine CloudCat with other command-line tools:

```bash
# Count records with wc
cloudcat -p s3://bucket/data.csv -o csv -n 0 | wc -l

# Filter with grep
cloudcat -p gcs://logs/app.json -o json | grep "ERROR"

# Process with awk
cloudcat -p s3://data/report.csv -o csv | awk -F',' '{sum+=$3} END {print sum}'

# Sort and unique
cloudcat -p gcs://data/users.csv -c country -o csv -n 0 | sort | uniq -c
```
