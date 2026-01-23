## Output Formats

CloudCat supports multiple output formats to suit different workflows. Use the `-o, --output-format` option to choose.

### Table (Default)

Beautiful ASCII tables with colored headers, perfect for terminal viewing:

```bash
cloudcat -p gcs://bucket/data.csv
```

Output:
```
┌────────┬─────────────┬─────────────────────┬────────────────────┐
│ id     │ name        │ email               │ created_at         │
├────────┼─────────────┼─────────────────────┼────────────────────┤
│ 1      │ Alice       │ alice@example.com   │ 2024-01-15 10:30   │
│ 2      │ Bob         │ bob@example.com     │ 2024-01-15 11:45   │
│ 3      │ Charlie     │ charlie@example.com │ 2024-01-16 09:00   │
└────────┴─────────────┴─────────────────────┴────────────────────┘
```

Best for:
- Interactive terminal use
- Quick data inspection
- Readable output

### JSON Lines

Standard JSON Lines format (one JSON object per line):

```bash
cloudcat -p s3://bucket/data.parquet -o json
```

Output:
```json
{"id": 1, "name": "Alice", "email": "alice@example.com"}
{"id": 2, "name": "Bob", "email": "bob@example.com"}
{"id": 3, "name": "Charlie", "email": "charlie@example.com"}
```

Best for:
- Piping to `jq` for processing
- Integration with other tools
- Machine-readable output

#### Processing with jq

```bash
# Filter by field
cloudcat -p s3://bucket/events.json -o json | jq 'select(.status == "error")'

# Extract specific fields
cloudcat -p gcs://bucket/users.parquet -o json | jq '.email'

# Count by field
cloudcat -p s3://bucket/logs.json -o json -n 0 | jq -s 'group_by(.level) | map({level: .[0].level, count: length})'
```

### Pretty JSON

Syntax-highlighted, indented JSON for human readability:

```bash
cloudcat -p gcs://bucket/config.json -o jsonp
```

Output:
```json
{
  "id": 1,
  "name": "Alice",
  "metadata": {
    "created": "2024-01-15",
    "tags": ["user", "active"]
  }
}
```

Best for:
- Viewing nested JSON structures
- Debugging API responses
- Human-readable inspection

### CSV

Comma-separated values for export and further processing:

```bash
cloudcat -p s3://bucket/data.parquet -o csv
```

Output:
```
id,name,email,created_at
1,Alice,alice@example.com,2024-01-15 10:30
2,Bob,bob@example.com,2024-01-15 11:45
3,Charlie,charlie@example.com,2024-01-16 09:00
```

Best for:
- Exporting to spreadsheets
- Further data processing
- Format conversion

### Export Examples

#### Convert Parquet to CSV

```bash
cloudcat -p gcs://bucket/data.parquet -o csv -n 0 > data.csv
```

#### Export Specific Columns

```bash
cloudcat -p s3://bucket/users.parquet -c email,created_at -o csv -n 0 > emails.csv
```

#### Export Filtered Data

```bash
cloudcat -p gcs://bucket/events.json --where "status=error" -o csv -n 0 > errors.csv
```

#### Convert JSON to CSV

```bash
cloudcat -p s3://bucket/api-response.json -o csv > response.csv
```

### Combining Output Formats with Other Options

```bash
# Table with column selection
cloudcat -p gcs://bucket/data.csv -c id,name,email -o table

# JSON with filtering
cloudcat -p s3://bucket/users.parquet --where "active=true" -o json

# CSV with row limit
cloudcat -p gcs://bucket/events.json -o csv -n 100

# Pretty JSON with schema
cloudcat -p s3://bucket/config.json -o jsonp -s show
```

### Tips

- Use `-n 0` to output all rows when exporting
- Use `table` for interactive inspection, `json` for piping, `csv` for export
- Redirect output to a file with `> filename` for large exports
- The `jsonp` format includes colors; redirect to file loses color codes
