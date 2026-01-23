## WHERE Operators

CloudCat supports SQL-like filtering with the `--where` option. Filter your data before it's displayed to focus on exactly what you need.

### Supported Operators

| Operator | Example | Description |
|----------|---------|-------------|
| `=` | `status=active` | Exact match |
| `!=` | `type!=deleted` | Not equal |
| `>` | `age>30` | Greater than |
| `<` | `price<100` | Less than |
| `>=` | `count>=10` | Greater than or equal |
| `<=` | `score<=50` | Less than or equal |
| `contains` | `name contains john` | Case-insensitive substring match |
| `startswith` | `email startswith admin` | String prefix match |
| `endswith` | `file endswith .csv` | String suffix match |

### Usage Examples

#### Exact Match

```bash
# Filter by status
cloudcat -p s3://bucket/users.parquet --where "status=active"

# Filter by category
cloudcat -p gcs://bucket/products.json --where "category=electronics"
```

#### Numeric Comparisons

```bash
# Greater than
cloudcat -p s3://bucket/users.parquet --where "age>30"

# Less than
cloudcat -p gcs://bucket/orders.csv --where "price<100"

# Greater than or equal
cloudcat -p s3://bucket/events.json --where "count>=10"

# Less than or equal
cloudcat -p gcs://bucket/scores.parquet --where "score<=50"
```

#### String Matching

```bash
# Contains (case-insensitive)
cloudcat -p s3://bucket/logs.json --where "message contains error"

# Starts with
cloudcat -p gcs://bucket/users.csv --where "email startswith admin"

# Ends with
cloudcat -p s3://bucket/files.json --where "filename endswith .csv"
```

#### Not Equal

```bash
# Exclude deleted records
cloudcat -p gcs://bucket/records.parquet --where "status!=deleted"

# Exclude specific type
cloudcat -p s3://bucket/events.json --where "type!=test"
```

### Combining with Other Options

```bash
# Filter and select columns
cloudcat -p s3://bucket/users.parquet --where "status=active" -c id,name,email

# Filter and limit rows
cloudcat -p gcs://bucket/events.json --where "type=error" -n 50

# Filter with pagination
cloudcat -p s3://bucket/logs.csv --where "level=ERROR" --offset 100 -n 20

# Filter and export
cloudcat -p gcs://bucket/users.parquet --where "country=US" -o csv -n 0 > us_users.csv
```

### Tips

- String values don't need quotes in the WHERE clause
- Comparisons are type-aware (numeric columns compare numerically)
- The `contains`, `startswith`, and `endswith` operators are case-insensitive
- For best performance, filter on columns that exist in your data
