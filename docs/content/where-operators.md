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
| `not contains` | `name not contains test` | Case-insensitive substring exclusion |
| `startswith` | `email startswith admin` | String prefix match |
| `endswith` | `file endswith .csv` | String suffix match |

> **Combine conditions with `AND`/`OR`** (AND binds tighter; parentheses are not supported). Quote values containing the words and/or: `title='Alice and Bob'`.
>
> CloudCat **streams** the file while filtering and stops as soon as `--num-rows` matches are found. For Parquet, row groups whose min/max statistics cannot match are skipped without being downloaded.

### Compound Conditions

```bash
# AND — both must hold
cloudcat s3://bucket/users.parquet --where "status=active AND age>30"

# OR — either may hold
cloudcat gcs://bucket/logs.csv --where "level=ERROR or level=FATAL"

# AND binds tighter than OR:  a  OR  (b AND c)
cloudcat s3://bucket/events.json --where "type=crash or type=error AND fatal=true"
```

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
- The `contains`, `not contains`, `startswith`, and `endswith` operators are case-insensitive
- `--where` streams and stops at `--num-rows` matches; with `-n 0` it scans the whole file
- Combine conditions with `AND`/`OR` (AND binds tighter); quote values containing those words
