## Roadmap

CloudCat is actively developed. Here's what's been accomplished and what's planned.

### Completed Features

- [x] **Google Cloud Storage support** - Full GCS integration
- [x] **Amazon S3 support** - Full S3 integration with profiles
- [x] **Azure Blob Storage support** - Full Azure integration
- [x] **CSV format** - With custom delimiters
- [x] **JSON format** - Standard JSON and JSON Lines
- [x] **Parquet format** - With efficient column selection
- [x] **Avro format** - Full Avro support
- [x] **ORC format** - Via PyArrow
- [x] **Plain text format** - For log files
- [x] **SQL-like filtering** - WHERE clause support
- [x] **Compression support** - gzip, bz2, zstd, lz4, snappy
- [x] **Row offset/pagination** - Skip and limit rows
- [x] **Schema inspection** - View data types
- [x] **Multi-file directories** - Spark/Hive output support
- [x] **Multiple output formats** - table, json, jsonp, csv
- [x] **Output to file** - Direct `--output-file` option

### Planned Features

- [x] **Multiple WHERE conditions** - AND/OR operators
- [x] **Configuration file** - `~/.config/cloudcat/config.toml` with named profiles
- [x] **Local file support** - `file://` URLs and plain paths
- [x] **Column statistics** - `--stats` per-column profile
- [x] **Shell completion** - bash/zsh/fish via `--completion`
- [ ] **Interactive mode** - Pagination with keyboard navigation
- [ ] **Sampling** - Random row sampling
- [ ] **Delta Lake support** - Read Delta tables
- [ ] **Iceberg support** - Read Iceberg tables

### Under Consideration

- **Write support** - Converting and writing data
- **SQL queries** - Full SQL query support via DuckDB
- **Data profiling** - Basic statistics and profiling
- **Diff mode** - Compare two files
- **Watch mode** - Monitor file changes
- **Plugins** - Custom reader/writer plugins

### Contributing to the Roadmap

Have a feature idea? We'd love to hear it!

1. Check [existing issues](https://github.com/jonathansudhakar1/cloudcat/issues) for similar requests
2. Open a new issue describing:
   - The use case
   - How it would work
   - Why it would be valuable
3. Join the discussion

### Feedback

Your feedback shapes CloudCat's development:

- [Star the repo](https://github.com/jonathansudhakar1/cloudcat) to show support
- [Open issues](https://github.com/jonathansudhakar1/cloudcat/issues) for bugs or features
- [Contribute](https://github.com/jonathansudhakar1/cloudcat/pulls) pull requests
