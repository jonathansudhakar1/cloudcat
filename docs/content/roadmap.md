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

### Planned Features

- [ ] **Interactive mode** - Pagination with keyboard navigation
- [ ] **Output to file** - Direct `--output-file` option
- [ ] **Configuration file** - `.cloudcatrc` for defaults
- [ ] **Multiple WHERE conditions** - AND/OR operators
- [ ] **Sampling** - Random row sampling
- [ ] **Profile support** - Named configuration profiles
- [ ] **Delta Lake support** - Read Delta tables
- [ ] **Iceberg support** - Read Iceberg tables

### Under Consideration

- **Write support** - Converting and writing data
- **SQL queries** - Full SQL query support via DuckDB
- **Data profiling** - Basic statistics and profiling
- **Diff mode** - Compare two files
- **Watch mode** - Monitor file changes
- **Plugins** - Custom reader/writer plugins

### Version History

**v0.2.2** (Current)
- Bug fixes and improvements
- Homebrew support for Apple Silicon

**v0.2.0**
- Azure Blob Storage support
- Avro and ORC format support
- WHERE clause filtering
- Row offset/pagination
- Compression support (zstd, lz4, snappy)

**v0.1.0**
- Initial release
- GCS and S3 support
- CSV, JSON, Parquet formats
- Basic functionality

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
