## Troubleshooting

Solutions to common issues when using CloudCat.

### Missing Package Errors

#### "google-cloud-storage package is required"

```bash
pip install cloudcat
# or
pip install google-cloud-storage
```

#### "boto3 package is required"

```bash
pip install cloudcat
# or
pip install boto3
```

#### "azure-storage-blob package is required"

```bash
pip install cloudcat
# or
pip install azure-storage-blob azure-identity
```

#### "pyarrow package is required"

For Parquet or ORC file support:

```bash
pip install 'cloudcat[parquet]'
# or
pip install pyarrow
```

#### "fastavro package is required"

For Avro file support:

```bash
pip install 'cloudcat[avro]'
# or
pip install fastavro
```

#### "zstandard package is required for .zst files"

```bash
pip install 'cloudcat[zstd]'
# or for all compression:
pip install 'cloudcat[compression]'
```

#### "lz4 package is required for .lz4 files"

```bash
pip install 'cloudcat[lz4]'
```

#### "python-snappy package is required for .snappy files"

```bash
pip install 'cloudcat[snappy]'
```

### Authentication Errors

#### GCS: "Could not automatically determine credentials"

Set up Google Cloud authentication:

```bash
# Option 1: User credentials
gcloud auth application-default login

# Option 2: Service account
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Option 3: CLI option
cloudcat -p gcs://bucket/file.csv --credentials /path/to/key.json
```

#### S3: "Unable to locate credentials"

Set up AWS authentication:

```bash
# Option 1: Configure AWS CLI
aws configure

# Option 2: Environment variables
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"

# Option 3: Use named profile
cloudcat -p s3://bucket/file.csv --profile myprofile
```

#### Azure: "Azure credentials not found"

Set up Azure authentication:

```bash
# Option 1: Connection string
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"

# Option 2: Azure AD
export AZURE_STORAGE_ACCOUNT_URL="https://account.blob.core.windows.net"
az login

# Option 3: Specify account
cloudcat -p az://container/file.csv --account mystorageaccount
```

### Format Detection Issues

#### "Could not infer format from path"

When CloudCat can't determine the file format:

```bash
# Specify the format explicitly
cloudcat -p gcs://bucket/data -i parquet
cloudcat -p s3://bucket/file -i csv
cloudcat -p az://container/logs -i json
```

#### Reading files without extensions

```bash
cloudcat -p s3://bucket/data-file -i parquet
```

### Access Permission Errors

#### "Access Denied" or "403 Forbidden"

Check that your credentials have the necessary permissions:

**GCS:**
- `storage.objects.get` for reading files
- `storage.objects.list` for listing directories

**S3:**
- `s3:GetObject` for reading files
- `s3:ListBucket` for listing directories

**Azure:**
- `Storage Blob Data Reader` role or equivalent

### Network Issues

#### Timeout errors

For slow connections or large files:

- Use `--num-rows` to limit data transfer
- Counting is off by default (use `--count` only if needed)
- Check network connectivity to the cloud provider

#### "Connection reset" errors

May indicate network instability. Try:

```bash
# Smaller preview (counting is already off by default)
cloudcat -p s3://bucket/file.csv -n 10
```

### Memory Issues

#### "MemoryError" or system slowdown

When previewing large files:

```bash
# Limit rows
cloudcat -p gcs://bucket/huge.parquet -n 100

# Don't load all rows
# Avoid: cloudcat -p s3://bucket/huge.csv -n 0

# Limit directory size
cloudcat -p s3://bucket/large-dir/ -m all --max-size-mb 10
```

### CSV Issues

#### Wrong columns or parsing errors

For non-standard CSV files:

```bash
# Tab-separated
cloudcat -p gcs://bucket/data.tsv -d "\t"

# Pipe-delimited
cloudcat -p s3://bucket/data.txt -d "|"

# Semicolon-delimited
cloudcat -p gcs://bucket/data.csv -d ";"
```

### Directory Issues

#### "No data files found in directory"

Check that:

1. The directory contains files with recognized extensions
2. Files aren't all metadata files (`_SUCCESS`, `.crc`, etc.)
3. You have permission to list the directory

```bash
# Specify format explicitly
cloudcat -p s3://bucket/output/ -i parquet
```

### Getting Help

If you're still having issues:

1. Check you're using the latest version: `pip install --upgrade cloudcat`
2. Try with `--help` to see all options
3. [Open an issue](https://github.com/jonathansudhakar1/cloudcat/issues) on GitHub with:
   - CloudCat version
   - Python version
   - Full error message
   - Command that caused the error
