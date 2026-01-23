## Authentication

CloudCat uses standard authentication methods for each cloud provider. Configure your credentials once and CloudCat will use them automatically.

### Google Cloud Storage (GCS)

CloudCat uses [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials) for GCS authentication.

#### Option 1: User Credentials (Development)

Best for local development:

```bash
gcloud auth application-default login
```

This opens a browser for Google account authentication.

#### Option 2: Service Account (Environment Variable)

Set the path to your service account JSON file:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

Then use CloudCat normally:

```bash
cloudcat -p gcs://bucket/data.csv
```

#### Option 3: Service Account (CLI Option)

Pass the credentials file directly:

```bash
cloudcat -p gcs://bucket/data.csv --credentials /path/to/service-account.json
```

#### Option 4: Specify GCP Project

If your credentials have access to multiple projects:

```bash
cloudcat -p gcs://bucket/data.csv --project my-gcp-project
```

### Amazon S3

CloudCat uses the standard [AWS credential chain](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html).

#### Option 1: Environment Variables

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
```

#### Option 2: AWS Credentials File

Configure credentials using the AWS CLI:

```bash
aws configure
```

This creates `~/.aws/credentials` with your access keys.

#### Option 3: Named Profile

Use a specific AWS profile:

```bash
cloudcat -p s3://bucket/data.csv --profile production
```

Profiles are defined in `~/.aws/credentials`:

```ini
[production]
aws_access_key_id = AKIA...
aws_secret_access_key = ...
region = us-west-2
```

#### Option 4: IAM Role (EC2/ECS/Lambda)

When running on AWS infrastructure (EC2, ECS, Lambda), CloudCat automatically uses the attached IAM role. No configuration needed.

### Azure Blob Storage

CloudCat supports multiple authentication methods for Azure.

#### Option 1: Connection String (Simplest)

Set the full connection string:

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
```

#### Option 2: Azure AD Authentication

Use Azure CLI login with account URL:

```bash
# Set the account URL
export AZURE_STORAGE_ACCOUNT_URL="https://youraccount.blob.core.windows.net"

# Login with Azure CLI
az login
```

CloudCat will use DefaultAzureCredential to authenticate.

#### Option 3: Storage Account (CLI Option)

Specify the storage account directly:

```bash
cloudcat -p az://container/data.csv --account mystorageaccount
```

This requires either a connection string or Azure AD authentication to be configured.

### Path Formats

| Provider | URL Format | Example |
|----------|------------|---------|
| GCS | `gcs://bucket/path` or `gs://bucket/path` | `gcs://my-bucket/data/file.csv` |
| S3 | `s3://bucket/path` | `s3://my-bucket/data/file.parquet` |
| Azure | `az://container/path` or `azure://container/path` | `az://my-container/data/file.json` |

### Troubleshooting Authentication

**GCS: "Could not automatically determine credentials"**

```bash
gcloud auth application-default login
```

**S3: "Unable to locate credentials"**

```bash
aws configure
# Or set environment variables
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
```

**Azure: "Azure credentials not found"**

```bash
# Option 1: Set connection string
export AZURE_STORAGE_CONNECTION_STRING="..."

# Option 2: Use Azure CLI
export AZURE_STORAGE_ACCOUNT_URL="https://account.blob.core.windows.net"
az login
```
