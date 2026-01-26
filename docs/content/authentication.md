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

### Azure Data Lake Storage Gen2 (ADLS Gen2)

CloudCat supports Azure Data Lake Storage Gen2 with multiple authentication methods.

#### Option 1: Storage Account Access Key (Simplest)

Get your access key from Azure Portal: Storage Account → Security + networking → Access keys

```bash
# Pass directly to cloudcat
cloudcat -p abfss://container@account.dfs.core.windows.net/data.parquet --az-access-key "YOUR_KEY"

# Or set as environment variable
export AZURE_STORAGE_ACCESS_KEY="YOUR_KEY"
cloudcat -p abfss://container@account.dfs.core.windows.net/data.parquet
```

#### Option 2: Azure CLI (Development)

Best for local development with Azure AD authentication:

```bash
# Install Azure CLI
brew install azure-cli  # macOS
# or: curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash  # Linux

# Login to Azure
az login

# If you have multiple subscriptions, set the active one
az account set --subscription "Your Subscription Name"
```

**Important:** Your Azure AD account needs the **Storage Blob Data Reader** role (or Contributor) on the storage account. Assign this in Azure Portal: Storage Account → Access Control (IAM) → Add role assignment.

Then use CloudCat:

```bash
cloudcat -p abfss://container@account.dfs.core.windows.net/path/data.parquet
```

#### Option 3: Connection String

Set the full connection string:

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
```

#### Option 4: Service Principal (CI/CD)

For automated pipelines, use a service principal:

```bash
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
```

The service principal needs the **Storage Blob Data Reader** role on the storage account.

### Path Formats

| Provider | URL Format | Example |
|----------|------------|---------|
| GCS | `gcs://bucket/path` or `gs://bucket/path` | `gcs://my-bucket/data/file.csv` |
| S3 | `s3://bucket/path` | `s3://my-bucket/data/file.parquet` |
| Azure ADLS Gen2 | `abfss://container@account.dfs.core.windows.net/path` | `abfss://data@myaccount.dfs.core.windows.net/folder/file.parquet` |

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

**Azure: "AuthorizationPermissionMismatch" or "403 Forbidden"**

This usually means your credentials don't have the right role. For Azure AD authentication (az login), you need the **Storage Blob Data Reader** role on the storage account, not just the subscription.

```bash
# Option 1: Use access key instead
cloudcat -p abfss://container@account.dfs.core.windows.net/data.parquet --az-access-key "YOUR_KEY"

# Option 2: Add role assignment in Azure Portal
# Storage Account → Access Control (IAM) → Add role assignment
# Role: Storage Blob Data Reader
# Assign to: Your user or service principal
```

**Azure: "Azure credentials not found"**

```bash
# Option 1: Use access key
export AZURE_STORAGE_ACCESS_KEY="..."
cloudcat -p abfss://container@account.dfs.core.windows.net/data.parquet

# Option 2: Use Azure CLI
az login
```
