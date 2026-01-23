## Installation

### Homebrew (macOS Apple Silicon)

The easiest way to install on Apple Silicon Macs (M1/M2/M3/M4) — no Python required:

```bash
brew install jonathansudhakar1/cloudcat/cloudcat
```

This installs a self-contained binary that includes Python and all dependencies.

> **Intel Mac users:** Homebrew bottles are not available for Intel. Please use `pip install 'cloudcat[all]'` instead.

To upgrade:

```bash
brew upgrade cloudcat
```

> **Note:** On first run, macOS may block the app. Go to System Settings > Privacy & Security and click "Allow", or run:
> ```bash
> xattr -d com.apple.quarantine $(which cloudcat)
> ```

### pip (All Platforms)

Install CloudCat with all features enabled:

```bash
pip install 'cloudcat[all]'
```

This includes support for all cloud providers (GCS, S3, Azure), all file formats (Parquet, Avro, ORC), and all compression types (zstd, lz4, snappy).

### Standard pip Installation

For basic functionality with GCS, S3, and Azure support:

```bash
pip install cloudcat
```

Includes CSV, JSON, and text format support with gzip and bz2 compression.

### Install with Specific Features

Install only what you need:

| Extra | Command | Adds Support For |
|-------|---------|------------------|
| `parquet` | `pip install 'cloudcat[parquet]'` | Apache Parquet files |
| `avro` | `pip install 'cloudcat[avro]'` | Apache Avro files |
| `orc` | `pip install 'cloudcat[orc]'` | Apache ORC files |
| `compression` | `pip install 'cloudcat[compression]'` | zstd, lz4, snappy |
| `zstd` | `pip install 'cloudcat[zstd]'` | Zstandard compression only |
| `lz4` | `pip install 'cloudcat[lz4]'` | LZ4 compression only |
| `snappy` | `pip install 'cloudcat[snappy]'` | Snappy compression only |

### Requirements

- **Homebrew**: macOS Apple Silicon (M1/M2/M3/M4). Intel Mac users should use pip.
- **pip**: Python 3.7 or higher (all platforms)
- **Cloud Credentials**: Configured for your cloud provider (see [Authentication](#authentication))

> **Note**: If using zsh (default on macOS), quotes around extras are required to prevent shell interpretation of brackets.

### Upgrading

Upgrade to the latest version:

```bash
pip install --upgrade cloudcat
```

Or with all extras:

```bash
pip install --upgrade 'cloudcat[all]'
```

### Verifying Installation

Check that CloudCat is installed correctly:

```bash
cloudcat --help
```

You should see the help output with all available options.
