#!/usr/bin/env python
import click
import pandas as pd
import sys
import io
from tabulate import tabulate
from colorama import init, Fore, Style
import json
from urllib.parse import urlparse
import tempfile

# Initialize colorama
init()

# For GCS
try:
    from google.cloud import storage as gcs
    HAS_GCS = True
except ImportError:
    HAS_GCS = False

# For S3
try:
    import boto3
    import botocore
    HAS_S3 = True
except ImportError:
    HAS_S3 = False

# For Parquet
try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False


def parse_cloud_path(path):
    """Parse a cloud storage path into service, bucket, and object components."""
    parsed = urlparse(path)
    
    if parsed.scheme == 'gs' or parsed.scheme == 'gcs':
        service = 'gcs'
        bucket = parsed.netloc
        object_path = parsed.path.lstrip('/')
    elif parsed.scheme == 's3':
        service = 's3'
        bucket = parsed.netloc
        object_path = parsed.path.lstrip('/')
    else:
        raise ValueError(f"Unsupported scheme: {parsed.scheme}. Use gcs:// or s3://")
    
    return service, bucket, object_path


def detect_format_from_path(path):
    """Detect file format from file extension."""
    if path.lower().endswith('.json'):
        return 'json'
    elif path.lower().endswith('.csv'):
        return 'csv'
    elif path.lower().endswith('.parquet'):
        return 'parquet'
    else:
        raise ValueError(f"Could not infer format from path: {path}. Please specify --input-format.")


def get_gcs_stream(bucket_name, object_name):
    """Get a file stream from GCS with minimal downloading."""
    if not HAS_GCS:
        sys.stderr.write(Fore.RED + "Error: google-cloud-storage package is required for GCS access.\n" + 
                        "Install it with: pip install google-cloud-storage\n" + Style.RESET_ALL)
        sys.exit(1)
    
    client = gcs.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    
    # Create a streaming buffer
    buffer = io.BytesIO()
    blob.download_to_file(buffer)
    buffer.seek(0)
    
    return buffer


def get_s3_stream(bucket_name, object_name):
    """Get a file stream from S3."""
    if not HAS_S3:
        sys.stderr.write(Fore.RED + "Error: boto3 package is required for S3 access.\n" + 
                        "Install it with: pip install boto3\n" + Style.RESET_ALL)
        sys.exit(1)
    
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=object_name)
    return response['Body']


def read_csv_data(stream, num_rows, columns=None):
    """Read CSV data from a stream."""
    if num_rows > 0:
        df = pd.read_csv(stream, nrows=num_rows)
    else:
        df = pd.read_csv(stream)
    
    if columns:
        cols = [c.strip() for c in columns.split(',')]
        valid_cols = [c for c in cols if c in df.columns]
        if len(valid_cols) != len(cols):
            missing = set(cols) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        df = df[valid_cols]
    
    return df


def read_json_data(stream, num_rows, columns=None):
    """Read JSON data from a stream."""
    if num_rows > 0:
        df = pd.read_json(stream, lines=True, nrows=num_rows)
    else:
        df = pd.read_json(stream, lines=True)
    
    if columns:
        cols = [c.strip() for c in columns.split(',')]
        valid_cols = [c for c in cols if c in df.columns]
        if len(valid_cols) != len(cols):
            missing = set(cols) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        df = df[valid_cols]
    
    return df


def read_parquet_data(stream, num_rows, columns=None):
    """Read Parquet data from a stream."""
    if not HAS_PARQUET:
        sys.stderr.write(Fore.RED + "Error: pyarrow package is required for Parquet support.\n" + 
                         "Install it with: pip install pyarrow\n" + Style.RESET_ALL)
        sys.exit(1)
    
    # For Parquet, we need a temporary file to properly read the metadata
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_path = temp_file.name
    
    try:
        # If stream is a file-like object, copy to temp file
        if hasattr(stream, 'read'):
            with open(temp_path, 'wb') as f:
                f.write(stream.read())
        else:
            # Assume it's already a path
            temp_path = stream
        
        parquet_file = pq.ParquetFile(temp_path)
        
        # Select columns if specified
        col_names = columns.split(',') if columns else None
        
        # Read the data efficiently
        if num_rows > 0:
            tables = []
            rows_read = 0
            
            for i in range(parquet_file.num_row_groups):
                if rows_read >= num_rows:
                    break
                
                table = parquet_file.read_row_group(i, columns=col_names)
                
                # Limit rows if needed for the last batch
                if rows_read + table.num_rows > num_rows:
                    table = table.slice(0, num_rows - rows_read)
                
                tables.append(table)
                rows_read += min(table.num_rows, num_rows - rows_read)
            
            if tables:
                result_table = pa.concat_tables(tables)
                return result_table.to_pandas()
            else:
                return pd.DataFrame()
        else:
            # Read all data (potentially with column filtering)
            table = parquet_file.read(columns=col_names)
            return table.to_pandas()
    
    finally:
        # Clean up the temporary file
        import os
        try:
            if hasattr(stream, 'read'):  # Only delete if we created temp file
                os.unlink(temp_path)
        except:
            pass


def read_data(service, bucket, object_path, input_format, num_rows, columns=None):
    """Read data from cloud storage."""
    # Get appropriate stream based on service
    if service == 'gcs':
        stream = get_gcs_stream(bucket, object_path)
    elif service == 's3':
        stream = get_s3_stream(bucket, object_path)
    else:
        raise ValueError(f"Unsupported service: {service}")
    
    # Read based on format
    if input_format == 'csv':
        return read_csv_data(stream, num_rows, columns)
    elif input_format == 'json':
        return read_json_data(stream, num_rows, columns)
    elif input_format == 'parquet':
        return read_parquet_data(stream, num_rows, columns)
    else:
        raise ValueError(f"Unsupported format: {input_format}")


def get_record_count(service, bucket, object_path, input_format):
    """Get record count from a file."""
    if input_format == 'parquet' and HAS_PARQUET:
        # For Parquet, we can get count from metadata
        if service == 'gcs':
            stream = get_gcs_stream(bucket, object_path)
        elif service == 's3':
            stream = get_s3_stream(bucket, object_path)
        
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            if hasattr(stream, 'read'):
                with open(temp_path, 'wb') as f:
                    f.write(stream.read())
            else:
                temp_path = stream
            
            parquet_file = pq.ParquetFile(temp_path)
            return parquet_file.metadata.num_rows
        finally:
            import os
            try:
                if hasattr(stream, 'read'):
                    os.unlink(temp_path)
            except:
                pass
    else:
        # For CSV and JSON, we need to count the rows
        click.echo(Fore.YELLOW + "Counting records (this might take a while for large files)..." + Style.RESET_ALL)
        
        # Use pandas to count rows in chunks
        if service == 'gcs':
            stream = get_gcs_stream(bucket, object_path)
        elif service == 's3':
            stream = get_s3_stream(bucket, object_path)
        
        if input_format == 'csv':
            chunk_count = 0
            for chunk in pd.read_csv(stream, chunksize=10000):
                chunk_count += len(chunk)
            return chunk_count
        elif input_format == 'json':
            chunk_count = 0
            for chunk in pd.read_json(stream, lines=True, chunksize=10000):
                chunk_count += len(chunk)
            return chunk_count
        
        return "Unknown"


@click.command()
@click.option('--path', required=True, help='Path to the file (gcs://... or s3://...)')
@click.option('--output-format', type=click.Choice(['json', 'csv', 'table']), default='table', 
              help='Output format (default: table)')
@click.option('--input-format', type=click.Choice(['json', 'csv', 'parquet']), 
              help='Input format (default: inferred from path)')
@click.option('--columns', help='Comma-separated list of columns to display (default: all)')
@click.option('--num-rows', '-n', default=10, type=int, help='Number of rows to display (default: 10)')
@click.option('--schema', type=click.Choice(['show', 'dont_show', 'schema_only']), default='show',
              help='Schema display option (default: show)')
@click.option('--count', is_flag=True, help='Show record count at the end')
def main(path, output_format, input_format, columns, num_rows, schema, count):
    """Display data from files in Google Cloud Storage or AWS S3.
    
    Example usage:
    
    \b
    # Read from GCS
    cloudcat --path gcs://my-bucket/data.csv --output-format table
    
    \b
    # Read from S3 with column selection
    cloudcat --path s3://my-bucket/data.parquet --columns id,name,value
    
    \b 
    # Show record count and limit to 20 rows
    cloudcat --path gcs://bucket/events.json --num-rows 20 --count
    """
    try:
        # Parse the path
        service, bucket, object_path = parse_cloud_path(path)
        
        # Determine input format if not specified
        if not input_format:
            input_format = detect_format_from_path(object_path)
            click.echo(Fore.BLUE + f"Inferred input format: {input_format}" + Style.RESET_ALL)
        
        # Read the data
        df = read_data(service, bucket, object_path, input_format, num_rows, columns)
        
        # Display schema if requested
        if schema in ['show', 'schema_only']:
            click.echo(Fore.GREEN + "Schema:" + Style.RESET_ALL)
            for col, dtype in df.dtypes.items():
                click.echo(f"  {col}: {dtype}")
            click.echo("")
        
        # Exit if only schema was requested
        if schema == 'schema_only':
            return
        
        # Display the data
        if output_format == 'table':
            click.echo(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
        elif output_format == 'json':
            click.echo(df.to_json(orient='records', lines=True))
        elif output_format == 'csv':
            click.echo(df.to_csv(index=False))
        
        # Count records if requested
        if count:
            try:
                total_count = get_record_count(service, bucket, object_path, input_format)
                click.echo(Fore.CYAN + f"\nTotal records: {total_count}" + Style.RESET_ALL)
            except Exception as e:
                click.echo(Fore.YELLOW + f"\nCould not count records: {str(e)}" + Style.RESET_ALL)
    
    except Exception as e:
        click.echo(Fore.RED + f"Error: {str(e)}" + Style.RESET_ALL, err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()