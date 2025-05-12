#!/usr/bin/env python
import click
import pandas as pd
import sys
import io
import os
from tabulate import tabulate
from colorama import init, Fore, Style
import json
from urllib.parse import urlparse
import tempfile
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def list_gcs_directory(bucket_name, prefix):
    """List files in a GCS directory."""
    if not HAS_GCS:
        sys.stderr.write(Fore.RED + "Error: google-cloud-storage package is required for GCS access.\n" + 
                        "Install it with: pip install google-cloud-storage\n" + Style.RESET_ALL)
        sys.exit(1)
    
    client = gcs.Client()
    bucket = client.bucket(bucket_name)
    
    # Ensure prefix ends with / to indicate a directory
    if not prefix.endswith('/'):
        prefix = prefix + '/'
    
    blobs = bucket.list_blobs(prefix=prefix)
    
    # Return a list of files with their size
    return [(blob.name, blob.size) for blob in blobs if not blob.name.endswith('/')]


def list_s3_directory(bucket_name, prefix):
    """List files in an S3 directory."""
    if not HAS_S3:
        sys.stderr.write(Fore.RED + "Error: boto3 package is required for S3 access.\n" + 
                        "Install it with: pip install boto3\n" + Style.RESET_ALL)
        sys.exit(1)
    
    s3 = boto3.client('s3')
    
    # Ensure prefix ends with / to indicate a directory
    if not prefix.endswith('/'):
        prefix = prefix + '/'
    
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    
    # Return a list of files with their size
    file_list = []
    for page in pages:
        if 'Contents' in page:
            file_list.extend([(item['Key'], item['Size']) for item in page['Contents'] 
                              if not item['Key'].endswith('/')])
    
    return file_list


def get_files_for_multiread(service, bucket, prefix, input_format=None, max_size_mb=25):
    """Get a list of files to read up to max_size_mb."""
    if service == 'gcs':
        files = list_gcs_directory(bucket, prefix)
    elif service == 's3':
        files = list_s3_directory(bucket, prefix)
    else:
        raise ValueError(f"Unsupported service: {service}")
    
    if not files:
        raise ValueError(f"No files found in {service}://{bucket}/{prefix}")
    
    # Filter files by size > 0
    non_empty_files = [f for f in files if f[1] > 0]
    
    if not non_empty_files:
        raise ValueError(f"No non-empty files found in {service}://{bucket}/{prefix}")
    
    # Skip common metadata files
    skip_patterns = [r'_SUCCESS$', r'\.crc$', r'\.committed$', r'\.pending$', r'_metadata$']
    non_metadata_files = []
    
    for file_name, file_size in non_empty_files:
        # Skip if the file matches any of the patterns to ignore
        if not any(re.search(pattern, file_name) for pattern in skip_patterns):
            non_metadata_files.append((file_name, file_size))
    
    # If no non-metadata files found, use all non-empty files
    if not non_metadata_files:
        click.echo(Fore.YELLOW + "Only found metadata files, using all non-empty files." + Style.RESET_ALL)
        filtered_files = non_empty_files
    else:
        filtered_files = non_metadata_files
    
    # Filter by input format if specified
    if input_format:
        format_ext_map = {
            'csv': r'\.csv$',
            'json': r'\.json$',
            'parquet': r'\.parquet$'
        }
        
        format_regex = format_ext_map.get(input_format, None)
        if format_regex:
            matching_files = [f for f in filtered_files if re.search(format_regex, f[0], re.IGNORECASE)]
            if matching_files:
                filtered_files = matching_files
            else:
                click.echo(Fore.YELLOW + f"No files matching format '{input_format}' found. Using all available files." + Style.RESET_ALL)
    
    # Sort by name for deterministic behavior
    filtered_files.sort(key=lambda x: x[0])
    
    # Select files up to max_size_mb
    max_size_bytes = max_size_mb * 1024 * 1024
    selected_files = []
    total_size = 0
    
    for file_name, file_size in filtered_files:
        selected_files.append((file_name, file_size))
        total_size += file_size
        
        if total_size >= max_size_bytes:
            break
    
    if not selected_files:
        raise ValueError(f"No suitable files found in {service}://{bucket}/{prefix}")
    
    # Report on selected files
    total_mb = total_size / (1024 * 1024)
    click.echo(Fore.BLUE + f"Reading {len(selected_files)} files totaling {total_mb:.2f} MB" + Style.RESET_ALL)
    
    return selected_files


def find_first_non_empty_file(service, bucket, prefix, input_format=None):
    """Find the first non-empty file in a directory that matches the input format."""
    if service == 'gcs':
        files = list_gcs_directory(bucket, prefix)
    elif service == 's3':
        files = list_s3_directory(bucket, prefix)
    else:
        raise ValueError(f"Unsupported service: {service}")
    
    if not files:
        raise ValueError(f"No files found in {service}://{bucket}/{prefix}")
    
    # Filter files by size > 0
    non_empty_files = [f for f in files if f[1] > 0]
    
    if not non_empty_files:
        raise ValueError(f"No non-empty files found in {service}://{bucket}/{prefix}")
    
    # Sort by name to ensure deterministic behavior
    non_empty_files.sort(key=lambda x: x[0])
    
    # Filter by input format if specified
    if input_format:
        format_ext_map = {
            'csv': r'\.csv$',
            'json': r'\.json$',
            'parquet': r'\.parquet$'
        }
        
        format_regex = format_ext_map.get(input_format, None)
        if format_regex:
            matching_files = [f for f in non_empty_files if re.search(format_regex, f[0], re.IGNORECASE)]
            if matching_files:
                # Use the first matching file
                selected_file = matching_files[0]
                click.echo(Fore.BLUE + f"Selected file: {selected_file[0]} ({selected_file[1]} bytes)" + Style.RESET_ALL)
                return selected_file[0]
    
    # If no input_format specified or no matching files found, use the first non-empty file
    # Skip common metadata files
    skip_patterns = [r'_SUCCESS$', r'\.crc$', r'\.committed$', r'\.pending$', r'_metadata$']
    
    for file_name, file_size in non_empty_files:
        # Skip if the file matches any of the patterns to ignore
        if not any(re.search(pattern, file_name) for pattern in skip_patterns):
            click.echo(Fore.BLUE + f"Selected file: {file_name} ({file_size} bytes)" + Style.RESET_ALL)
            return file_name
    
    # If all files are skipped, use the first non-empty file anyway
    selected_file = non_empty_files[0]
    click.echo(Fore.YELLOW + f"Only found metadata files, using: {selected_file[0]} ({selected_file[1]} bytes)" + Style.RESET_ALL)
    return selected_file[0]


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
    # First read the data without column filtering to get full schema
    if num_rows > 0:
        full_df = pd.read_csv(stream, nrows=num_rows)
    else:
        full_df = pd.read_csv(stream)
    
    # Store the full schema for later use
    full_schema = full_df.dtypes
    
    # Apply column filtering if specified
    if columns:
        cols = [c.strip() for c in columns.split(',')]
        valid_cols = [c for c in cols if c in full_df.columns]
        if len(valid_cols) != len(cols):
            missing = set(cols) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        df = full_df[valid_cols]
    else:
        df = full_df
    
    # Return both the filtered dataframe and the full schema
    return df, full_schema


def read_json_data(stream, num_rows, columns=None):
    """Read JSON data from a stream."""
    # First read the data without column filtering to get full schema
    if num_rows > 0:
        full_df = pd.read_json(stream, lines=True, nrows=num_rows)
    else:
        full_df = pd.read_json(stream, lines=True)
    
    # Store the full schema for later use
    full_schema = full_df.dtypes
    
    # Apply column filtering if specified
    if columns:
        cols = [c.strip() for c in columns.split(',')]
        valid_cols = [c for c in cols if c in full_df.columns]
        if len(valid_cols) != len(cols):
            missing = set(cols) - set(valid_cols)
            click.echo(Fore.YELLOW + f"Warning: Columns not found: {', '.join(missing)}" + Style.RESET_ALL)
        df = full_df[valid_cols]
    else:
        df = full_df
    
    # Return both the filtered dataframe and the full schema
    return df, full_schema


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
        
        # Read the full schema first
        full_schema = parquet_file.schema_arrow
        
        # Extract columns if specified for filtering
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
                df = result_table.to_pandas()
            else:
                df = pd.DataFrame()
        else:
            # Read all data (potentially with column filtering)
            table = parquet_file.read(columns=col_names)
            df = table.to_pandas()
        
        # Get the full schema as a pandas Series for consistency with other formats
        # First, read a small amount of data to get pandas dtypes
        full_df = pq.read_table(temp_path, nrows=1).to_pandas()
        full_schema = full_df.dtypes
        
        return df, full_schema
    
    finally:
        # Clean up the temporary file
        import os
        try:
            if hasattr(stream, 'read'):  # Only delete if we created temp file
                os.unlink(temp_path)
        except:
            pass


def read_data_from_multiple_files(service, bucket, file_list, input_format, num_rows, columns=None):
    """Read data from multiple files and concatenate the results."""
    dfs = []
    schemas = []
    rows_read = 0
    total_rows = 0
    
    # Define a function to process each file
    def process_file(file_info):
        file_name, file_size = file_info
        click.echo(Fore.BLUE + f"Reading file: {file_name} ({file_size/1024:.1f} KB)" + Style.RESET_ALL)
        
        if service == 'gcs':
            stream = get_gcs_stream(bucket, file_name)
        elif service == 's3':
            stream = get_s3_stream(bucket, file_name)
        else:
            raise ValueError(f"Unsupported service: {service}")
        
        # Calculate remaining rows to read if num_rows is specified
        remaining_rows = max(0, num_rows - rows_read) if num_rows > 0 else 0
        
        # Read the file
        if input_format == 'csv':
            df, schema = read_csv_data(stream, remaining_rows if remaining_rows > 0 else 0, columns)
        elif input_format == 'json':
            df, schema = read_json_data(stream, remaining_rows if remaining_rows > 0 else 0, columns)
        elif input_format == 'parquet':
            df, schema = read_parquet_data(stream, remaining_rows if remaining_rows > 0 else 0, columns)
        else:
            raise ValueError(f"Unsupported format: {input_format}")
        
        return df, schema, len(df)
    
    # Process files in order until we have enough rows
    for file_info in file_list:
        try:
            df, schema, file_rows = process_file(file_info)
            
            if not df.empty:
                dfs.append(df)
                schemas.append(schema)
                rows_read += file_rows
                total_rows += file_rows
                
                # Stop if we've read enough rows
                if num_rows > 0 and rows_read >= num_rows:
                    break
        except Exception as e:
            click.echo(Fore.YELLOW + f"Warning: Error reading file {file_info[0]}: {str(e)}" + Style.RESET_ALL)
    
    if not dfs:
        raise ValueError("No data could be read from any of the files")
    
    # Concatenate the dataframes
    result_df = pd.concat(dfs, ignore_index=True)
    
    # For the full schema, merge all schemas
    all_columns = {}
    for schema in schemas:
        for col, dtype in schema.items():
            if col in all_columns:
                # If the same column has different types, use object type
                if all_columns[col] != dtype:
                    all_columns[col] = 'object'
            else:
                all_columns[col] = dtype
    
    full_schema = pd.Series(all_columns)
    
    # If we read more rows than requested, truncate the result
    if num_rows > 0 and len(result_df) > num_rows:
        result_df = result_df.iloc[:num_rows]
    
    return result_df, full_schema, total_rows


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


def colorize_json(json_str):
    """Add colors to JSON for better readability."""
    # Parse the JSON
    parsed = json.loads(json_str)
    
    # Convert to a colored string
    result = []
    
    for item in parsed:
        item_parts = []
        item_parts.append('{')
        
        for i, (key, value) in enumerate(item.items()):
            # Format key
            key_str = f'  {Fore.BLUE}"{key}"{Style.RESET_ALL}: '
            
            # Format value based on type
            if isinstance(value, str):
                val_str = f'{Fore.GREEN}"{value}"{Style.RESET_ALL}'
            elif isinstance(value, (int, float)):
                val_str = f'{Fore.CYAN}{value}{Style.RESET_ALL}'
            elif value is None:
                val_str = f'{Fore.RED}null{Style.RESET_ALL}'
            elif isinstance(value, bool):
                val_str = f'{Fore.YELLOW}{str(value).lower()}{Style.RESET_ALL}'
            else:
                # For complex types, just convert to string
                val_str = f'{json.dumps(value)}'
            
            # Add comma if not the last item
            if i < len(item) - 1:
                item_parts.append(f"{key_str}{val_str},")
            else:
                item_parts.append(f"{key_str}{val_str}")
        
        item_parts.append('}')
        result.append('\n'.join(item_parts))
    
    return '\n'.join(result)


def format_table_with_colored_header(df):
    """Format a dataframe as a table with colored and bold headers."""
    if df.empty:
        return "Empty dataset"
    
    # Get the column headers and format them
    headers = [f"{Fore.CYAN}{Style.BRIGHT}{col}{Style.RESET_ALL}" for col in df.columns]
    
    # Convert the dataframe to a list of lists for tabulate
    data = df.values.tolist()
    
    # Use tabulate with the formatted headers
    return tabulate(data, headers, tablefmt='psql')


@click.command()
@click.option('--path', '-p', required=True, help='Path to the file or directory (gcs://... or s3://...)')
@click.option('--output-format', '-o', type=click.Choice(['json', 'jsonp', 'csv', 'table']), default='table', 
              help='Output format (default: table)')
@click.option('--input-format', '-i', type=click.Choice(['json', 'csv', 'parquet']), 
              help='Input format (default: inferred from path)')
@click.option('--columns', '-c', help='Comma-separated list of columns to display (default: all)')
@click.option('--num-rows', '-n', default=10, type=int, help='Number of rows to display (default: 10)')
@click.option('--schema', '-s', type=click.Choice(['show', 'dont_show', 'schema_only']), default='show',
              help='Schema display option (default: show)')
@click.option('--no-count', is_flag=True, help='Disable record count display')
@click.option('--multi-file-mode', '-m', type=click.Choice(['first', 'auto', 'all']), default='auto',
              help='How to handle directories with multiple files (default: auto)')
@click.option('--max-size-mb', default=25, type=int, 
              help='Maximum size in MB to read when reading multiple files (default: 25)')
def main(path, output_format, input_format, columns, num_rows, schema, no_count, multi_file_mode, max_size_mb):
    """Display data from files in Google Cloud Storage or AWS S3.
    
    Example usage:
    
    \b
    # Read from GCS
    cloudcat --path gcs://my-bucket/data.csv --output-format table
    
    \b
    # Read from S3 with column selection
    cloudcat --path s3://my-bucket/data.parquet --columns id,name,value
    
    \b 
    # Show 20 rows
    cloudcat --path gcs://bucket/events.json --num-rows 20
    
    \b
    # Output JSON with pretty formatting and colors
    cloudcat --path s3://bucket/data.json --output-format jsonp
    
    \b
    # Read from a directory (reads first non-empty data file)
    cloudcat --path gcs://my-bucket/sparkoutput/ --input-format parquet
    
    \b
    # Read from multiple files in a directory (up to 25MB)
    cloudcat --path s3://my-bucket/daily-data/ --multi-file-mode all --max-size-mb 25
    """
    try:
        # Parse the path
        service, bucket, object_path = parse_cloud_path(path)
        
        # Check if path is a directory (ends with '/')
        is_directory = object_path.endswith('/')
        
        # Handle directory paths based on multi-file-mode
        if is_directory:
            click.echo(Fore.BLUE + f"Path is a directory" + Style.RESET_ALL)
            
            if multi_file_mode == 'first' or (multi_file_mode == 'auto' and max_size_mb <= 0):
                # Use a single file
                click.echo(Fore.BLUE + f"Looking for first suitable file..." + Style.RESET_ALL)
                object_path = find_first_non_empty_file(service, bucket, object_path, input_format)
                
                # Determine input format if not specified
                if not input_format:
                    input_format = detect_format_from_path(object_path)
                    click.echo(Fore.BLUE + f"Inferred input format: {input_format}" + Style.RESET_ALL)
                
                # Read the data from the single file
                df, full_schema = read_data(service, bucket, object_path, input_format, num_rows, columns)
                total_record_count = None  # Will be computed later if needed
            else:
                # Read from multiple files
                click.echo(Fore.BLUE + f"Reading multiple files (up to {max_size_mb}MB)..." + Style.RESET_ALL)
                
                # Determine input format if not specified (use the first file to infer)
                if not input_format:
                    first_file = find_first_non_empty_file(service, bucket, object_path)
                    input_format = detect_format_from_path(first_file)
                    click.echo(Fore.BLUE + f"Inferred input format from first file: {input_format}" + Style.RESET_ALL)
                
                # Get files to read
                file_list = get_files_for_multiread(service, bucket, object_path, input_format, max_size_mb)
                
                # Read data from multiple files
                df, full_schema, total_record_count = read_data_from_multiple_files(
                    service, bucket, file_list, input_format, num_rows, columns
                )
                
                # Update object_path for display/logging purposes
                object_path = f"{object_path} ({len(file_list)} files)"
        else:
            # Single file path
            # Determine input format if not specified
            if not input_format:
                input_format = detect_format_from_path(object_path)
                click.echo(Fore.BLUE + f"Inferred input format: {input_format}" + Style.RESET_ALL)
            
            # Read the data
            df, full_schema = read_data(service, bucket, object_path, input_format, num_rows, columns)
            total_record_count = None  # Will be computed later if needed
        
        # Display schema if requested
        if schema in ['show', 'schema_only']:
            click.echo(Fore.GREEN + "Schema:" + Style.RESET_ALL)
            for col, dtype in full_schema.items():
                click.echo(f"  {col}: {dtype}")
            click.echo("")
        
        # Exit if only schema was requested
        if schema == 'schema_only':
            # Still show count even with schema_only unless --no-count is specified
            if not no_count:
                try:
                    if total_record_count is None:
                        total_record_count = get_record_count(service, bucket, object_path, input_format)
                    click.echo(Fore.CYAN + f"Total records: {total_record_count}" + Style.RESET_ALL)
                except Exception as e:
                    click.echo(Fore.YELLOW + f"Could not count records: {str(e)}" + Style.RESET_ALL)
            return
        
        # Display the data
        if output_format == 'table':
            # Use our custom function for formatted table output
            click.echo(format_table_with_colored_header(df))
        elif output_format == 'jsonp':
            # Pretty print JSON with colors
            json_str = df.to_json(orient='records')
            pretty_json = json.dumps(json.loads(json_str), indent=2)
            # Apply colors for better readability
            click.echo(colorize_json(json_str))
        elif output_format == 'json':
            click.echo(df.to_json(orient='records', lines=True))
        elif output_format == 'csv':
            click.echo(df.to_csv(index=False))
        
        # Count records by default unless --no-count is specified
        if not no_count:
            try:
                if total_record_count is None:
                    total_record_count = get_record_count(service, bucket, object_path, input_format)
                click.echo(Fore.CYAN + f"\nTotal records: {total_record_count}" + Style.RESET_ALL)
            except Exception as e:
                click.echo(Fore.YELLOW + f"\nCould not count records: {str(e)}" + Style.RESET_ALL)
    
    except Exception as e:
        click.echo(Fore.RED + f"Error: {str(e)}" + Style.RESET_ALL, err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()