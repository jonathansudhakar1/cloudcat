import pytest
import pandas as pd
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from cloudcat.cli import main
from cloudcat.streaming import StreamingStats


class TestCLI:
    def setup_method(self):
        self.runner = CliRunner()

    @patch('cloudcat.cli.read_data_streaming')
    @patch('cloudcat.cli.parse_cloud_path')
    @patch('cloudcat.cli.detect_format_from_path')
    def test_basic_file_read(self, mock_detect, mock_parse, mock_read):
        mock_parse.return_value = ("gcs", "bucket", "file.csv")
        mock_detect.return_value = "csv"
        mock_df = pd.DataFrame({"name": ["John"], "age": [25]})
        mock_read.return_value = (mock_df, mock_df.dtypes, StreamingStats())

        result = self.runner.invoke(main, ['--path', 'gcs://bucket/file.csv'])

        assert result.exit_code == 0
        assert "John" in result.output

    @patch('cloudcat.cli.read_data_streaming')
    @patch('cloudcat.cli.parse_cloud_path')
    @patch('cloudcat.cli.detect_format_from_path')
    def test_json_output_format(self, mock_detect, mock_parse, mock_read):
        mock_parse.return_value = ("s3", "bucket", "file.json")
        mock_detect.return_value = "json"
        mock_df = pd.DataFrame({"name": ["John"], "age": [25]})
        mock_read.return_value = (mock_df, mock_df.dtypes, StreamingStats())

        result = self.runner.invoke(main, [
            '--path', 's3://bucket/file.json',
            '--output-format', 'json'
        ])

        assert result.exit_code == 0
        assert '{"name":"John","age":25}' in result.output

    @patch('cloudcat.cli.read_data_streaming')
    @patch('cloudcat.cli.parse_cloud_path')
    @patch('cloudcat.cli.detect_format_from_path')
    def test_csv_output_format(self, mock_detect, mock_parse, mock_read):
        mock_parse.return_value = ("gcs", "bucket", "file.parquet")
        mock_detect.return_value = "parquet"
        mock_df = pd.DataFrame({"name": ["John"], "age": [25]})
        mock_read.return_value = (mock_df, mock_df.dtypes, StreamingStats())

        result = self.runner.invoke(main, [
            '--path', 'gcs://bucket/file.parquet',
            '--output-format', 'csv'
        ])

        assert result.exit_code == 0
        assert "name,age" in result.output
        assert "John,25" in result.output

    @patch('cloudcat.cli.read_data_streaming')
    @patch('cloudcat.cli.parse_cloud_path')
    def test_schema_only_option(self, mock_parse, mock_read):
        mock_parse.return_value = ("gcs", "bucket", "file.csv")
        mock_df = pd.DataFrame({"name": ["John"], "age": [25]})
        mock_read.return_value = (mock_df, mock_df.dtypes, StreamingStats())

        result = self.runner.invoke(main, [
            '--path', 'gcs://bucket/file.csv',
            '--input-format', 'csv',
            '--schema', 'schema_only'
        ])

        assert result.exit_code == 0
        assert "Schema:" in result.output
        assert "name:" in result.output
        assert "age:" in result.output
        assert "John" not in result.output

    @patch('cloudcat.cli.read_data_streaming')
    @patch('cloudcat.cli.parse_cloud_path')
    def test_num_rows_option(self, mock_parse, mock_read):
        mock_parse.return_value = ("s3", "bucket", "file.json")
        mock_df = pd.DataFrame({"name": ["John", "Jane", "Bob"], "age": [25, 30, 35]})
        mock_read.return_value = (mock_df, mock_df.dtypes, StreamingStats())

        result = self.runner.invoke(main, [
            '--path', 's3://bucket/file.json',
            '--input-format', 'json',
            '--num-rows', '2'
        ])

        assert result.exit_code == 0
        mock_read.assert_called_with("s3", "bucket", "file.json", "json", 2, None, None, 0)

    @patch('cloudcat.cli.read_data_streaming')
    @patch('cloudcat.cli.parse_cloud_path')
    def test_columns_option(self, mock_parse, mock_read):
        mock_parse.return_value = ("gcs", "bucket", "file.csv")
        mock_df = pd.DataFrame({"name": ["John"], "age": [25]})
        mock_read.return_value = (mock_df, mock_df.dtypes, StreamingStats())

        result = self.runner.invoke(main, [
            '--path', 'gcs://bucket/file.csv',
            '--input-format', 'csv',
            '--columns', 'name,age'
        ])

        assert result.exit_code == 0
        mock_read.assert_called_with("gcs", "bucket", "file.csv", "csv", 10, "name,age", None, 0)

    @patch('cloudcat.cli.read_data_streaming')
    @patch('cloudcat.cli.parse_cloud_path')
    def test_delimiter_option(self, mock_parse, mock_read):
        mock_parse.return_value = ("gcs", "bucket", "file.csv")
        mock_df = pd.DataFrame({"name": ["John"], "age": [25]})
        mock_read.return_value = (mock_df, mock_df.dtypes, StreamingStats())

        result = self.runner.invoke(main, [
            '--path', 'gcs://bucket/file.csv',
            '--input-format', 'csv',
            '--delimiter', '\\t'
        ])

        assert result.exit_code == 0
        mock_read.assert_called_with("gcs", "bucket", "file.csv", "csv", 10, None, "\t", 0)

    @patch('cloudcat.cli.read_data_streaming')
    @patch('cloudcat.cli.parse_cloud_path')
    def test_offset_option(self, mock_parse, mock_read):
        mock_parse.return_value = ("s3", "bucket", "file.csv")
        mock_df = pd.DataFrame({"name": ["John"], "age": [25]})
        mock_read.return_value = (mock_df, mock_df.dtypes, StreamingStats())

        result = self.runner.invoke(main, [
            '--path', 's3://bucket/file.csv',
            '--input-format', 'csv',
            '--offset', '5'
        ])

        assert result.exit_code == 0
        mock_read.assert_called_with("s3", "bucket", "file.csv", "csv", 10, None, None, 5)

    @patch('cloudcat.cli.read_data_streaming')
    @patch('cloudcat.cli.parse_cloud_path')
    def test_offset_with_num_rows(self, mock_parse, mock_read):
        mock_parse.return_value = ("gcs", "bucket", "file.json")
        mock_df = pd.DataFrame({"name": ["John"], "age": [25]})
        mock_read.return_value = (mock_df, mock_df.dtypes, StreamingStats())

        result = self.runner.invoke(main, [
            '--path', 'gcs://bucket/file.json',
            '--input-format', 'json',
            '--num-rows', '20',
            '--offset', '10'
        ])

        assert result.exit_code == 0
        mock_read.assert_called_with("gcs", "bucket", "file.json", "json", 20, None, None, 10)

    @patch('cloudcat.cli.find_first_non_empty_file')
    @patch('cloudcat.cli.read_data_streaming')
    @patch('cloudcat.cli.parse_cloud_path')
    def test_directory_path_first_mode(self, mock_parse, mock_read, mock_find):
        mock_parse.return_value = ("gcs", "bucket", "folder/")
        mock_find.return_value = "folder/data.csv"
        mock_df = pd.DataFrame({"name": ["John"], "age": [25]})
        mock_read.return_value = (mock_df, mock_df.dtypes, StreamingStats())

        result = self.runner.invoke(main, [
            '--path', 'gcs://bucket/folder/',
            '--multi-file-mode', 'first'
        ])

        assert result.exit_code == 0
        mock_find.assert_called_once()

    @patch('cloudcat.cli.get_files_for_multiread')
    @patch('cloudcat.cli.read_data_from_multiple_files')
    @patch('cloudcat.cli.parse_cloud_path')
    @patch('cloudcat.cli.find_first_non_empty_file')
    def test_directory_path_all_mode(self, mock_find, mock_parse, mock_read_multi, mock_get_files):
        mock_parse.return_value = ("s3", "bucket", "folder/")
        mock_find.return_value = "folder/data.csv"
        mock_get_files.return_value = [("file1.csv", 1024), ("file2.csv", 2048)]
        mock_df = pd.DataFrame({"name": ["John"], "age": [25]})
        mock_read_multi.return_value = (mock_df, mock_df.dtypes, 100)

        result = self.runner.invoke(main, [
            '--path', 's3://bucket/folder/',
            '--multi-file-mode', 'all',
            '--max-size-mb', '5'
        ])

        assert result.exit_code == 0
        mock_get_files.assert_called_once()
        mock_read_multi.assert_called_once()

    @patch('cloudcat.cli.get_record_count')
    @patch('cloudcat.cli.read_data_streaming')
    @patch('cloudcat.cli.parse_cloud_path')
    def test_count_option(self, mock_parse, mock_read, mock_count):
        """Test that --count flag enables record counting."""
        mock_parse.return_value = ("gcs", "bucket", "file.csv")
        mock_df = pd.DataFrame({"name": ["John"], "age": [25]})
        mock_read.return_value = (mock_df, mock_df.dtypes, StreamingStats())
        mock_count.return_value = 1000

        result = self.runner.invoke(main, [
            '--path', 'gcs://bucket/file.csv',
            '--input-format', 'csv',
            '--count'
        ])

        assert result.exit_code == 0
        mock_count.assert_called_once()
        assert "Total records:" in result.output

    @patch('cloudcat.cli.get_record_count')
    @patch('cloudcat.cli.read_data_streaming')
    @patch('cloudcat.cli.parse_cloud_path')
    def test_default_no_count(self, mock_parse, mock_read, mock_count):
        """Test that counting is disabled by default."""
        mock_parse.return_value = ("gcs", "bucket", "file.csv")
        mock_df = pd.DataFrame({"name": ["John"], "age": [25]})
        mock_read.return_value = (mock_df, mock_df.dtypes, StreamingStats())
        mock_count.return_value = 1000

        result = self.runner.invoke(main, [
            '--path', 'gcs://bucket/file.csv',
            '--input-format', 'csv'
        ])

        assert result.exit_code == 0
        mock_count.assert_not_called()
        assert "Total records:" not in result.output

    def test_missing_path_parameter(self):
        result = self.runner.invoke(main, [])

        assert result.exit_code != 0
        assert "Missing option '--path'" in result.output

    @patch('cloudcat.cli.parse_cloud_path')
    def test_invalid_cloud_path(self, mock_parse):
        mock_parse.side_effect = ValueError("Unsupported scheme: http")

        result = self.runner.invoke(main, ['--path', 'http://bucket/file.csv'])

        assert result.exit_code == 1
        assert "Error: Unsupported scheme: http" in result.output

    @patch('cloudcat.cli.read_data_streaming')
    @patch('cloudcat.cli.parse_cloud_path')
    def test_schema_dont_show_option(self, mock_parse, mock_read):
        mock_parse.return_value = ("gcs", "bucket", "file.csv")
        mock_df = pd.DataFrame({"name": ["John"], "age": [25]})
        mock_read.return_value = (mock_df, mock_df.dtypes, StreamingStats())

        result = self.runner.invoke(main, [
            '--path', 'gcs://bucket/file.csv',
            '--input-format', 'csv',
            '--schema', 'dont_show'
        ])

        assert result.exit_code == 0
        assert "Schema:" not in result.output
        assert "John" in result.output
