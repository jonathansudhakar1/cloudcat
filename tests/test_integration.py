import pytest
import pandas as pd
import io
from unittest.mock import patch, MagicMock
from cloudcat.cli import read_data_from_multiple_files


class TestIntegration:
    @patch('cloudcat.cli.get_gcs_stream')
    @patch('cloudcat.cli.read_csv_data')
    def test_read_data_from_multiple_csv_files(self, mock_read_csv, mock_stream):
        # Mock file streams
        mock_stream.side_effect = [io.StringIO(), io.StringIO()]
        
        # Mock CSV data reading
        df1 = pd.DataFrame({"name": ["John"], "age": [25]})
        df2 = pd.DataFrame({"name": ["Jane"], "age": [30]})
        schema = pd.Series({"name": "object", "age": "int64"})
        
        mock_read_csv.side_effect = [(df1, schema), (df2, schema)]
        
        file_list = [("file1.csv", 1024), ("file2.csv", 2048)]
        result_df, full_schema, total_rows = read_data_from_multiple_files(
            "gcs", "bucket", file_list, "csv", 0, None
        )
        
        assert len(result_df) == 2
        assert result_df.iloc[0]["name"] == "John"
        assert result_df.iloc[1]["name"] == "Jane"
        assert total_rows == 2

    @patch('cloudcat.cli.get_s3_stream')
    @patch('cloudcat.cli.read_json_data')
    def test_read_data_from_multiple_json_files_with_limit(self, mock_read_json, mock_stream):
        # Mock file streams
        mock_stream.side_effect = [io.StringIO(), io.StringIO()]
        
        # Mock JSON data reading
        df1 = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        df2 = pd.DataFrame({"id": [3, 4], "value": ["c", "d"]})
        schema = pd.Series({"id": "int64", "value": "object"})
        
        mock_read_json.side_effect = [(df1, schema), (df2, schema)]
        
        file_list = [("file1.json", 512), ("file2.json", 1024)]
        result_df, full_schema, total_rows = read_data_from_multiple_files(
            "s3", "bucket", file_list, "json", 3, None  # Limit to 3 rows
        )
        
        assert len(result_df) == 3  # Should be truncated
        assert total_rows == 4  # But total_rows tracks actual read
        assert result_df.iloc[0]["id"] == 1
        assert result_df.iloc[2]["id"] == 3

    @patch('cloudcat.cli.get_gcs_stream')
    @patch('cloudcat.cli.read_csv_data')
    def test_read_data_with_schema_mismatch(self, mock_read_csv, mock_stream):
        # Mock file streams
        mock_stream.side_effect = [io.StringIO(), io.StringIO()]
        
        # Mock CSV data with different schemas
        df1 = pd.DataFrame({"name": ["John"], "age": [25]})
        df2 = pd.DataFrame({"name": ["Jane"], "score": [95.5]})  # Different column
        
        schema1 = pd.Series({"name": "object", "age": "int64"})
        schema2 = pd.Series({"name": "object", "score": "float64"})
        
        mock_read_csv.side_effect = [(df1, schema1), (df2, schema2)]
        
        file_list = [("file1.csv", 1024), ("file2.csv", 2048)]
        result_df, full_schema, total_rows = read_data_from_multiple_files(
            "gcs", "bucket", file_list, "csv", 0, None
        )
        
        # Should handle mismatched schemas gracefully
        assert len(result_df) == 2
        assert "name" in result_df.columns
        # Both age and score should be in the final schema with object type
        expected_cols = {"name", "age", "score"}
        assert set(full_schema.index) == expected_cols

    @patch('cloudcat.cli.get_gcs_stream')
    @patch('cloudcat.cli.read_csv_data')
    def test_read_data_with_file_errors(self, mock_read_csv, mock_stream):
        # Mock file streams
        mock_stream.side_effect = [io.StringIO(), io.StringIO()]
        
        # First file succeeds, second file fails
        df1 = pd.DataFrame({"name": ["John"], "age": [25]})
        schema1 = pd.Series({"name": "object", "age": "int64"})
        
        mock_read_csv.side_effect = [
            (df1, schema1),
            Exception("File corrupted")
        ]
        
        file_list = [("file1.csv", 1024), ("file2.csv", 2048)]
        result_df, full_schema, total_rows = read_data_from_multiple_files(
            "gcs", "bucket", file_list, "csv", 0, None
        )
        
        # Should continue with successful files
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "John"
        assert total_rows == 1

    @patch('cloudcat.cli.get_s3_stream')
    @patch('cloudcat.cli.read_json_data')
    def test_read_data_all_files_fail(self, mock_read_json, mock_stream):
        # Mock file streams
        mock_stream.side_effect = [io.StringIO(), io.StringIO()]
        
        # Both files fail
        mock_read_json.side_effect = [
            Exception("File 1 corrupted"),
            Exception("File 2 corrupted")
        ]
        
        file_list = [("file1.json", 512), ("file2.json", 1024)]
        
        with pytest.raises(ValueError, match="No data could be read from any of the files"):
            read_data_from_multiple_files(
                "s3", "bucket", file_list, "json", 0, None
            )

    @patch('cloudcat.cli.get_gcs_stream')
    @patch('cloudcat.cli.read_csv_data')
    def test_read_data_with_column_filtering(self, mock_read_csv, mock_stream):
        # Mock file streams
        mock_stream.side_effect = [io.StringIO(), io.StringIO()]
        
        # Mock CSV data reading with column filtering
        df1 = pd.DataFrame({"name": ["John"], "city": ["NYC"]})
        df2 = pd.DataFrame({"name": ["Jane"], "city": ["LA"]})
        schema = pd.Series({"name": "object", "city": "object"})
        
        mock_read_csv.side_effect = [(df1, schema), (df2, schema)]
        
        file_list = [("file1.csv", 1024), ("file2.csv", 2048)]
        result_df, full_schema, total_rows = read_data_from_multiple_files(
            "gcs", "bucket", file_list, "csv", 0, "name,city"
        )
        
        assert len(result_df) == 2
        assert list(result_df.columns) == ["name", "city"]
        assert result_df.iloc[0]["name"] == "John"
        assert result_df.iloc[1]["name"] == "Jane"

    def test_unsupported_service_in_multifile_read(self):
        file_list = [("file1.csv", 1024)]
        
        with pytest.raises(ValueError, match="Unsupported service: azure"):
            read_data_from_multiple_files(
                "azure", "bucket", file_list, "csv", 0, None
            )