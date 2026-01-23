import pytest
import pandas as pd
import io
from unittest.mock import patch, MagicMock
from cloudcat.cli import read_csv_data, read_json_data, get_record_count


class TestDataReading:
    def test_read_csv_data_basic(self):
        csv_data = "name,age,city\nJohn,25,NYC\nJane,30,LA\nBob,35,SF"
        stream = io.StringIO(csv_data)
        
        df, schema = read_csv_data(stream, 2)
        
        assert len(df) == 2
        assert list(df.columns) == ["name", "age", "city"]
        assert df.iloc[0]["name"] == "John"
        assert df.iloc[1]["name"] == "Jane"

    def test_read_csv_data_with_columns(self):
        csv_data = "name,age,city\nJohn,25,NYC\nJane,30,LA"
        stream = io.StringIO(csv_data)
        
        df, schema = read_csv_data(stream, 0, columns="name,city")
        
        assert list(df.columns) == ["name", "city"]
        assert "age" not in df.columns
        assert len(df) == 2

    def test_read_csv_data_with_delimiter(self):
        csv_data = "name\tage\tcity\nJohn\t25\tNYC\nJane\t30\tLA"
        stream = io.StringIO(csv_data)
        
        df, schema = read_csv_data(stream, 0, delimiter="\t")
        
        assert list(df.columns) == ["name", "age", "city"]
        assert len(df) == 2

    def test_read_csv_data_invalid_columns_warning(self):
        csv_data = "name,age,city\nJohn,25,NYC\nJane,30,LA"
        stream = io.StringIO(csv_data)
        
        df, schema = read_csv_data(stream, 0, columns="name,invalid_col")
        
        assert list(df.columns) == ["name"]
        assert "invalid_col" not in df.columns

    def test_read_json_data_basic(self):
        json_data = '{"name": "John", "age": 25}\n{"name": "Jane", "age": 30}'
        stream = io.StringIO(json_data)
        
        df, schema = read_json_data(stream, 1)
        
        assert len(df) == 1
        assert list(df.columns) == ["name", "age"]
        assert df.iloc[0]["name"] == "John"

    def test_read_json_data_with_columns(self):
        json_data = '{"name": "John", "age": 25, "city": "NYC"}\n{"name": "Jane", "age": 30, "city": "LA"}'
        stream = io.StringIO(json_data)
        
        df, schema = read_json_data(stream, 0, columns="name,city")
        
        assert list(df.columns) == ["name", "city"]
        assert "age" not in df.columns

    def test_read_json_data_invalid_columns_warning(self):
        json_data = '{"name": "John", "age": 25}\n{"name": "Jane", "age": 30}'
        stream = io.StringIO(json_data)
        
        df, schema = read_json_data(stream, 0, columns="name,invalid_col")
        
        assert list(df.columns) == ["name"]
        assert "invalid_col" not in df.columns

    @patch('cloudcat.cli.HAS_PARQUET', True)
    @patch('cloudcat.cli.pq')
    @patch('cloudcat.cli.get_stream')
    def test_get_record_count_parquet(self, mock_get_stream, mock_pq):
        mock_file = MagicMock()
        mock_file.metadata.num_rows = 1000
        mock_pq.ParquetFile.return_value = mock_file
        mock_get_stream.return_value.read.return_value = b"parquet_data"

        count = get_record_count("gcs", "bucket", "file.parquet", "parquet")

        assert count == 1000

    @patch('cloudcat.cli.get_stream')
    @patch('cloudcat.cli.pd.read_csv')
    def test_get_record_count_csv(self, mock_read_csv, mock_get_stream):
        # Mock chunked reading
        chunk1 = pd.DataFrame({"col1": [1, 2, 3]})
        chunk2 = pd.DataFrame({"col1": [4, 5]})
        mock_read_csv.return_value = [chunk1, chunk2]

        count = get_record_count("gcs", "bucket", "file.csv", "csv")

        assert count == 5

    @patch('cloudcat.cli.get_stream')
    def test_get_record_count_json(self, mock_get_stream):
        # Mock JSON Lines content (6 records)
        json_lines = """{"col1": 1}
{"col1": 2}
{"col1": 3}
{"col1": 4}
{"col1": 5}
{"col1": 6}"""
        mock_stream = io.BytesIO(json_lines.encode('utf-8'))
        mock_get_stream.return_value = mock_stream

        count = get_record_count("s3", "bucket", "file.json", "json")

        assert count == 6

    def test_read_csv_all_rows(self):
        csv_data = "name,age\nJohn,25\nJane,30\nBob,35"
        stream = io.StringIO(csv_data)
        
        df, schema = read_csv_data(stream, 0)  # 0 means read all rows
        
        assert len(df) == 3
        assert list(df.columns) == ["name", "age"]

    def test_read_json_all_rows(self):
        json_data = '{"name": "John"}\n{"name": "Jane"}\n{"name": "Bob"}'
        stream = io.StringIO(json_data)
        
        df, schema = read_json_data(stream, 0)  # 0 means read all rows
        
        assert len(df) == 3
        assert list(df.columns) == ["name"]