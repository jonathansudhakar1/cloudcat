"""Tests for format detection."""

import pytest
from cloudcat.cli import detect_format_from_path


class TestFormatDetection:
    def test_csv_format_detection(self):
        assert detect_format_from_path("data.csv") == "csv"
        assert detect_format_from_path("path/to/data.CSV") == "csv"
        assert detect_format_from_path("folder/file.csv") == "csv"

    def test_json_format_detection(self):
        assert detect_format_from_path("data.json") == "json"
        assert detect_format_from_path("path/to/data.JSON") == "json"
        assert detect_format_from_path("events.json") == "json"

    def test_jsonl_format_detection(self):
        assert detect_format_from_path("data.jsonl") == "json"
        assert detect_format_from_path("events.ndjson") == "json"

    def test_parquet_format_detection(self):
        assert detect_format_from_path("data.parquet") == "parquet"
        assert detect_format_from_path("path/to/data.PARQUET") == "parquet"
        assert detect_format_from_path("table.parquet") == "parquet"

    def test_avro_format_detection(self):
        assert detect_format_from_path("data.avro") == "avro"
        assert detect_format_from_path("path/to/data.AVRO") == "avro"

    def test_orc_format_detection(self):
        assert detect_format_from_path("data.orc") == "orc"
        assert detect_format_from_path("path/to/data.ORC") == "orc"

    def test_text_format_detection(self):
        assert detect_format_from_path("data.txt") == "text"
        assert detect_format_from_path("app.log") == "text"
        assert detect_format_from_path("path/to/file.LOG") == "text"

    def test_compressed_csv_format_detection(self):
        assert detect_format_from_path("data.csv.gz") == "csv"
        assert detect_format_from_path("data.csv.zst") == "csv"
        assert detect_format_from_path("data.csv.lz4") == "csv"
        assert detect_format_from_path("data.csv.bz2") == "csv"
        assert detect_format_from_path("data.csv.snappy") == "csv"

    def test_compressed_json_format_detection(self):
        assert detect_format_from_path("data.json.gz") == "json"
        assert detect_format_from_path("data.jsonl.zst") == "json"

    def test_compressed_parquet_format_detection(self):
        assert detect_format_from_path("data.parquet.gz") == "parquet"
        assert detect_format_from_path("data.parquet.zst") == "parquet"

    def test_unsupported_format_raises_error(self):
        with pytest.raises(ValueError, match="Could not infer format from path"):
            detect_format_from_path("data.xlsx")

        with pytest.raises(ValueError, match="Could not infer format from path"):
            detect_format_from_path("no_extension")

    def test_path_with_multiple_dots(self):
        assert detect_format_from_path("data.backup.csv") == "csv"
        assert detect_format_from_path("file.v1.2.json") == "json"
        assert detect_format_from_path("table.final.parquet") == "parquet"
