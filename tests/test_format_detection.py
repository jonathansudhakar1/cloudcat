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

    def test_parquet_format_detection(self):
        assert detect_format_from_path("data.parquet") == "parquet"
        assert detect_format_from_path("path/to/data.PARQUET") == "parquet"
        assert detect_format_from_path("table.parquet") == "parquet"

    def test_unsupported_format_raises_error(self):
        with pytest.raises(ValueError, match="Could not infer format from path"):
            detect_format_from_path("data.txt")

        with pytest.raises(ValueError, match="Could not infer format from path"):
            detect_format_from_path("data.xlsx")

        with pytest.raises(ValueError, match="Could not infer format from path"):
            detect_format_from_path("no_extension")

    def test_path_with_multiple_dots(self):
        assert detect_format_from_path("data.backup.csv") == "csv"
        assert detect_format_from_path("file.v1.2.json") == "json"
        assert detect_format_from_path("table.final.parquet") == "parquet"