"""Tests for file filtering and directory operations."""

import pytest
from unittest.mock import patch, MagicMock
from cloudcat.cli import get_files_for_multiread, find_first_non_empty_file


class TestFileFiltering:
    @patch('cloudcat.cli.list_directory')
    def test_get_files_for_multiread_gcs(self, mock_list_dir):
        mock_list_dir.return_value = [
            ("data1.csv", 1024),
            ("data2.csv", 2048),
            ("_SUCCESS", 0),
            ("data3.csv", 4096)
        ]

        files = get_files_for_multiread("gcs", "bucket", "prefix", max_size_mb=1)

        # Should get data1.csv and data2.csv (totaling ~3KB, less than 1MB limit)
        # but will stop after exceeding the limit
        assert len(files) >= 1
        assert files[0] == ("data1.csv", 1024)

    @patch('cloudcat.cli.list_directory')
    def test_get_files_for_multiread_s3(self, mock_list_dir):
        mock_list_dir.return_value = [
            ("file1.json", 512),
            ("file2.json", 1024),
            ("file3.json", 2048)
        ]

        files = get_files_for_multiread("s3", "bucket", "prefix", max_size_mb=2)

        assert len(files) >= 2
        assert files[0] == ("file1.json", 512)
        assert files[1] == ("file2.json", 1024)

    @patch('cloudcat.cli.list_directory')
    def test_get_files_for_multiread_azure(self, mock_list_dir):
        mock_list_dir.return_value = [
            ("data1.csv", 1024),
            ("data2.csv", 2048)
        ]

        files = get_files_for_multiread("azure", "container", "prefix", max_size_mb=1)

        assert len(files) >= 1
        assert files[0] == ("data1.csv", 1024)

    @patch('cloudcat.cli.list_directory')
    def test_get_files_filters_metadata(self, mock_list_dir):
        mock_list_dir.return_value = [
            ("data.csv", 1024),
            ("_SUCCESS", 0),
            ("file.crc", 128),
            ("data.committed", 64),
            ("data.pending", 32),
            ("_metadata", 256)
        ]

        files = get_files_for_multiread("gcs", "bucket", "prefix")

        assert len(files) == 1
        assert files[0] == ("data.csv", 1024)

    @patch('cloudcat.cli.list_directory')
    def test_get_files_filters_by_format(self, mock_list_dir):
        mock_list_dir.return_value = [
            ("data1.csv", 1024),
            ("data2.json", 2048),
            ("data3.parquet", 4096)
        ]

        files = get_files_for_multiread("gcs", "bucket", "prefix", input_format="csv")

        assert len(files) == 1
        assert files[0] == ("data1.csv", 1024)

    @patch('cloudcat.cli.list_directory')
    def test_get_files_no_matching_format_uses_all(self, mock_list_dir):
        mock_list_dir.return_value = [
            ("data1.json", 1024),
            ("data2.json", 2048)
        ]

        files = get_files_for_multiread("gcs", "bucket", "prefix", input_format="csv")

        assert len(files) == 2

    @patch('cloudcat.cli.list_directory')
    def test_find_first_non_empty_file(self, mock_list_dir):
        mock_list_dir.return_value = [
            ("_SUCCESS", 0),
            ("data1.csv", 1024),
            ("data2.csv", 2048)
        ]

        file_path, file_size = find_first_non_empty_file("gcs", "bucket", "prefix", quiet=True)

        assert file_path == "data1.csv"
        assert file_size == 1024

    @patch('cloudcat.cli.list_directory')
    def test_find_first_non_empty_file_with_format_filter(self, mock_list_dir):
        mock_list_dir.return_value = [
            ("data1.csv", 1024),
            ("data2.json", 2048),
            ("data3.parquet", 4096)
        ]

        file_path, file_size = find_first_non_empty_file("gcs", "bucket", "prefix", input_format="json", quiet=True)

        assert file_path == "data2.json"
        assert file_size == 2048

    @patch('cloudcat.cli.list_directory')
    def test_find_first_non_empty_file_azure(self, mock_list_dir):
        mock_list_dir.return_value = [
            ("_SUCCESS", 0),
            ("data.csv", 1024)
        ]

        file_path, file_size = find_first_non_empty_file("azure", "container", "prefix", quiet=True)

        assert file_path == "data.csv"
        assert file_size == 1024

    @patch('cloudcat.cli.list_directory')
    def test_get_files_for_multiread_no_files_raises_error(self, mock_list_dir):
        mock_list_dir.return_value = []

        with pytest.raises(ValueError, match="No files found"):
            get_files_for_multiread("gcs", "bucket", "prefix")

    @patch('cloudcat.cli.list_directory')
    def test_get_files_for_multiread_only_empty_files_raises_error(self, mock_list_dir):
        mock_list_dir.return_value = [
            ("_SUCCESS", 0),
            ("file.crc", 0)
        ]

        with pytest.raises(ValueError, match="No non-empty files found"):
            get_files_for_multiread("gcs", "bucket", "prefix")

    def test_unsupported_service_raises_error(self):
        with pytest.raises(ValueError, match="Unsupported service"):
            get_files_for_multiread("ftp", "bucket", "prefix")
