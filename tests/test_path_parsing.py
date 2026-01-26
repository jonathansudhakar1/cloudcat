import pytest
from cloudcat.cli import parse_cloud_path


class TestPathParsing:
    def test_gcs_path_parsing(self):
        service, bucket, object_path = parse_cloud_path("gcs://my-bucket/path/to/file.csv")
        assert service == "gcs"
        assert bucket == "my-bucket"
        assert object_path == "path/to/file.csv"

    def test_gs_path_parsing(self):
        service, bucket, object_path = parse_cloud_path("gs://my-bucket/data/file.json")
        assert service == "gcs"
        assert bucket == "my-bucket"
        assert object_path == "data/file.json"

    def test_s3_path_parsing(self):
        service, bucket, object_path = parse_cloud_path("s3://my-bucket/folder/data.parquet")
        assert service == "s3"
        assert bucket == "my-bucket"
        assert object_path == "folder/data.parquet"

    def test_path_with_directory_trailing_slash(self):
        service, bucket, object_path = parse_cloud_path("gcs://bucket/folder/")
        assert service == "gcs"
        assert bucket == "bucket"
        assert object_path == "folder/"

    def test_path_without_object(self):
        service, bucket, object_path = parse_cloud_path("s3://bucket/")
        assert service == "s3"
        assert bucket == "bucket"
        assert object_path == ""

    def test_invalid_scheme_raises_error(self):
        with pytest.raises(ValueError, match="Unsupported scheme: http"):
            parse_cloud_path("http://bucket/file.csv")

    def test_empty_path_raises_error(self):
        with pytest.raises(ValueError):
            parse_cloud_path("")

    def test_path_with_nested_folders(self):
        service, bucket, object_path = parse_cloud_path("gcs://data-lake/year=2023/month=12/day=01/data.parquet")
        assert service == "gcs"
        assert bucket == "data-lake"
        assert object_path == "year=2023/month=12/day=01/data.parquet"

    def test_az_path_parsing(self):
        service, bucket, object_path = parse_cloud_path("az://mycontainer/path/to/file.csv")
        assert service == "azure"
        assert bucket == "mycontainer"
        assert object_path == "path/to/file.csv"

    def test_abfss_path_parsing(self):
        """Test Azure Data Lake Storage Gen2 abfss:// scheme."""
        service, bucket, object_path = parse_cloud_path("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/path/to/file.parquet")
        assert service == "azure"
        assert bucket == "mycontainer"
        assert object_path == "path/to/file.parquet"

    def test_abfss_path_without_account(self):
        """Test abfss:// with just container name."""
        service, bucket, object_path = parse_cloud_path("abfss://mycontainer/path/to/file.csv")
        assert service == "azure"
        assert bucket == "mycontainer"
        assert object_path == "path/to/file.csv"