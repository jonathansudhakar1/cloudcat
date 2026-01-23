"""Tests for configuration management."""

import pytest
from cloudcat.config import (
    CloudConfig,
    cloud_config,
    SKIP_PATTERNS,
    COMPRESSION_EXTENSIONS,
    FORMAT_EXTENSION_MAP,
)


class TestCloudConfig:
    """Tests for CloudConfig class."""

    def test_default_values(self):
        config = CloudConfig()
        assert config.aws_profile is None
        assert config.gcp_project is None
        assert config.gcp_credentials is None
        assert config.azure_account is None

    def test_set_aws_profile(self):
        config = CloudConfig()
        config.aws_profile = "production"
        assert config.aws_profile == "production"

    def test_set_gcp_project(self):
        config = CloudConfig()
        config.gcp_project = "my-project"
        assert config.gcp_project == "my-project"

    def test_set_gcp_credentials(self):
        config = CloudConfig()
        config.gcp_credentials = "/path/to/credentials.json"
        assert config.gcp_credentials == "/path/to/credentials.json"

    def test_set_azure_account(self):
        config = CloudConfig()
        config.azure_account = "mystorageaccount"
        assert config.azure_account == "mystorageaccount"

    def test_reset(self):
        config = CloudConfig()
        config.aws_profile = "production"
        config.gcp_project = "my-project"
        config.gcp_credentials = "/path/to/credentials.json"
        config.azure_account = "mystorageaccount"

        config.reset()

        assert config.aws_profile is None
        assert config.gcp_project is None
        assert config.gcp_credentials is None
        assert config.azure_account is None


class TestGlobalConfig:
    """Tests for global cloud_config instance."""

    def setup_method(self):
        """Reset global config before each test."""
        cloud_config.reset()

    def teardown_method(self):
        """Reset global config after each test."""
        cloud_config.reset()

    def test_global_config_exists(self):
        assert cloud_config is not None
        assert isinstance(cloud_config, CloudConfig)

    def test_global_config_can_be_modified(self):
        cloud_config.aws_profile = "test-profile"
        assert cloud_config.aws_profile == "test-profile"


class TestConstants:
    """Tests for configuration constants."""

    def test_skip_patterns_exist(self):
        assert len(SKIP_PATTERNS) > 0
        assert r'_SUCCESS$' in SKIP_PATTERNS
        assert r'\.crc$' in SKIP_PATTERNS

    def test_compression_extensions_exist(self):
        assert len(COMPRESSION_EXTENSIONS) > 0
        assert '.gz' in COMPRESSION_EXTENSIONS
        assert '.zst' in COMPRESSION_EXTENSIONS
        assert '.lz4' in COMPRESSION_EXTENSIONS
        assert '.snappy' in COMPRESSION_EXTENSIONS
        assert '.bz2' in COMPRESSION_EXTENSIONS

    def test_format_extension_map_exists(self):
        assert 'csv' in FORMAT_EXTENSION_MAP
        assert 'json' in FORMAT_EXTENSION_MAP
        assert 'parquet' in FORMAT_EXTENSION_MAP
        assert 'avro' in FORMAT_EXTENSION_MAP
        assert 'orc' in FORMAT_EXTENSION_MAP
        assert 'text' in FORMAT_EXTENSION_MAP

    def test_format_extension_map_patterns(self):
        import re
        # Test CSV pattern matches .csv files
        assert re.search(FORMAT_EXTENSION_MAP['csv'], 'file.csv')
        assert not re.search(FORMAT_EXTENSION_MAP['csv'], 'file.json')

        # Test JSON pattern matches .json, .jsonl, .ndjson
        assert re.search(FORMAT_EXTENSION_MAP['json'], 'file.json')
        assert re.search(FORMAT_EXTENSION_MAP['json'], 'file.jsonl')
        assert re.search(FORMAT_EXTENSION_MAP['json'], 'file.ndjson')
