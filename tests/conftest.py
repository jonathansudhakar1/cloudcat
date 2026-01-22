import pytest
import pandas as pd
from unittest.mock import MagicMock


@pytest.fixture
def sample_csv_data():
    return "name,age,city\nJohn,25,NYC\nJane,30,LA\nBob,35,SF"


@pytest.fixture
def sample_json_data():
    return '{"name": "John", "age": 25, "city": "NYC"}\n{"name": "Jane", "age": 30, "city": "LA"}'


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({
        "name": ["John", "Jane", "Bob"],
        "age": [25, 30, 35],
        "city": ["NYC", "LA", "SF"]
    })


@pytest.fixture
def mock_gcs_client():
    client = MagicMock()
    bucket = MagicMock()
    client.bucket.return_value = bucket
    return client, bucket


@pytest.fixture
def mock_s3_client():
    client = MagicMock()
    return client


@pytest.fixture
def sample_file_list():
    return [
        ("data1.csv", 1024),
        ("data2.csv", 2048),
        ("data3.csv", 4096)
    ]