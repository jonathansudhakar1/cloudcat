"""Tests for read_data_streaming, the core read path mocked out elsewhere.

These patch only the lowest-level storage boundary (get_stream/get_file_size)
so the real streaming/offset/column logic runs.
"""

import io
import pytest
from unittest.mock import patch

import cloudcat.cli as cli

CSV = b"name,age,city\nJohn,25,NYC\nJane,30,LA\nBob,35,SF\nAmy,40,DC\n"
JSONL = b'{"a": 1}\n{"a": 2}\n{"a": 3}\n{"a": 4}\n'


def _run_csv(num_rows, offset=0, columns=None):
    with patch.object(cli, "get_stream", lambda s, b, p: io.BytesIO(CSV)), \
         patch.object(cli, "get_file_size", lambda s, b, p: len(CSV)):
        return cli.read_data_streaming("s3", "bucket", "f.csv", "csv",
                                       num_rows, columns, None, offset)


def test_num_rows_limit():
    df, schema, stats = _run_csv(2)
    assert len(df) == 2
    assert df.iloc[0]["name"] == "John"


def test_offset_skips_rows():
    df, schema, stats = _run_csv(2, offset=1)
    assert len(df) == 2
    assert df.iloc[0]["name"] == "Jane"  # first row skipped


def test_column_projection():
    df, schema, stats = _run_csv(10, columns="name,age")
    assert list(df.columns) == ["name", "age"]
    # Full schema still carries every column
    assert set(schema.index) == {"name", "age", "city"}


def test_offset_beyond_data_yields_empty():
    df, schema, stats = _run_csv(2, offset=100)
    assert len(df) == 0


def test_stats_bytes_read_reflects_transfer_not_memory():
    df, schema, stats = _run_csv(2)
    # bytes_read should be measured from the source, never exceeding file size
    assert 0 < stats.bytes_read <= len(CSV)


def test_jsonlines_streaming():
    with patch.object(cli, "get_stream", lambda s, b, p: io.BytesIO(JSONL)), \
         patch.object(cli, "get_file_size", lambda s, b, p: len(JSONL)):
        df, schema, stats = cli.read_data_streaming("gcs", "b", "f.json", "json", 2, None, None, 0)
    assert len(df) == 2
    assert df.iloc[0]["a"] == 1
