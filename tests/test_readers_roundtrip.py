"""Round-trip tests for the format readers: row limiting + column projection.

These exercise the actual reader code paths (previously largely untested) by
writing a small file in each format and reading it back.
"""

import io
import pytest
import pandas as pd

from cloudcat.readers import (
    read_csv_data,
    read_json_data,
    read_parquet_data,
    read_avro_data,
    read_orc_data,
    read_text_data,
)

SAMPLE = pd.DataFrame({
    "a": [1, 2, 3, 4, 5],
    "b": ["v", "w", "x", "y", "z"],
    "c": [1.0, 2.0, 3.0, 4.0, 5.0],
})


def test_csv_roundtrip_row_limit_and_columns():
    stream = io.BytesIO(SAMPLE.to_csv(index=False).encode())
    df, schema = read_csv_data(stream, 2, "a,b")
    assert len(df) == 2
    assert list(df.columns) == ["a", "b"]
    # Full schema retains all columns
    assert set(schema.index) == {"a", "b", "c"}


def test_json_array_roundtrip():
    content = SAMPLE.to_json(orient="records").encode()
    df, _ = read_json_data(io.BytesIO(content), 3, "a")
    assert len(df) == 3
    assert list(df.columns) == ["a"]


def test_jsonlines_roundtrip():
    content = SAMPLE.to_json(orient="records", lines=True).encode()
    df, _ = read_json_data(io.BytesIO(content), 2)
    assert len(df) == 2


def test_parquet_roundtrip_row_limit_and_columns():
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(SAMPLE), buf)
    buf.seek(0)
    df, schema = read_parquet_data(buf, 2, "a")
    assert len(df) == 2
    assert list(df.columns) == ["a"]
    # Schema derived from metadata reflects every column
    assert set(schema.index) == {"a", "b", "c"}


def test_orc_roundtrip_row_limit_and_columns():
    pa = pytest.importorskip("pyarrow")
    import pyarrow.orc as orc
    buf = io.BytesIO()
    orc.write_table(pa.Table.from_pandas(SAMPLE), buf)
    buf.seek(0)
    df, schema = read_orc_data(buf, 3, "b")
    assert len(df) == 3
    assert list(df.columns) == ["b"]
    assert set(schema.index) == {"a", "b", "c"}


def test_avro_roundtrip_row_limit_and_columns():
    fastavro = pytest.importorskip("fastavro")
    schema = {
        "type": "record",
        "name": "r",
        "fields": [
            {"name": "a", "type": "int"},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "double"},
        ],
    }
    buf = io.BytesIO()
    fastavro.writer(buf, schema, SAMPLE.to_dict("records"))
    buf.seek(0)
    df, _ = read_avro_data(buf, 2, "a")
    assert len(df) == 2
    assert list(df.columns) == ["a"]


def test_avro_roundtrip_from_file_path(tmp_path):
    """Regression: reading from a path must not read from a closed handle."""
    fastavro = pytest.importorskip("fastavro")
    schema = {
        "type": "record",
        "name": "r",
        "fields": [{"name": "a", "type": "int"}, {"name": "b", "type": "string"}],
    }
    p = tmp_path / "data.avro"
    with open(p, "wb") as f:
        fastavro.writer(f, schema, SAMPLE[["a", "b"]].to_dict("records"))
    df, _ = read_avro_data(str(p), 3)
    assert len(df) == 3
    assert list(df.columns) == ["a", "b"]


def test_text_roundtrip_row_limit():
    stream = io.BytesIO(b"line1\nline2\nline3\nline4\n")
    df, _ = read_text_data(stream, 2)
    assert len(df) == 2
    assert list(df.columns) == ["line", "line_number"]
    assert df.iloc[0]["line"] == "line1"
    assert df.iloc[0]["line_number"] == 1


def test_text_non_utf8_degrades_gracefully():
    """Non-UTF-8 bytes should be replaced, not crash."""
    stream = io.BytesIO(b"good line\n\xff\xfe bad bytes\n")
    df, _ = read_text_data(stream, 0)
    assert len(df) == 2  # both lines present, no UnicodeDecodeError
