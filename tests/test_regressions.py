"""Regression tests for specific bugs found and fixed."""

import io
import pytest
from unittest.mock import patch

import cloudcat.cli as cli
from cloudcat.readers.json import read_json_data_streaming
from cloudcat.filtering import parse_where_clause
from cloudcat.compression import decompress_stream, HAS_ZSTD


class TestJsonPrettyPrintedObject:
    """A multi-line single JSON object starts with '{' but is not JSON Lines."""

    def test_pretty_object_returns_one_row(self):
        pretty = b'{\n  "name": "John",\n  "age": 30,\n  "city": "NYC"\n}\n'
        df, schema, stats = read_json_data_streaming(io.BytesIO(pretty), num_rows=10)
        assert len(df) == 1
        assert df.iloc[0]["name"] == "John"
        assert set(df.columns) == {"name", "age", "city"}

    def test_pretty_object_with_more_lines_than_num_rows(self):
        # 7 fields => 9 lines; num_rows=3 must NOT truncate the object.
        pretty = (b'{\n  "a": 1,\n  "b": 2,\n  "c": 3,\n  "d": 4,\n'
                  b'  "e": 5,\n  "f": 6,\n  "g": 7\n}\n')
        df, _, _ = read_json_data_streaming(io.BytesIO(pretty), num_rows=3)
        assert len(df) == 1
        assert df.iloc[0]["g"] == 7

    def test_jsonlines_still_streams_with_limit(self):
        jsonl = b"".join(b'{"a": %d}\n' % i for i in range(100))
        df, _, _ = read_json_data_streaming(io.BytesIO(jsonl), num_rows=5)
        assert len(df) == 5

    def test_pretty_array_returns_rows(self):
        pretty = b'[\n  {"a": 1},\n  {"a": 2}\n]\n'
        df, _, _ = read_json_data_streaming(io.BytesIO(pretty), num_rows=10)
        assert len(df) == 2


@pytest.mark.skipif(not HAS_ZSTD, reason="zstandard not installed")
class TestZstdMultiFrame:
    def test_multi_frame_fully_decompressed(self):
        import zstandard as zstd
        c = zstd.ZstdCompressor()
        multi = c.compress(b"frame1\n") + c.compress(b"frame2\n")
        out = decompress_stream(io.BytesIO(multi), "zstd").read()
        assert out == b"frame1\nframe2\n"

    def test_single_frame_still_works(self):
        import zstandard as zstd
        c = zstd.ZstdCompressor()
        out = decompress_stream(io.BytesIO(c.compress(b"hello\nworld\n")), "zstd").read()
        assert out == b"hello\nworld\n"


class TestCompoundWhereRejected:
    @pytest.mark.parametrize("clause", ["a=1 and b=2", "x>3 or y<5", "A=1 AND B=2"])
    def test_compound_raises(self, clause):
        with pytest.raises(ValueError, match="Compound conditions"):
            parse_where_clause(clause)

    def test_single_condition_still_parses(self):
        assert parse_where_clause("status=active") == ("status", "=", "active")
        # A value that merely contains the substring 'and' is fine
        assert parse_where_clause("name=sandra") == ("name", "=", "sandra")


class TestMultiFileOffsetReadAll:
    """num_rows=0 (read all) with an offset must not silently return empty."""

    def test_offset_with_read_all(self):
        csv = b"a,b\n1,x\n2,y\n3,z\n"  # 3 rows

        def fake_stream(service, bucket, path):
            return io.BytesIO(csv)

        file_list = [("f1.csv", len(csv)), ("f2.csv", len(csv))]
        with patch.object(cli, "get_stream", fake_stream):
            df, schema, total = cli.read_data_from_multiple_files(
                "s3", "bucket", file_list, "csv", 0, None, offset=1
            )
        # 2 files x 3 rows = 6 rows, minus offset of 1 => 5 rows
        assert len(df) == 5
