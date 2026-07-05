"""Regression tests for the second bug-hunt round (post-0.4.0)."""

import io
import gzip
import pytest
import pandas as pd
from unittest.mock import patch
from click.testing import CliRunner

import cloudcat.cli as cli
from cloudcat.cli import main, get_record_count_multiple_files
from cloudcat.compression import (
    decompress_stream,
    get_streaming_decompressor,
    HAS_ZSTD,
    HAS_LZ4,
)
from cloudcat.filtering import parse_where_clause, apply_where_filter
from cloudcat.formatters import format_table_with_colored_header
from cloudcat.readers.json import read_json_data_streaming
from cloudcat.readers.avro import read_avro_data_streaming
from cloudcat.storage.base import parse_cloud_path
from cloudcat.streaming.tracking import BytesTrackingStream
from cloudcat.streaming.stats import StreamingStats
from cloudcat.config import cloud_config


@pytest.mark.skipif(not HAS_ZSTD, reason="zstandard not installed")
class TestZstdStreamingLineIteration:
    """zstd's raw stream_reader has no readline; JSONL/.txt.zst crashed."""

    def test_jsonl_zst_streams_by_line(self):
        import zstandard as zstd
        data = zstd.ZstdCompressor().compress(b'{"a": 1}\n{"a": 2}\n')
        stream, is_streaming = get_streaming_decompressor(io.BytesIO(data), 'zstd')
        df, _, _ = read_json_data_streaming(stream, 0)
        assert len(df) == 2
        assert is_streaming

    def test_text_zst_streams_by_line(self):
        import zstandard as zstd
        from cloudcat.readers.text import read_text_data_streaming
        data = zstd.ZstdCompressor().compress(b"l1\nl2\nl3\n")
        stream, _ = get_streaming_decompressor(io.BytesIO(data), 'zstd')
        df, _, _ = read_text_data_streaming(stream, 2)
        assert len(df) == 2


@pytest.mark.skipif(not HAS_LZ4, reason="lz4 not installed")
class TestLz4MultiFrame:
    """decompress_stream dropped every lz4 frame after the first."""

    def test_multi_frame_fully_decompressed(self):
        import lz4.frame
        multi = lz4.frame.compress(b"frame1\n") + lz4.frame.compress(b"frame2\n")
        assert decompress_stream(io.BytesIO(multi), 'lz4').read() == b"frame1\nframe2\n"

    def test_single_frame_still_works(self):
        import lz4.frame
        single = lz4.frame.compress(b"hello\n")
        assert decompress_stream(io.BytesIO(single), 'lz4').read() == b"hello\n"


class TestJsonBom:
    """BOM-prefixed JSON silently returned an empty DataFrame."""

    def test_bom_jsonl(self):
        df, _, _ = read_json_data_streaming(io.BytesIO(b'\xef\xbb\xbf{"a": 1}\n{"a": 2}\n'), 0)
        assert len(df) == 2

    def test_bom_array(self):
        df, _, _ = read_json_data_streaming(io.BytesIO(b'\xef\xbb\xbf[{"a": 1}]'), 0)
        assert len(df) == 1


class TestDuplicateColumnTable:
    """Duplicate column names (legal in Parquet) crashed the table renderer."""

    def test_duplicate_columns_render(self):
        df = pd.DataFrame([[1, "x"], [2, "y"]], columns=["a", "a"])
        out = format_table_with_colored_header(df)
        assert "x" in out and "y" in out


class TestAvroMissingColumns:
    """All-missing --columns was silent for Avro; now matches CSV/JSON."""

    def _avro_stream(self):
        fastavro = pytest.importorskip("fastavro")
        schema = {"type": "record", "name": "R",
                  "fields": [{"name": "a", "type": "int"}]}
        buf = io.BytesIO()
        fastavro.writer(buf, schema, [{"a": i} for i in range(3)])
        buf.seek(0)
        return buf

    def test_all_missing_columns_raise(self):
        with pytest.raises(ValueError, match="None of the requested columns exist"):
            read_avro_data_streaming(self._avro_stream(), 0, "zzz")

    def test_valid_column_still_works(self):
        df, _, _ = read_avro_data_streaming(self._avro_stream(), 0, "a")
        assert len(df) == 3


class TestReadlinesHint:
    """readlines(0) must mean 'no limit', matching the stdlib contract."""

    def test_hint_zero_returns_all(self):
        s = BytesTrackingStream(io.BytesIO(b"l1\nl2\nl3\n"), StreamingStats())
        assert s.readlines(0) == [b"l1\n", b"l2\n", b"l3\n"]


class TestWhereParsing:
    def test_quoted_value_containing_and(self):
        assert parse_where_clause("title='Alice and Bob'") == ("title", "=", "Alice and Bob")

    def test_quoted_value_containing_or(self):
        assert parse_where_clause('note="this or that"') == ("note", "=", "this or that")

    def test_equals_value_containing_word_operator(self):
        assert parse_where_clause("note=this contains that") == ("note", "=", "this contains that")

    def test_single_parser_rejects_raw_compound(self):
        # The single-condition parser never mis-parses compound input;
        # compound expressions go through parse_where_expression instead.
        for clause in ["a=1 and b=2", "x>3 or y<5", "A=1 AND B=2"]:
            with pytest.raises(ValueError):
                parse_where_clause(clause)

    def test_word_operator_still_parses(self):
        assert parse_where_clause("name contains john") == ("name", "contains", "john")
        assert parse_where_clause("name not contains test") == ("name", "not contains", "test")

    def test_comparison_operators_unchanged(self):
        assert parse_where_clause("age>=30") == ("age", ">=", "30")
        assert parse_where_clause("age!=30") == ("age", "!=", "30")


class TestNotEqualsExcludesNull:
    def test_ne_excludes_nan(self):
        df = pd.DataFrame({"name": ["John", "Jane", "Bob"], "age": [25.0, None, 30.0]})
        out = apply_where_filter(df, "age!=25")
        assert list(out["name"]) == ["Bob"]  # NaN row excluded


class TestPathParsingSpecialChars:
    """'#' and '?' are legal in object keys; urlparse truncated them."""

    def test_hash_in_key(self):
        assert parse_cloud_path("s3://bucket/data#1.csv") == ("s3", "bucket", "data#1.csv")

    def test_question_mark_in_key(self):
        assert parse_cloud_path("gs://bucket/a?b.csv") == ("gcs", "bucket", "a?b.csv")

    def test_normal_paths_unchanged(self):
        assert parse_cloud_path("s3://bucket/dir/file.csv") == ("s3", "bucket", "dir/file.csv")
        assert parse_cloud_path("abfss://cont@acct.dfs.core.windows.net/p/f.csv")[0:2] == ("azure", "cont")

    def test_uppercase_scheme(self):
        assert parse_cloud_path("S3://bucket/f.csv")[0] == "s3"


class TestMultiFileCountFailures:
    def test_all_failures_raise_instead_of_zero(self):
        with patch.object(cli, "get_record_count", side_effect=ConnectionError("network down")):
            with pytest.raises(ValueError, match="could not count any"):
                get_record_count_multiple_files("s3", "b", [("f1.csv", 10), ("f2.csv", 10)], "csv")

    def test_partial_failure_returns_partial_with_warning(self, capsys):
        def counts(service, bucket, name, fmt, delim, quiet):
            if name == "bad.csv":
                raise ConnectionError("boom")
            return 5
        with patch.object(cli, "get_record_count", side_effect=counts):
            total = get_record_count_multiple_files("s3", "b", [("ok.csv", 10), ("bad.csv", 10)], "csv")
        assert total == 5
        assert "1 of 2 files could not be counted" in capsys.readouterr().err


CSV = b"name,age,status\nJohn,25,active\nJane,30,inactive\nBob,35,active\nAmy,40,active\nEve,28,active\nDan,50,inactive\n"


def _invoke(args):
    runner = CliRunner()
    with patch.object(cli, "get_stream", lambda s, b, p: io.BytesIO(CSV)), \
         patch.object(cli, "get_file_size", lambda s, b, p: len(CSV)):
        return runner.invoke(main, args)


class TestCliInteractions:
    def test_where_on_non_displayed_column(self):
        res = _invoke(["--path", "s3://b/f.csv", "--columns", "name",
                       "--where", "age>28", "--schema", "dont_show", "--output-format", "csv"])
        assert res.exit_code == 0
        # Filter column read for the WHERE, but display shows only 'name'
        assert "name" in res.stdout and "age" not in res.stdout.splitlines()[0]
        assert "Jane" in res.stdout and "Bob" in res.stdout
        assert "John" not in res.stdout  # age 25 filtered out

    def test_filtered_message_is_honest_about_matches(self):
        res = _invoke(["--path", "s3://b/f.csv", "--where", "status=active",
                       "--num-rows", "1", "--schema", "dont_show", "--output-format", "csv"])
        assert res.exit_code == 0
        # Streaming WHERE stops at the first match; the message reports the
        # matches returned and the rows scanned, never a fabricated total.
        assert "Filtered: 1 matching rows" in res.output
        assert "scanned" in res.output
        assert "John" in res.stdout  # first matching row

    def test_schema_only_with_output_file(self, tmp_path):
        out = tmp_path / "schema.txt"
        res = _invoke(["--path", "s3://b/f.csv", "--input-format", "csv",
                       "--schema", "schema_only", "--output-file", str(out)])
        assert res.exit_code == 0
        content = out.read_text()
        assert "Schema:" in content and "name" in content

    def test_count_abort_exits_nonzero(self):
        runner = CliRunner()
        listing = [("dir/a.csv", 10), ("dir/b.csv", 10)]
        with patch.object(cli, "get_stream", lambda s, b, p: io.BytesIO(CSV)), \
             patch.object(cli, "get_file_size", lambda s, b, p: len(CSV)), \
             patch.object(cli, "list_directory", lambda s, b, p: listing):
            res = runner.invoke(main, ["--path", "s3://b/dir/", "--count",
                                       "--multi-file-mode", "all"], input="n\n")
        assert res.exit_code == 1
        assert "Aborted" in res.output

    def test_cloud_config_does_not_leak_between_invocations(self):
        _invoke(["--path", "s3://b/f.csv", "--profile", "production", "--schema", "dont_show"])
        assert cloud_config.aws_profile == "production"
        _invoke(["--path", "s3://b/f.csv", "--schema", "dont_show"])
        assert cloud_config.aws_profile is None  # reset, not leaked

    def test_where_quoted_value_via_cli(self):
        csv = b"title,n\nAlice and Bob,1\nCharlie,2\n"
        runner = CliRunner()
        with patch.object(cli, "get_stream", lambda s, b, p: io.BytesIO(csv)), \
             patch.object(cli, "get_file_size", lambda s, b, p: len(csv)):
            res = runner.invoke(main, ["--path", "s3://b/f.csv",
                                       "--where", "title='Alice and Bob'",
                                       "--schema", "dont_show", "--output-format", "csv"])
        assert res.exit_code == 0
        assert "Alice and Bob" in res.stdout
        assert "Charlie" not in res.stdout
