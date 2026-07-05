"""Tests for the v0.5 feature set: local files, positional PATH,
compound WHERE, and streaming WHERE with Parquet pushdown."""

import io
import pytest
import pandas as pd
from click.testing import CliRunner

from cloudcat.cli import main
from cloudcat.storage.base import parse_cloud_path
from cloudcat.filtering import parse_where_expression, apply_where_filter, where_columns
from cloudcat.readers.csv import read_csv_data_streaming
from cloudcat.readers.json import read_json_data_streaming
from cloudcat.readers.text import read_text_data_streaming


runner = CliRunner()


class TestLocalPathParsing:
    def test_bare_relative_path(self, tmp_path):
        f = tmp_path / "x.csv"
        f.write_text("a\n1\n")
        service, bucket, obj = parse_cloud_path(str(f))
        assert service == "local"
        assert bucket == ""
        assert obj == str(f)

    def test_file_url(self, tmp_path):
        f = tmp_path / "x.csv"
        f.write_text("a\n1\n")
        service, _, obj = parse_cloud_path(f"file://{f}")
        assert service == "local"
        assert obj == str(f)

    def test_directory_gets_trailing_slash(self, tmp_path):
        service, _, obj = parse_cloud_path(str(tmp_path))
        assert service == "local"
        assert obj.endswith("/")

    def test_cloud_schemes_unaffected(self):
        assert parse_cloud_path("s3://bucket/k.csv")[0] == "s3"
        assert parse_cloud_path("gs://bucket/k.csv")[0] == "gcs"


class TestLocalEndToEnd:
    def _write_csv(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("name,age\nAlice,30\nBob,25\nCarol,35\n")
        return f

    def test_positional_path(self, tmp_path):
        f = self._write_csv(tmp_path)
        res = runner.invoke(main, [str(f), "--schema", "dont_show", "-o", "csv"])
        assert res.exit_code == 0
        assert "Alice" in res.stdout

    def test_path_option_alias(self, tmp_path):
        f = self._write_csv(tmp_path)
        res = runner.invoke(main, ["--path", str(f), "--schema", "dont_show", "-o", "csv"])
        assert res.exit_code == 0
        assert "Alice" in res.stdout

    def test_both_paths_differ_errors(self, tmp_path):
        f = self._write_csv(tmp_path)
        res = runner.invoke(main, [str(f), "--path", "other.csv"])
        assert res.exit_code == 2
        assert "use one" in res.output

    def test_missing_path_errors(self):
        res = runner.invoke(main, [])
        assert res.exit_code == 2
        assert "Missing PATH" in res.output

    def test_local_directory_multifile(self, tmp_path):
        (tmp_path / "a.csv").write_text("x\n1\n2\n")
        (tmp_path / "b.csv").write_text("x\n3\n4\n")
        res = runner.invoke(main, [str(tmp_path) + "/", "--schema", "dont_show",
                                   "-o", "csv", "-n", "0", "-m", "all"])
        assert res.exit_code == 0
        for v in ["1", "2", "3", "4"]:
            assert v in res.stdout

    def test_local_parquet_native_fs(self, tmp_path):
        pa = pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq
        f = tmp_path / "d.parquet"
        pq.write_table(pa.Table.from_pandas(pd.DataFrame({"a": [1, 2, 3]})), f)
        res = runner.invoke(main, [str(f), "--schema", "dont_show", "-o", "csv"])
        assert res.exit_code == 0
        assert "1" in res.stdout


class TestCompoundWhere:
    def _df(self):
        return pd.DataFrame({
            "status": ["active", "active", "inactive", "active"],
            "age": [25, 35, 40, 45],
        })

    def test_and(self):
        out = apply_where_filter(self._df(), "status=active AND age>30")
        assert list(out["age"]) == [35, 45]

    def test_or(self):
        out = apply_where_filter(self._df(), "age<30 or age>42")
        assert list(out["age"]) == [25, 45]

    def test_and_binds_tighter_than_or(self):
        # a OR (b AND c)
        out = apply_where_filter(self._df(), "age<30 or status=active and age>40")
        assert list(out["age"]) == [25, 45]

    def test_quoted_value_with_and_inside_compound(self):
        df = pd.DataFrame({"title": ["Alice and Bob", "Solo"], "n": [1, 2]})
        out = apply_where_filter(df, "title='Alice and Bob' or n=2")
        assert len(out) == 2

    def test_where_columns_dedup(self):
        assert where_columns("a=1 and b=2 or a=3") == ["a", "b"]

    def test_expression_structure(self):
        assert parse_where_expression("a=1 AND b=2 OR c=3") == [
            [("a", "=", "1"), ("b", "=", "2")],
            [("c", "=", "3")],
        ]


class TestStreamingWhere:
    def test_csv_early_stop(self):
        # 5000 rows; matches everywhere -> stops after the first chunk
        rows = "".join(f"{i},hit\n" for i in range(5000))
        stream = io.BytesIO(b"n,tag\n" + rows.encode())
        df, _, stats = read_csv_data_streaming(stream, 3, where="tag=hit")
        assert len(df) == 3
        assert stats.where_applied
        assert stats.rows_scanned < 5000  # early stop

    def test_csv_scans_everything_when_needed(self):
        rows = "".join(f"{i},{'hit' if i == 4999 else 'miss'}\n" for i in range(5000))
        stream = io.BytesIO(b"n,tag\n" + rows.encode())
        df, _, stats = read_csv_data_streaming(stream, 3, where="tag=hit")
        assert len(df) == 1
        assert df.iloc[0]["n"] == 4999
        assert stats.rows_scanned == 5000

    def test_csv_no_matches_keeps_schema(self):
        stream = io.BytesIO(b"a,b\n1,x\n2,y\n")
        df, schema, stats = read_csv_data_streaming(stream, 5, where="a>99")
        assert len(df) == 0
        assert list(df.columns) == ["a", "b"]

    def test_jsonl_early_stop(self):
        payload = b"".join(b'{"n": %d}\n' % i for i in range(5000))
        df, _, stats = read_json_data_streaming(io.BytesIO(payload), 2, where="n>=0")
        assert len(df) == 2
        assert stats.where_applied
        assert stats.rows_scanned < 5000

    def test_json_array_filtered(self):
        payload = b'[{"n": 1}, {"n": 2}, {"n": 3}]'
        df, _, stats = read_json_data_streaming(io.BytesIO(payload), 0, where="n>1")
        assert list(df["n"]) == [2, 3]
        assert stats.where_applied

    def test_text_filter_preserves_line_numbers(self):
        stream = io.BytesIO(b"apple\nbanana\napricot\ncherry\n")
        df, _, stats = read_text_data_streaming(stream, 0, where="line startswith a")
        assert list(df["line"]) == ["apple", "apricot"]
        assert list(df["line_number"]) == [1, 3]  # original file positions

    def test_avro_filtered(self):
        fastavro = pytest.importorskip("fastavro")
        from cloudcat.readers.avro import read_avro_data_streaming
        schema = {"type": "record", "name": "R",
                  "fields": [{"name": "n", "type": "int"}]}
        buf = io.BytesIO()
        fastavro.writer(buf, schema, [{"n": i} for i in range(100)])
        buf.seek(0)
        df, _, stats = read_avro_data_streaming(buf, 5, where="n>=90")
        assert list(df["n"]) == [90, 91, 92, 93, 94]
        assert stats.where_applied


class TestParquetPushdown:
    def _write_grouped(self, tmp_path):
        pa = pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq
        f = tmp_path / "grouped.parquet"
        df = pd.DataFrame({"id": range(1000), "val": [f"v{i}" for i in range(1000)]})
        pq.write_table(pa.Table.from_pandas(df), f, row_group_size=100)
        return f

    def test_row_groups_skipped_via_statistics(self, tmp_path):
        pytest.importorskip("pyarrow")
        from cloudcat.readers.parquet import read_parquet_data_streaming
        from cloudcat.streaming.stats import StreamingStats
        f = self._write_grouped(tmp_path)
        stats = StreamingStats()
        with open(f, "rb") as fh:
            df, _, stats = read_parquet_data_streaming(
                stream=fh, num_rows=5, stats=stats, where="id>=900")
        assert list(df["id"]) == [900, 901, 902, 903, 904]
        assert stats.row_groups_skipped == 9  # groups 0..8 pruned by min/max
        assert stats.where_applied

    def test_pushdown_never_skips_matching_groups(self, tmp_path):
        pytest.importorskip("pyarrow")
        from cloudcat.readers.parquet import read_parquet_data_streaming
        from cloudcat.streaming.stats import StreamingStats
        f = self._write_grouped(tmp_path)
        with open(f, "rb") as fh:
            df, _, stats = read_parquet_data_streaming(
                stream=fh, num_rows=0, stats=StreamingStats(), where="id=550")
        assert list(df["id"]) == [550]

    def test_contains_operator_does_not_prune(self, tmp_path):
        pytest.importorskip("pyarrow")
        from cloudcat.readers.parquet import read_parquet_data_streaming
        from cloudcat.streaming.stats import StreamingStats
        f = self._write_grouped(tmp_path)
        with open(f, "rb") as fh:
            df, _, stats = read_parquet_data_streaming(
                stream=fh, num_rows=0, stats=StreamingStats(), where="val contains v99")
        # v99, v990..v999 -> 11 matches; contains can't use min/max stats
        assert len(df) == 11
        assert stats.row_groups_skipped == 0

    def test_compound_where_with_pushdown(self, tmp_path):
        pytest.importorskip("pyarrow")
        from cloudcat.readers.parquet import read_parquet_data_streaming
        from cloudcat.streaming.stats import StreamingStats
        f = self._write_grouped(tmp_path)
        with open(f, "rb") as fh:
            df, _, stats = read_parquet_data_streaming(
                stream=fh, num_rows=0, stats=StreamingStats(),
                where="id<50 or id>=950")
        assert len(df) == 100
        assert stats.row_groups_skipped == 8  # middle groups pruned


class TestWhereOffsetPagination:
    def test_offset_pages_through_matches(self, tmp_path):
        f = tmp_path / "p.csv"
        f.write_text("n,tag\n" + "".join(
            f"{i},{'hit' if i % 2 == 0 else 'miss'}\n" for i in range(20)))
        res = runner.invoke(main, [str(f), "--schema", "dont_show", "-o", "csv",
                                   "-w", "tag=hit", "-n", "2", "--offset", "2"])
        assert res.exit_code == 0
        # matches are n=0,2,4,...; offset 2 skips the first two matches
        lines = [l for l in res.stdout.splitlines() if l and not l.startswith("n,")]
        assert lines == ["4,hit", "6,hit"]
