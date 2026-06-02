"""Integration tests for new CLI behavior (output hygiene, --where, --output-file).

These patch the storage boundary so the full main() flow runs against in-memory
data.
"""

import io
import re
import pytest
from click.testing import CliRunner
from unittest.mock import patch

import cloudcat.cli as cli
from cloudcat.cli import main

CSV = b"name,age,status\nJohn,25,active\nJane,30,inactive\nBob,35,active\nAmy,40,active\n"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _patched(func, args):
    runner = CliRunner()
    with patch.object(cli, "get_stream", lambda s, b, p: io.BytesIO(CSV)), \
         patch.object(cli, "get_file_size", lambda s, b, p: len(CSV)):
        return runner.invoke(main, args)


class TestWhereScansFullFile:
    def test_where_returns_matches_beyond_first_window(self):
        # Only 2 of 4 rows are inactive-free; with num_rows=2 we still want the
        # first 2 *matching* rows, which requires scanning the whole file.
        res = _patched(main, [
            "--path", "s3://b/f.csv", "--where", "status=active",
            "--num-rows", "2", "--schema", "dont_show", "--output-format", "csv",
        ])
        assert res.exit_code == 0
        assert "John" in res.stdout and "Bob" in res.stdout
        assert "Jane" not in res.stdout  # inactive, filtered out

    def test_compound_where_errors_clearly(self):
        res = _patched(main, ["--path", "s3://b/f.csv", "--where", "a=1 and b=2"])
        assert res.exit_code == 1
        assert "Compound conditions" in res.output


class TestOutputFile:
    def test_writes_clean_data_and_keeps_stdout_empty(self, tmp_path):
        out = tmp_path / "out.csv"
        res = _patched(main, [
            "--path", "s3://b/f.csv", "--output-file", str(out),
            "--schema", "dont_show", "--output-format", "csv",
        ])
        assert res.exit_code == 0
        # stdout carries no data rows (only stderr diagnostics, if any)
        assert "John" not in res.stdout
        content = out.read_text()
        assert content.startswith("name,age,status")
        assert "John" in content
        # No ANSI escape codes leak into the file
        assert ANSI_RE.search(content) is None


class TestColorControl:
    def test_no_color_flag_strips_ansi(self):
        res = _patched(main, [
            "--path", "s3://b/f.csv", "--no-color",
            "--schema", "show", "--output-format", "table",
        ])
        assert res.exit_code == 0
        assert ANSI_RE.search(res.output) is None


class TestInputValidation:
    def test_negative_offset_rejected(self):
        res = CliRunner().invoke(main, ["--path", "s3://b/f.csv", "--offset", "-1"])
        assert res.exit_code != 0
        assert "Invalid value" in res.output

    def test_negative_num_rows_rejected(self):
        res = CliRunner().invoke(main, ["--path", "s3://b/f.csv", "--num-rows", "-5"])
        assert res.exit_code != 0
        assert "Invalid value" in res.output


class TestSchemaOnlyToStdout:
    def test_schema_only_prints_schema_to_stdout(self):
        res = _patched(main, [
            "--path", "s3://b/f.csv", "--input-format", "csv", "--schema", "schema_only",
        ])
        assert res.exit_code == 0
        assert "Schema:" in res.stdout  # schema is the requested output -> stdout
        assert "John" not in res.output  # no data rows


class TestCountNonNumeric:
    def test_unknown_count_does_not_crash_formatting(self):
        # get_record_count returns a string for unsupported/uninstalled formats;
        # the display must not raise a format error.
        res = CliRunner()
        with patch.object(cli, "get_stream", lambda s, b, p: io.BytesIO(CSV)), \
             patch.object(cli, "get_file_size", lambda s, b, p: len(CSV)), \
             patch.object(cli, "get_record_count", return_value="Unknown (fastavro not installed)"):
            result = res.invoke(main, [
                "--path", "s3://b/f.csv", "--input-format", "csv",
                "--count", "--schema", "dont_show",
            ])
        assert result.exit_code == 0
        assert "Unknown (fastavro not installed)" in result.output
