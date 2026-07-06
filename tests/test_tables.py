"""Tests for lakehouse table support (Delta Lake + Apache Iceberg).

Fixtures write real tables with the actual libraries, so the full stack —
detection, snapshot resolution, pushdown, CLI integration — is exercised
without mocks. Skipped cleanly when the optional deps are absent.
"""

import re
import pytest
import pandas as pd
import pyarrow as pa
from click.testing import CliRunner

from cloudcat.cli import main
from cloudcat.tables import detect_table_format, table_row_count

deltalake = pytest.importorskip("deltalake", reason="delta extra not installed")
pyiceberg = pytest.importorskip("pyiceberg", reason="iceberg extra not installed")
# Writing Iceberg fixtures uses a sqlite catalog; runtime reads don't need it.
pytest.importorskip("sqlalchemy", reason="pyiceberg[sql-sqlite] needed to write test fixtures")

runner = CliRunner()


# --------------------------------------------------------------------------
# fixtures
# --------------------------------------------------------------------------

@pytest.fixture
def delta_table(tmp_path):
    """A Delta table with two versions: only the latest snapshot is live."""
    from deltalake import write_deltalake
    path = tmp_path / "sales_delta"
    df = pd.DataFrame({
        "id": range(200),
        "region": ["eu" if i % 2 else "us" for i in range(200)],
        "amount": [float(i) for i in range(200)],
    })
    write_deltalake(str(path), df)
    # Overwrite so stale files exist on disk but are dead in the log.
    # reset_index: a filtered frame's gappy index becomes an extra
    # __index_level_0__ column on older pandas/deltalake stacks.
    write_deltalake(str(path), df[df.id < 50].reset_index(drop=True), mode="overwrite")
    return path


@pytest.fixture
def iceberg_table(tmp_path):
    """An Iceberg table written via a throwaway sqlite catalog."""
    from pyiceberg.catalog.sql import SqlCatalog
    catalog = SqlCatalog(
        "test", uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}/warehouse",
    )
    catalog.create_namespace("ns")
    table = catalog.create_table(
        "ns.sales",
        schema=pa.schema([("id", pa.int64()), ("region", pa.string()), ("amount", pa.float64())]),
    )
    table.append(pa.table({
        "id": list(range(300)),
        "region": ["eu" if i % 2 else "us" for i in range(300)],
        "amount": [float(i) for i in range(300)],
    }))
    # Derive the table root from the API rather than guessing the
    # warehouse layout (it differs between pyiceberg versions).
    location = table.location()
    assert location.startswith("file://")
    from pathlib import Path
    return Path(location[len("file://"):])


def _table_dir(fixture_path):
    return str(fixture_path) + "/"


# --------------------------------------------------------------------------
# detection & counts
# --------------------------------------------------------------------------

class TestDetection:
    def test_detects_delta(self, delta_table):
        assert detect_table_format("local", "", _table_dir(delta_table)) == "delta"

    def test_detects_iceberg(self, iceberg_table):
        assert detect_table_format("local", "", _table_dir(iceberg_table)) == "iceberg"

    def test_plain_directory_is_not_a_table(self, tmp_path):
        (tmp_path / "a.csv").write_text("x\n1\n")
        assert detect_table_format("local", "", str(tmp_path) + "/") is None

    def test_missing_directory_is_not_a_table(self, tmp_path):
        assert detect_table_format("local", "", str(tmp_path / "nope") + "/") is None


class TestCounts:
    def test_delta_count_reflects_latest_version_only(self, delta_table):
        # 200 rows written, overwritten with 50 — dead files must not count.
        assert table_row_count("local", "", _table_dir(delta_table), "delta") == 50

    def test_iceberg_count_from_snapshot_summary(self, iceberg_table):
        assert table_row_count("local", "", _table_dir(iceberg_table), "iceberg") == 300


# --------------------------------------------------------------------------
# CLI end-to-end
# --------------------------------------------------------------------------

class TestDeltaCli:
    def test_preview_reads_only_live_snapshot(self, delta_table):
        res = runner.invoke(main, [_table_dir(delta_table), "-n", "0", "-o", "json",
                                   "-s", "dont_show", "-y", "--no-color"])
        assert res.exit_code == 0
        rows = [l for l in res.stdout.splitlines() if l.startswith("{")]
        assert len(rows) == 50  # overwritten snapshot, not 200

    def test_schema_comes_from_table_metadata(self, delta_table):
        res = runner.invoke(main, [_table_dir(delta_table), "-s", "schema_only", "-y", "--no-color"])
        assert res.exit_code == 0
        for col in ["id", "region", "amount"]:
            assert col in res.output

    def test_where_pushdown_and_projection(self, delta_table):
        res = runner.invoke(main, [_table_dir(delta_table), "-w", "region=us AND id>=40",
                                   "-c", "id", "-n", "3", "-o", "json",
                                   "-s", "dont_show", "-y", "--no-color"])
        assert res.exit_code == 0
        rows = [l for l in res.stdout.splitlines() if l.startswith("{")]
        assert rows == ['{"id":40}', '{"id":42}', '{"id":44}']
        assert "region" not in rows[0]  # filter column projected away

    def test_where_contains_falls_back_locally(self, delta_table):
        res = runner.invoke(main, [_table_dir(delta_table), "-w", "region contains u",
                                   "-n", "2", "-o", "json", "-s", "dont_show", "-y", "--no-color"])
        assert res.exit_code == 0
        assert "Filtered: 2 matching rows" in res.output

    def test_count_flag(self, delta_table):
        res = runner.invoke(main, [_table_dir(delta_table), "--count", "-s", "schema_only",
                                   "-y", "--no-color"])
        assert res.exit_code == 0
        assert "Total records: 50" in res.output

    def test_explicit_input_format_on_file_errors(self, delta_table):
        log = next((delta_table / "_delta_log").glob("*.json"))
        res = runner.invoke(main, [str(log), "-i", "delta", "--no-color"])
        assert res.exit_code == 1
        assert "table root" in res.output


class TestIcebergCli:
    def test_preview(self, iceberg_table):
        res = runner.invoke(main, [_table_dir(iceberg_table), "-n", "4", "-o", "json",
                                   "-s", "dont_show", "-y", "--no-color"])
        assert res.exit_code == 0
        rows = [l for l in res.stdout.splitlines() if l.startswith("{")]
        assert len(rows) == 4
        assert "Detected iceberg table" in res.output

    def test_where_pushdown_with_offset_pagination(self, iceberg_table):
        res = runner.invoke(main, [_table_dir(iceberg_table), "-w", "id>=100 AND id<110",
                                   "-n", "3", "--offset", "2", "-o", "json",
                                   "-s", "dont_show", "-y", "--no-color"])
        assert res.exit_code == 0
        rows = [l for l in res.stdout.splitlines() if l.startswith("{")]
        ids = [int(re.search(r'"id":(\d+)', r).group(1)) for r in rows]
        assert ids == [102, 103, 104]  # offset pages through matches

    def test_compound_or_filter(self, iceberg_table):
        res = runner.invoke(main, [_table_dir(iceberg_table), "-w", "id<2 or id>=298",
                                   "-n", "0", "-o", "json", "-s", "dont_show", "-y", "--no-color"])
        assert res.exit_code == 0
        rows = [l for l in res.stdout.splitlines() if l.startswith("{")]
        assert len(rows) == 4

    def test_stats_over_table(self, iceberg_table):
        res = runner.invoke(main, [_table_dir(iceberg_table), "--stats", "-n", "0",
                                   "-s", "dont_show", "-y", "--no-color"])
        assert res.exit_code == 0
        assert "distinct" in res.stdout
        assert "region" in res.stdout

    def test_count_flag(self, iceberg_table):
        res = runner.invoke(main, [_table_dir(iceberg_table), "--count", "-s", "schema_only",
                                   "-y", "--no-color"])
        assert res.exit_code == 0
        assert "Total records: 300" in res.output

    def test_missing_columns_error(self, iceberg_table):
        res = runner.invoke(main, [_table_dir(iceberg_table), "-c", "bogus",
                                   "-s", "dont_show", "-y", "--no-color"])
        assert res.exit_code == 1
        assert "None of the requested columns exist" in res.output


class TestOutputIntegration:
    def test_output_file_export(self, delta_table, tmp_path):
        out = tmp_path / "export.csv"
        res = runner.invoke(main, [_table_dir(delta_table), "-n", "0", "-o", "csv",
                                   "-O", str(out), "-s", "dont_show", "-y", "--no-color"])
        assert res.exit_code == 0
        lines = out.read_text().strip().splitlines()
        assert lines[0] == "id,region,amount"
        assert len(lines) == 51  # header + 50 live rows
