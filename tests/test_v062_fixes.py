"""Tests for the 0.6.2 fixes: mixed-directory inference and completion setup."""

import os
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from cloudcat.cli import main, _find_nested_tables
import cloudcat.completion as completion

runner = CliRunner()


# --------------------------------------------------------------------------
# directory inference (bug 1)
# --------------------------------------------------------------------------

class TestNestedTableDetection:
    def test_finds_delta_and_iceberg_roots(self):
        files = [
            ("/lake/orders_delta/_delta_log/000.json", 10),
            ("/lake/orders_delta/part-0.parquet", 10),
            ("/lake/shop/orders/metadata/00001-abc.metadata.json", 10),
            ("/lake/shop/orders/data/f.parquet", 10),
            ("/lake/catalog.db", 10),
        ]
        tables = _find_nested_tables(files)
        assert tables == {
            "/lake/orders_delta": "delta",
            "/lake/shop/orders": "iceberg",
        }

    def test_hadoop_style_iceberg_metadata(self):
        files = [("/w/t/metadata/v3.metadata.json", 5)]
        assert _find_nested_tables(files) == {"/w/t": "iceberg"}

    def test_plain_files_are_not_tables(self):
        files = [("/data/a.parquet", 5), ("/data/metadata/notes.txt", 5)]
        assert _find_nested_tables(files) == {}


class TestMixedDirectoryCli:
    def test_directory_of_tables_gets_guidance(self, tmp_path):
        deltalake = pytest.importorskip("deltalake")
        import pandas as pd
        from deltalake import write_deltalake
        write_deltalake(str(tmp_path / "orders_delta"),
                        pd.DataFrame({"a": [1, 2]}))
        # a second, fake iceberg table (detection only needs the marker file)
        meta = tmp_path / "shop" / "orders" / "metadata"
        meta.mkdir(parents=True)
        (meta / "00001-abc.metadata.json").write_text("{}")
        (tmp_path / "catalog.db").write_text("sqlite")

        res = runner.invoke(main, [str(tmp_path) + "/", "--no-color"])
        assert res.exit_code == 1
        assert "contains lakehouse table(s)" in res.output
        assert "orders_delta" in res.output
        assert "shop/orders" in res.output
        assert "Point at a table root" in res.output

    def test_unknown_extensions_are_skipped_for_inference(self, tmp_path):
        (tmp_path / "catalog.db").write_text("junk")
        (tmp_path / "data.csv").write_text("a\n1\n2\n")
        res = runner.invoke(main, [str(tmp_path) + "/", "--no-color",
                                   "-s", "dont_show", "-o", "csv"])
        assert res.exit_code == 0
        assert "1" in res.stdout and "2" in res.stdout

    def test_only_unknown_extensions_gives_clear_error(self, tmp_path):
        (tmp_path / "catalog.db").write_text("junk")
        (tmp_path / "state.lock").write_text("x")
        res = runner.invoke(main, [str(tmp_path) + "/", "--no-color"])
        assert res.exit_code == 1
        assert "no files with a recognized data format" in res.output
        assert "catalog.db" in res.output
        assert "--input-format" in res.output

    def test_explicit_format_still_forces_reading(self, tmp_path):
        (tmp_path / "catalog.db").write_text("junk")
        (tmp_path / "data.csv").write_text("a\n1\n")
        res = runner.invoke(main, [str(tmp_path) + "/", "--no-color", "-i", "csv",
                                   "-s", "dont_show", "-o", "csv"])
        assert res.exit_code == 0


# --------------------------------------------------------------------------
# completion hardening (bug 2)
# --------------------------------------------------------------------------

class TestS3RegionFallback:
    def test_client_gets_a_region_when_none_configured(self, monkeypatch, tmp_path):
        for var in ("AWS_DEFAULT_REGION", "AWS_REGION", "AWS_PROFILE"):
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "none"))
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "none"))
        client = completion._s3_client(None)
        assert client.meta.region_name == "us-east-1"


class TestZshHardening:
    def test_emitted_script_bootstraps_compinit(self):
        res = runner.invoke(main, ["--completion", "zsh"])
        assert res.exit_code == 0
        assert "typeset -f compdef" in res.stdout
        assert "compinit" in res.stdout


class TestInstallCompletion:
    def test_zsh_install_is_idempotent(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        first = runner.invoke(main, ["--install-completion", "zsh"])
        second = runner.invoke(main, ["--install-completion", "zsh"])
        assert first.exit_code == 0 and second.exit_code == 0
        content = (tmp_path / ".zshrc").read_text()
        assert content.count('eval "$(cloudcat --completion zsh)"') == 1
        assert "already set up" in second.output

    def test_bash_install_includes_wordbreaks_fix(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        res = runner.invoke(main, ["--install-completion", "bash"])
        assert res.exit_code == 0
        content = (tmp_path / ".bashrc").read_text()
        assert "COMP_WORDBREAKS" in content
        assert 'eval "$(cloudcat --completion bash)"' in content

    def test_fish_install_creates_config_dir(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        res = runner.invoke(main, ["--install-completion", "fish"])
        assert res.exit_code == 0
        assert "cloudcat --completion fish | source" in \
            (tmp_path / ".config" / "fish" / "config.fish").read_text()


class TestCompletionDebugLog:
    def test_failures_are_logged_when_enabled(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CLOUDCAT_COMPLETE_DEBUG", "1")
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        from click.shell_completion import ShellComplete
        with patch.object(completion, "_list_s3_buckets", side_effect=RuntimeError("boom")):
            sc = ShellComplete(main, {}, "cloudcat", "_CLOUDCAT_COMPLETE")
            assert sc.get_completions([], "s3://") == []
        log = tmp_path / "cloudcat" / "completion.log"
        assert log.exists()
        assert "boom" in log.read_text()

    def test_silent_by_default(self, tmp_path, monkeypatch):
        monkeypatch.delenv("CLOUDCAT_COMPLETE_DEBUG", raising=False)
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        from click.shell_completion import ShellComplete
        with patch.object(completion, "_list_s3_buckets", side_effect=RuntimeError("boom")):
            sc = ShellComplete(main, {}, "cloudcat", "_CLOUDCAT_COMPLETE")
            sc.get_completions([], "s3://")
        assert not (tmp_path / "cloudcat" / "completion.log").exists()
