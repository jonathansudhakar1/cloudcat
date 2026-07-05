"""Tests for the config file (+profiles), --stats, and --completion."""

import pytest
from click.testing import CliRunner

from cloudcat.cli import main, _column_stats
from cloudcat.user_config import load_user_config
import pandas as pd

runner = CliRunner()


def _write_config(tmp_path, monkeypatch, body):
    cfg = tmp_path / "config.toml"
    cfg.write_text(body)
    monkeypatch.setenv("CLOUDCAT_CONFIG", str(cfg))
    return cfg


def _write_csv(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("name,age\nAlice,30\nBob,25\nCarol,35\n")
    return f


class TestUserConfig:
    def test_no_file_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CLOUDCAT_CONFIG", str(tmp_path / "missing.toml"))
        assert load_user_config() == {}

    def test_top_level_defaults(self, tmp_path, monkeypatch):
        _write_config(tmp_path, monkeypatch, 'num-rows = 7\noutput-format = "csv"\n')
        assert load_user_config() == {"num_rows": 7, "output_format": "csv"}

    def test_profile_overrides_top_level(self, tmp_path, monkeypatch):
        _write_config(tmp_path, monkeypatch,
                      'num-rows = 7\n[profiles.p]\nnum-rows = 3\n')
        assert load_user_config("p")["num_rows"] == 3

    def test_unknown_profile_raises(self, tmp_path, monkeypatch):
        _write_config(tmp_path, monkeypatch, "[profiles.a]\nnum-rows = 1\n")
        with pytest.raises(ValueError, match="Profile 'b' not found"):
            load_user_config("b")

    def test_unknown_key_warns_but_loads(self, tmp_path, monkeypatch, capsys):
        _write_config(tmp_path, monkeypatch, "num-rows = 2\nbogus-key = 1\n")
        cfg = load_user_config()
        assert cfg == {"num_rows": 2}
        assert "bogus-key" in capsys.readouterr().err

    def test_cli_uses_config_defaults(self, tmp_path, monkeypatch):
        _write_config(tmp_path, monkeypatch,
                      'num-rows = 2\noutput-format = "csv"\nschema = "dont_show"\n')
        f = _write_csv(tmp_path)
        res = runner.invoke(main, [str(f), "--no-color"])
        assert res.exit_code == 0
        assert "Alice" in res.stdout and "Bob" in res.stdout
        assert "Carol" not in res.stdout  # num-rows = 2 applied

    def test_explicit_flag_beats_config(self, tmp_path, monkeypatch):
        _write_config(tmp_path, monkeypatch,
                      'num-rows = 2\noutput-format = "csv"\nschema = "dont_show"\n')
        f = _write_csv(tmp_path)
        res = runner.invoke(main, [str(f), "--no-color", "-n", "3"])
        assert res.exit_code == 0
        assert "Carol" in res.stdout

    def test_config_profile_flag(self, tmp_path, monkeypatch):
        _write_config(tmp_path, monkeypatch,
                      'num-rows = 1\noutput-format = "csv"\nschema = "dont_show"\n'
                      '[profiles.wide]\nnum-rows = 0\n')
        f = _write_csv(tmp_path)
        res = runner.invoke(main, [str(f), "--no-color", "--config-profile", "wide"])
        assert res.exit_code == 0
        assert "Carol" in res.stdout  # profile overrode num-rows to 0 (all)


class TestColumnStats:
    def test_basic_profile(self):
        df = pd.DataFrame({"a": [1, 2, None], "b": ["x", "x", "y"]})
        out = _column_stats(df)
        a = out[out["column"] == "a"].iloc[0]
        assert a["non_null"] == 2 and a["nulls"] == 1
        assert a["min"] == 1.0 and a["max"] == 2.0
        b = out[out["column"] == "b"].iloc[0]
        assert b["distinct"] == 2

    def test_unhashable_and_mixed_columns_dont_crash(self):
        df = pd.DataFrame({"j": [[1], [2]], "m": [1, "x"]})
        out = _column_stats(df)
        assert len(out) == 2  # no TypeError

    def test_stats_flag_cli(self, tmp_path):
        f = _write_csv(tmp_path)
        res = runner.invoke(main, [str(f), "--no-color", "--stats",
                                   "-n", "0", "--schema", "dont_show"])
        assert res.exit_code == 0
        assert "distinct" in res.stdout
        assert "Column statistics over 3 retrieved rows" in res.output
        # stats view replaces the data rows
        assert "Alice" not in res.stdout.splitlines()[0]


class TestCompletion:
    @pytest.mark.parametrize("shell", ["bash", "zsh", "fish"])
    def test_completion_script_prints(self, shell):
        res = runner.invoke(main, ["--completion", shell])
        assert res.exit_code == 0
        assert "cloudcat" in res.stdout.lower()
        assert "_CLOUDCAT_COMPLETE" in res.stdout or "cloudcat" in res.stdout
