"""Tests for the bundled agent skill and --install-skill."""

import json
import os
from pathlib import Path

from click.testing import CliRunner

from cloudcat.cli import main

runner = CliRunner()

REPO_ROOT = Path(__file__).resolve().parent.parent


class TestSkillBundle:
    def test_bundled_copy_in_sync_with_repo_copy(self):
        """cloudcat/data/SKILL.md ships in the wheel; skills/cloudcat/SKILL.md
        is the repo/marketplace copy. They must never drift."""
        repo_copy = (REPO_ROOT / "skills" / "cloudcat" / "SKILL.md").read_text()
        bundled = (REPO_ROOT / "cloudcat" / "data" / "SKILL.md").read_text()
        assert repo_copy == bundled

    def test_skill_has_valid_frontmatter(self):
        content = (REPO_ROOT / "cloudcat" / "data" / "SKILL.md").read_text()
        assert content.startswith("---\n")
        assert "name: cloudcat" in content
        assert "description: Use when" in content

    def test_marketplace_manifests_are_valid_json(self):
        marketplace = json.loads((REPO_ROOT / ".claude-plugin" / "marketplace.json").read_text())
        plugin = json.loads((REPO_ROOT / ".claude-plugin" / "plugin.json").read_text())
        assert marketplace["plugins"][0]["name"] == "cloudcat"
        assert plugin["name"] == "cloudcat"


class TestInstallSkill:
    def test_print_writes_skill_to_stdout(self):
        res = runner.invoke(main, ["--install-skill", "print"])
        assert res.exit_code == 0
        assert res.stdout.startswith("---\n")
        assert "cloudcat — fast data scanning" in res.stdout

    def test_claude_target_writes_under_home(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        res = runner.invoke(main, ["--install-skill", "claude"])
        assert res.exit_code == 0
        dest = tmp_path / ".claude" / "skills" / "cloudcat" / "SKILL.md"
        assert dest.exists()
        assert "name: cloudcat" in dest.read_text()
        assert str(dest) in res.output

    def test_codex_target_writes_under_home(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        res = runner.invoke(main, ["--install-skill", "codex"])
        assert res.exit_code == 0
        assert (tmp_path / ".codex" / "skills" / "cloudcat" / "SKILL.md").exists()

    def test_claude_project_target_writes_in_cwd(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        res = runner.invoke(main, ["--install-skill", "claude-project"])
        assert res.exit_code == 0
        assert (tmp_path / ".claude" / "skills" / "cloudcat" / "SKILL.md").exists()

    def test_install_skill_needs_no_path_argument(self, tmp_path, monkeypatch):
        # Eager option: must work without the otherwise-required PATH.
        monkeypatch.setenv("HOME", str(tmp_path))
        res = runner.invoke(main, ["--install-skill", "claude"])
        assert res.exit_code == 0
        assert "Missing PATH" not in res.output
