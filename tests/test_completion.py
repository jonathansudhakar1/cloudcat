"""Tests for PATH tab-completion (local, scheme hints, cloud listings).

Driven through Click's ShellComplete so the full protocol runs — argument
resolution, partial-flag parsing, and our completer — with the provider
listing seams patched (no network).
"""

from unittest.mock import patch

from click.shell_completion import ShellComplete

from cloudcat.cli import main
import cloudcat.completion as completion


def complete(args, incomplete):
    sc = ShellComplete(main, {}, "cloudcat", "_CLOUDCAT_COMPLETE")
    return sc.get_completions(args, incomplete)


class TestLocalAndSchemes:
    def test_empty_falls_back_to_file_completion(self):
        items = complete([], "")
        assert any(i.type == "file" for i in items)

    def test_relative_path_uses_file_completion(self):
        items = complete([], "data/ex")
        assert [i.type for i in items] == ["file"]

    def test_scheme_prefix_suggests_schemes_plus_files(self):
        items = complete([], "s3")
        values = [i.value for i in items]
        assert "s3://" in values
        assert any(i.type == "file" for i in items)

    def test_gc_prefix_suggests_gcs(self):
        values = [i.value for i in complete([], "gc")]
        assert "gcs://" in values


class TestS3Completion:
    def test_bucket_listing_filters_by_prefix(self):
        with patch.object(completion, "_list_s3_buckets", return_value=["alpha", "beta", "alps"]):
            items = complete([], "s3://al")
        assert [i.value for i in items] == ["s3://alpha/", "s3://alps/"]

    def test_empty_bucket_part_lists_all(self):
        with patch.object(completion, "_list_s3_buckets", return_value=["b1", "b2"]):
            items = complete([], "s3://")
        assert [i.value for i in items] == ["s3://b1/", "s3://b2/"]

    def test_prefix_listing_returns_dirs_and_files(self):
        with patch.object(completion, "_shallow_list_s3",
                          return_value=(["logs/2026/"], ["logs/app.log"])) as lister:
            items = complete([], "s3://bucket/logs/")
        assert [i.value for i in items] == ["s3://bucket/logs/2026/", "s3://bucket/logs/app.log"]
        assert lister.call_args[0][:2] == ("bucket", "logs/")

    def test_profile_flag_reaches_the_lister(self):
        with patch.object(completion, "_list_s3_buckets", return_value=[]) as lister:
            complete(["--profile", "prod"], "s3://")
        assert lister.call_args[0][0] == "prod"

    def test_listing_errors_complete_to_nothing(self):
        with patch.object(completion, "_list_s3_buckets", side_effect=ConnectionError("no creds")):
            assert complete([], "s3://") == []


class TestGcsCompletion:
    def test_bucket_listing(self):
        with patch.object(completion, "_list_gcs_buckets", return_value=["lake"]):
            items = complete([], "gs://l")
        assert [i.value for i in items] == ["gs://lake/"]

    def test_gcs_scheme_variant(self):
        with patch.object(completion, "_shallow_list_gcs",
                          return_value=(["raw/"], [])):
            items = complete([], "gcs://lake/")
        assert [i.value for i in items] == ["gcs://lake/raw/"]

    def test_project_flag_reaches_the_lister(self):
        with patch.object(completion, "_list_gcs_buckets", return_value=[]) as lister:
            complete(["--project", "my-proj"], "gs://")
        assert lister.call_args[0][0] == "my-proj"


class TestAbfssCompletion:
    def test_needs_account_before_completing(self):
        assert complete([], "abfss://cont") == []

    def test_container_listing_with_account(self):
        with patch.object(completion, "_list_abfss_containers", return_value=["data", "logs"]):
            items = complete([], "abfss://d@acct.dfs.core.windows.net")
        assert [i.value for i in items] == ["abfss://data@acct.dfs.core.windows.net/"]

    def test_prefix_listing(self):
        with patch.object(completion, "_shallow_list_abfss",
                          return_value=(["raw/"], ["raw.csv"])):
            items = complete([], "abfss://data@acct.dfs.core.windows.net/")
        values = [i.value for i in items]
        assert "abfss://data@acct.dfs.core.windows.net/raw/" in values
        assert "abfss://data@acct.dfs.core.windows.net/raw.csv" in values


class TestGuardrails:
    def test_results_are_capped(self):
        many = [f"bucket{i:03d}" for i in range(500)]
        with patch.object(completion, "_list_s3_buckets", return_value=many):
            items = complete([], "s3://")
        assert len(items) == completion.LIMIT

    def test_unknown_scheme_completes_to_nothing(self):
        assert complete([], "ftp://host/") == []
