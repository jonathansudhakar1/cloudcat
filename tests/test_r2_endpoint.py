"""Tests for r2:// (Cloudflare R2) and custom S3-compatible endpoints.

A real S3-compatible server (moto) runs locally, standing in for R2/MinIO —
so these exercise the genuine boto3 and pyarrow endpoint paths, not mocks.
"""

import io
import os

import pandas as pd
import pytest
from click.testing import CliRunner

moto_server = pytest.importorskip("moto.server", reason="moto[server] not installed")

from cloudcat.cli import main
from cloudcat.config import cloud_config
from cloudcat.storage.base import parse_cloud_path
import cloudcat.completion as completion

runner = CliRunner()


@pytest.fixture(scope="module")
def s3_server():
    """A live S3-compatible endpoint with seeded data."""
    server = moto_server.ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"

    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")

    import boto3
    client = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1")
    client.create_bucket(Bucket="r2-bucket")
    client.put_object(Bucket="r2-bucket", Key="data/orders.csv",
                      Body=b"id,region\n1,eu\n2,us\n3,eu\n")

    import pyarrow as pa
    import pyarrow.parquet as pq
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(
        pd.DataFrame({"n": range(100), "tag": ["hit" if i < 5 else "miss" for i in range(100)]})
    ), buf)
    client.put_object(Bucket="r2-bucket", Key="data/events.parquet", Body=buf.getvalue())

    yield endpoint
    server.stop()


@pytest.fixture(autouse=True)
def _clean_config():
    yield
    cloud_config.reset()


class TestParsing:
    def test_r2_scheme_maps_to_s3_service(self):
        assert parse_cloud_path("r2://bucket/key.csv") == ("s3", "bucket", "key.csv")
        assert cloud_config.s3_scheme == "r2"

    def test_s3_scheme_resets_marker(self):
        parse_cloud_path("r2://bucket/key.csv")
        parse_cloud_path("s3://bucket/key.csv")
        assert cloud_config.s3_scheme == "s3"


class TestErrors:
    def test_r2_without_endpoint_is_a_clear_error(self):
        res = runner.invoke(main, ["r2://bucket/key.csv", "--no-color"])
        assert res.exit_code == 1
        assert "endpoint" in res.output
        assert "r2.cloudflarestorage.com" in res.output


class TestEndToEnd:
    def test_csv_read_via_r2_scheme(self, s3_server):
        res = runner.invoke(main, ["r2://r2-bucket/data/orders.csv",
                                   "--endpoint-url", s3_server,
                                   "--no-color", "-s", "dont_show", "-o", "csv"])
        assert res.exit_code == 0, res.output
        assert "1,eu" in res.stdout and "3,eu" in res.stdout

    def test_parquet_native_fs_with_pushdown(self, s3_server):
        # Exercises pyarrow S3FileSystem endpoint_override (http scheme).
        res = runner.invoke(main, ["r2://r2-bucket/data/events.parquet",
                                   "--endpoint-url", s3_server,
                                   "--no-color", "-s", "dont_show", "-o", "json",
                                   "-w", "tag=hit", "-n", "3", "-y"])
        assert res.exit_code == 0, res.output
        rows = [l for l in res.stdout.splitlines() if l.startswith("{")]
        assert len(rows) == 3
        assert all('"tag":"hit"' in r for r in rows)

    def test_count_from_metadata(self, s3_server):
        res = runner.invoke(main, ["r2://r2-bucket/data/events.parquet",
                                   "--endpoint-url", s3_server,
                                   "--no-color", "-s", "schema_only", "--count", "-y"])
        assert res.exit_code == 0, res.output
        assert "Total records: 100" in res.output

    def test_plain_s3_scheme_also_honors_endpoint(self, s3_server):
        # MinIO/Wasabi style: s3:// + --endpoint-url
        res = runner.invoke(main, ["s3://r2-bucket/data/orders.csv",
                                   "--endpoint-url", s3_server,
                                   "--no-color", "-s", "dont_show", "-o", "csv"])
        assert res.exit_code == 0, res.output
        assert "2,us" in res.stdout

    def test_env_var_endpoint(self, s3_server, monkeypatch):
        monkeypatch.setenv("AWS_ENDPOINT_URL_S3", s3_server)
        res = runner.invoke(main, ["r2://r2-bucket/data/orders.csv",
                                   "--no-color", "-s", "dont_show", "-o", "csv"])
        assert res.exit_code == 0, res.output
        assert "1,eu" in res.stdout

    def test_directory_listing_via_endpoint(self, s3_server):
        res = runner.invoke(main, ["r2://r2-bucket/data/", "-i", "csv",
                                   "--endpoint-url", s3_server,
                                   "--no-color", "-s", "dont_show", "-o", "csv", "-y"])
        assert res.exit_code == 0, res.output
        assert "1,eu" in res.stdout


class TestCompletion:
    def _complete(self, args, incomplete):
        from click.shell_completion import ShellComplete
        sc = ShellComplete(main, {}, "cloudcat", "_CLOUDCAT_COMPLETE")
        return sc.get_completions(args, incomplete)

    def test_r2_bucket_completion_with_endpoint(self, s3_server):
        items = self._complete(["--endpoint-url", s3_server], "r2://")
        assert [i.value for i in items] == ["r2://r2-bucket/"]

    def test_r2_prefix_completion_with_endpoint(self, s3_server):
        items = self._complete(["--endpoint-url", s3_server], "r2://r2-bucket/data/")
        values = [i.value for i in items]
        assert "r2://r2-bucket/data/orders.csv" in values
        assert "r2://r2-bucket/data/events.parquet" in values

    def test_r2_without_endpoint_completes_to_nothing(self, monkeypatch):
        monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        assert self._complete([], "r2://") == []

    def test_r2_scheme_hint_offered(self):
        values = [i.value for i in self._complete([], "r2")]
        assert "r2://" in values


class TestConfigFile:
    def test_endpoint_url_from_config_profile(self, s3_server, tmp_path, monkeypatch):
        cfg = tmp_path / "config.toml"
        cfg.write_text(f'[profiles.r2]\nendpoint-url = "{s3_server}"\n')
        monkeypatch.setenv("CLOUDCAT_CONFIG", str(cfg))
        res = runner.invoke(main, ["r2://r2-bucket/data/orders.csv",
                                   "--config-profile", "r2",
                                   "--no-color", "-s", "dont_show", "-o", "csv"])
        assert res.exit_code == 0, res.output
        assert "1,eu" in res.stdout
