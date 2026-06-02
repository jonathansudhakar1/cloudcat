"""Regression tests for directory listing, especially bucket-root prefixes.

Bucket-root paths produce an empty prefix; the listing functions must NOT turn
'' into '/' (which matches no S3/GCS object). These patch the cloud clients so
no network or credentials are needed.
"""

from unittest.mock import MagicMock, patch

from cloudcat.storage import s3 as s3mod
from cloudcat.storage import gcs as gcsmod


class TestS3Listing:
    def _client_with_keys(self, keys):
        client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"Contents": [{"Key": k, "Size": sz} for k, sz in keys]}
        ]
        client.get_paginator.return_value = paginator
        return client, paginator

    def test_bucket_root_uses_empty_prefix(self):
        client, paginator = self._client_with_keys([("a.csv", 10), ("dir/b.csv", 20)])
        with patch.object(s3mod, "get_s3_client", return_value=client):
            result = s3mod.list_s3_directory("bucket", "")
        # Empty prefix stays empty (never '/')
        _, kwargs = paginator.paginate.call_args
        assert kwargs["Prefix"] == ""
        assert ("a.csv", 10) in result

    def test_subdir_prefix_gets_trailing_slash(self):
        client, paginator = self._client_with_keys([("dir/b.csv", 20)])
        with patch.object(s3mod, "get_s3_client", return_value=client):
            s3mod.list_s3_directory("bucket", "dir")
        _, kwargs = paginator.paginate.call_args
        assert kwargs["Prefix"] == "dir/"

    def test_directory_marker_keys_excluded(self):
        client, paginator = self._client_with_keys([("dir/", 0), ("dir/b.csv", 20)])
        with patch.object(s3mod, "get_s3_client", return_value=client):
            result = s3mod.list_s3_directory("bucket", "dir/")
        assert ("dir/", 0) not in result
        assert ("dir/b.csv", 20) in result


class TestGCSListing:
    def _bucket_with_blobs(self, blobs):
        client = MagicMock()
        bucket = MagicMock()
        blob_objs = []
        for name, size in blobs:
            b = MagicMock()
            b.name = name
            b.size = size
            blob_objs.append(b)
        bucket.list_blobs.return_value = blob_objs
        client.bucket.return_value = bucket
        return client, bucket

    def test_bucket_root_uses_empty_prefix(self):
        client, bucket = self._bucket_with_blobs([("a.csv", 10), ("dir/b.csv", 20)])
        with patch.object(gcsmod, "get_gcs_client", return_value=client):
            result = gcsmod.list_gcs_directory("bucket", "")
        _, kwargs = bucket.list_blobs.call_args
        assert kwargs["prefix"] == ""
        assert ("a.csv", 10) in result

    def test_subdir_prefix_gets_trailing_slash(self):
        client, bucket = self._bucket_with_blobs([("dir/b.csv", 20)])
        with patch.object(gcsmod, "get_gcs_client", return_value=client):
            gcsmod.list_gcs_directory("bucket", "dir")
        _, kwargs = bucket.list_blobs.call_args
        assert kwargs["prefix"] == "dir/"
