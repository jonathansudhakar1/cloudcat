"""Tests for compression detection and decompression."""

import pytest
import io
import gzip
from cloudcat.compression import (
    detect_compression,
    decompress_stream,
    strip_compression_extension,
    HAS_LZ4,
    HAS_ZSTD,
    HAS_SNAPPY,
)


class TestCompressionDetection:
    """Tests for detect_compression function."""

    def test_gzip_detection(self):
        assert detect_compression("data.csv.gz") == "gzip"
        assert detect_compression("data.json.gzip") == "gzip"
        assert detect_compression("path/to/file.CSV.GZ") == "gzip"

    def test_zstd_detection(self):
        assert detect_compression("data.csv.zst") == "zstd"
        assert detect_compression("data.json.zstd") == "zstd"
        assert detect_compression("path/to/file.PARQUET.ZST") == "zstd"

    def test_lz4_detection(self):
        assert detect_compression("data.csv.lz4") == "lz4"
        assert detect_compression("path/to/file.JSON.LZ4") == "lz4"

    def test_snappy_detection(self):
        assert detect_compression("data.csv.snappy") == "snappy"
        assert detect_compression("data.json.snappy") == "snappy"

    def test_parquet_snappy_is_internal_codec(self):
        # For Parquet files, .snappy is an internal codec handled by PyArrow
        # Not external compression - PyArrow reads these directly
        assert detect_compression("path/to/file.parquet.snappy") is None
        assert detect_compression("data.PARQUET.SNAPPY") is None

    def test_orc_snappy_is_internal_codec(self):
        # For ORC files, .snappy is an internal codec handled by PyArrow
        assert detect_compression("path/to/file.orc.snappy") is None
        assert detect_compression("data.ORC.SNAPPY") is None

    def test_bz2_detection(self):
        assert detect_compression("data.csv.bz2") == "bz2"
        assert detect_compression("path/to/file.JSON.BZ2") == "bz2"

    def test_no_compression(self):
        assert detect_compression("data.csv") is None
        assert detect_compression("data.json") is None
        assert detect_compression("data.parquet") is None
        assert detect_compression("path/to/file.txt") is None

    def test_compression_in_path(self):
        # Compression extension should only be detected at the end
        assert detect_compression("gz/data.csv") is None
        assert detect_compression("zst_folder/file.json") is None


class TestStripCompressionExtension:
    """Tests for strip_compression_extension function."""

    def test_strip_gzip(self):
        assert strip_compression_extension("data.csv.gz") == "data.csv"
        assert strip_compression_extension("data.json.gzip") == "data.json"

    def test_strip_zstd(self):
        assert strip_compression_extension("data.csv.zst") == "data.csv"
        assert strip_compression_extension("data.json.zstd") == "data.json"

    def test_strip_lz4(self):
        assert strip_compression_extension("data.csv.lz4") == "data.csv"

    def test_strip_snappy(self):
        assert strip_compression_extension("data.csv.snappy") == "data.csv"
        assert strip_compression_extension("data.json.snappy") == "data.json"

    def test_strip_parquet_snappy(self):
        # For .parquet.snappy, strip .snappy to get .parquet format
        # (snappy is internal codec, not external compression)
        assert strip_compression_extension("data.parquet.snappy") == "data.parquet"
        assert strip_compression_extension("path/to/file.PARQUET.SNAPPY") == "path/to/file.PARQUET"

    def test_strip_orc_snappy(self):
        # For .orc.snappy, strip .snappy to get .orc format
        assert strip_compression_extension("data.orc.snappy") == "data.orc"
        assert strip_compression_extension("path/to/file.ORC.SNAPPY") == "path/to/file.ORC"

    def test_strip_bz2(self):
        assert strip_compression_extension("data.csv.bz2") == "data.csv"

    def test_no_compression_unchanged(self):
        assert strip_compression_extension("data.csv") == "data.csv"
        assert strip_compression_extension("data.json") == "data.json"
        assert strip_compression_extension("path/to/file.parquet") == "path/to/file.parquet"

    def test_preserves_path(self):
        assert strip_compression_extension("path/to/data.csv.gz") == "path/to/data.csv"


class TestDecompressStream:
    """Tests for decompress_stream function."""

    def test_gzip_decompression(self):
        # Create gzip compressed data
        original = b"Hello, World!"
        compressed = gzip.compress(original)
        stream = io.BytesIO(compressed)

        result = decompress_stream(stream, "gzip")
        assert result.read() == original

    def test_gzip_decompression_from_bytes(self):
        original = b"Test data for compression"
        compressed = gzip.compress(original)

        result = decompress_stream(compressed, "gzip")
        assert result.read() == original

    def test_bz2_decompression(self):
        import bz2
        original = b"Hello, BZ2 World!"
        compressed = bz2.compress(original)
        stream = io.BytesIO(compressed)

        result = decompress_stream(stream, "bz2")
        assert result.read() == original

    @pytest.mark.skipif(not HAS_ZSTD, reason="zstandard not installed")
    def test_zstd_decompression(self):
        import zstandard as zstd
        original = b"Hello, Zstandard World!"
        cctx = zstd.ZstdCompressor()
        compressed = cctx.compress(original)
        stream = io.BytesIO(compressed)

        result = decompress_stream(stream, "zstd")
        assert result.read() == original

    @pytest.mark.skipif(not HAS_LZ4, reason="lz4 not installed")
    def test_lz4_decompression(self):
        import lz4.frame as lz4
        original = b"Hello, LZ4 World!"
        compressed = lz4.compress(original)
        stream = io.BytesIO(compressed)

        result = decompress_stream(stream, "lz4")
        assert result.read() == original

    @pytest.mark.skipif(not HAS_SNAPPY, reason="python-snappy not installed")
    def test_snappy_decompression(self):
        import snappy
        original = b"Hello, Snappy World!"
        compressed = snappy.compress(original)
        stream = io.BytesIO(compressed)

        result = decompress_stream(stream, "snappy")
        assert result.read() == original

    def test_missing_zstd_raises_error(self):
        if HAS_ZSTD:
            pytest.skip("zstandard is installed")

        stream = io.BytesIO(b"fake data")
        with pytest.raises(ValueError, match="zstandard package is required"):
            decompress_stream(stream, "zstd")

    def test_missing_lz4_raises_error(self):
        if HAS_LZ4:
            pytest.skip("lz4 is installed")

        stream = io.BytesIO(b"fake data")
        with pytest.raises(ValueError, match="lz4 package is required"):
            decompress_stream(stream, "lz4")

    def test_missing_snappy_raises_error(self):
        if HAS_SNAPPY:
            pytest.skip("python-snappy is installed")

        stream = io.BytesIO(b"fake data")
        with pytest.raises(ValueError, match="python-snappy package is required"):
            decompress_stream(stream, "snappy")
