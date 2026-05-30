"""Unit tests for byte-tracking stream wrappers."""

import io

from cloudcat.streaming.tracking import BytesTrackingStream, DecompressingTrackingStream
from cloudcat.streaming.stats import StreamingStats


class TestBytesTrackingStream:
    def test_read_tracks_bytes(self):
        stats = StreamingStats()
        wrapped = BytesTrackingStream(io.BytesIO(b"abcdef"), stats)
        assert wrapped.read(3) == b"abc"
        assert stats.bytes_read == 3
        assert wrapped.read() == b"def"
        assert stats.bytes_read == 6

    def test_iteration_tracks_all_bytes(self):
        data = b"l1\nl2\nl3\n"
        stats = StreamingStats()
        wrapped = BytesTrackingStream(io.BytesIO(data), stats)
        lines = list(wrapped)
        assert lines == [b"l1\n", b"l2\n", b"l3\n"]
        assert stats.bytes_read == len(data)

    def test_readline(self):
        stats = StreamingStats()
        wrapped = BytesTrackingStream(io.BytesIO(b"first\nsecond\n"), stats)
        assert wrapped.readline() == b"first\n"
        assert stats.bytes_read == 6

    def test_seek_and_tell(self):
        stats = StreamingStats()
        wrapped = BytesTrackingStream(io.BytesIO(b"abcdef"), stats)
        wrapped.read(2)
        assert wrapped.tell() == 2
        wrapped.seek(0)
        assert wrapped.tell() == 0

    def test_context_manager_closes(self):
        stats = StreamingStats()
        underlying = io.BytesIO(b"data")
        with BytesTrackingStream(underlying, stats) as wrapped:
            wrapped.read()
        assert underlying.closed

    def test_seekable_and_readable(self):
        stats = StreamingStats()
        wrapped = BytesTrackingStream(io.BytesIO(b"data"), stats)
        assert wrapped.readable() is True
        assert wrapped.seekable() is True


class TestDecompressingTrackingStream:
    def test_stats_come_from_compressed_stream(self):
        stats = StreamingStats()
        compressed = BytesTrackingStream(io.BytesIO(b"compressed-bytes"), stats)
        # decompressor just echoes here; we only verify stats wiring
        decompressor = io.BytesIO(b"plain\n")
        wrapper = DecompressingTrackingStream(decompressor, compressed)
        assert wrapper.stats is stats
        assert wrapper.read() == b"plain\n"
