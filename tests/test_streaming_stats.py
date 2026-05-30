"""Unit tests for streaming statistics (StreamingStats, format_bytes)."""

import pytest

from cloudcat.streaming.stats import format_bytes, StreamingStats


class TestFormatBytes:
    @pytest.mark.parametrize("n,expected", [
        (-1, "0 B"),
        (0, "0 B"),
        (512, "512 B"),
        (1023, "1023 B"),
        (1024, "1.0 KB"),
        (1536, "1.5 KB"),
        (1024 ** 2, "1.0 MB"),
        (1024 ** 3, "1.0 GB"),
        (1024 ** 4, "1.0 TB"),
        (1024 ** 5, "1.0 PB"),
    ])
    def test_format_bytes(self, n, expected):
        assert format_bytes(n) == expected


class TestStreamingStats:
    def test_percentages_normal(self):
        s = StreamingStats(file_size=1000, bytes_read=100)
        assert s.read_percent == 10.0
        assert s.efficiency_percent == 90.0

    def test_zero_file_size(self):
        s = StreamingStats(file_size=0, bytes_read=0)
        # read_percent defaults to 100 (nothing to save), efficiency 0
        assert s.read_percent == 100.0
        assert s.efficiency_percent == 0.0

    def test_read_exceeds_file_size_is_clamped(self):
        s = StreamingStats(file_size=100, bytes_read=250)
        assert s.read_percent == 100.0
        assert s.efficiency_percent == 0.0

    def test_add_bytes(self):
        s = StreamingStats(file_size=100)
        s.add_bytes(30)
        s.add_bytes(20)
        assert s.bytes_read == 50

    def test_format_report_with_savings(self):
        s = StreamingStats(file_size=1024 * 1024, bytes_read=1024)
        report = s.format_report()
        assert "File size: 1.0 MB" in report
        assert "Data read: 1.0 KB" in report
        assert "%" in report

    def test_format_report_full_read(self):
        s = StreamingStats(file_size=1024, bytes_read=1024)
        report = s.format_report()
        assert "File size: 1.0 KB" in report
        # No percentage shown when the whole file was read
        assert "%" not in report
