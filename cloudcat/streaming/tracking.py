"""Stream wrapper for tracking bytes read."""

from typing import BinaryIO, Optional, Iterator
import io

from .stats import StreamingStats


class BytesTrackingStream:
    """Wraps a file-like object to track bytes read.

    This wrapper intercepts read operations to count how many bytes
    are actually read from the underlying stream, enabling efficiency
    reporting.

    Attributes:
        stats: StreamingStats instance to update with read counts.
    """

    def __init__(self, stream: BinaryIO, stats: StreamingStats):
        """Initialize the tracking stream.

        Args:
            stream: Underlying file-like object to wrap.
            stats: StreamingStats instance for tracking.
        """
        self._stream = stream
        self.stats = stats

    def read(self, n: int = -1) -> bytes:
        """Read and track bytes from the stream.

        Args:
            n: Maximum number of bytes to read. -1 for all.

        Returns:
            Bytes read from the stream.
        """
        data = self._stream.read(n)
        self.stats.add_bytes(len(data))
        return data

    def readline(self, limit: int = -1) -> bytes:
        """Read and track a line from the stream.

        Args:
            limit: Maximum bytes to read. -1 for no limit.

        Returns:
            A line from the stream.
        """
        if hasattr(self._stream, 'readline'):
            if limit == -1:
                line = self._stream.readline()
            else:
                line = self._stream.readline(limit)
        else:
            # Fallback for streams without readline
            line = b''
            while True:
                char = self._stream.read(1)
                if not char:
                    break
                line += char
                if char == b'\n':
                    break
                if limit != -1 and len(line) >= limit:
                    break
        self.stats.add_bytes(len(line))
        return line

    def readlines(self, hint: int = -1) -> list:
        """Read and track all lines from the stream.

        Args:
            hint: Approximate number of bytes to read.

        Returns:
            List of lines from the stream.
        """
        lines = []
        bytes_read = 0
        while True:
            line = self.readline()
            if not line:
                break
            lines.append(line)
            bytes_read += len(line)
            if hint != -1 and bytes_read >= hint:
                break
        return lines

    def __iter__(self) -> Iterator[bytes]:
        """Iterate over lines in the stream."""
        while True:
            line = self.readline()
            if not line:
                break
            yield line

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek in the underlying stream.

        Args:
            offset: Position offset.
            whence: Reference point (0=start, 1=current, 2=end).

        Returns:
            New position in the stream.
        """
        return self._stream.seek(offset, whence)

    def tell(self) -> int:
        """Get current position in the stream.

        Returns:
            Current position.
        """
        return self._stream.tell()

    def close(self) -> None:
        """Close the underlying stream."""
        if hasattr(self._stream, 'close'):
            self._stream.close()

    def readable(self) -> bool:
        """Check if stream is readable."""
        if hasattr(self._stream, 'readable'):
            return self._stream.readable()
        return True

    def seekable(self) -> bool:
        """Check if stream is seekable."""
        if hasattr(self._stream, 'seekable'):
            return self._stream.seekable()
        return hasattr(self._stream, 'seek')

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False

    @property
    def closed(self) -> bool:
        """Check if stream is closed."""
        if hasattr(self._stream, 'closed'):
            return self._stream.closed
        return False


class DecompressingTrackingStream:
    """Wraps a decompressing stream to track compressed bytes read.

    This is used when we have a streaming decompressor that wraps
    a compressed stream. We track bytes on the compressed stream
    before decompression.
    """

    def __init__(
        self,
        decompressor_stream: BinaryIO,
        compressed_stream: 'BytesTrackingStream'
    ):
        """Initialize the decompressing tracking stream.

        Args:
            decompressor_stream: The decompressing stream (e.g., gzip.GzipFile).
            compressed_stream: The underlying BytesTrackingStream on compressed data.
        """
        self._decompressor = decompressor_stream
        self._compressed_stream = compressed_stream

    def read(self, n: int = -1) -> bytes:
        """Read decompressed data.

        Args:
            n: Maximum bytes to read.

        Returns:
            Decompressed bytes.
        """
        return self._decompressor.read(n)

    def readline(self, limit: int = -1) -> bytes:
        """Read a line of decompressed data."""
        if hasattr(self._decompressor, 'readline'):
            return self._decompressor.readline(limit) if limit != -1 else self._decompressor.readline()
        # Fallback
        line = b''
        while True:
            char = self._decompressor.read(1)
            if not char:
                break
            line += char
            if char == b'\n':
                break
            if limit != -1 and len(line) >= limit:
                break
        return line

    def __iter__(self) -> Iterator[bytes]:
        """Iterate over lines."""
        while True:
            line = self.readline()
            if not line:
                break
            yield line

    @property
    def stats(self) -> StreamingStats:
        """Get the stats from the underlying compressed stream."""
        return self._compressed_stream.stats

    def close(self) -> None:
        """Close the decompressor and underlying stream."""
        if hasattr(self._decompressor, 'close'):
            self._decompressor.close()
        self._compressed_stream.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
