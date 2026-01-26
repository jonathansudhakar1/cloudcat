"""Compression detection and decompression utilities."""

import bz2
import gzip
import io
from typing import Optional, Union, BinaryIO, Tuple

from .config import COMPRESSION_EXTENSIONS

# Optional compression library imports
try:
    import lz4.frame as lz4
    HAS_LZ4 = True
except ImportError:
    lz4 = None
    HAS_LZ4 = False

try:
    import zstandard as zstd
    HAS_ZSTD = True
except ImportError:
    zstd = None
    HAS_ZSTD = False

try:
    import snappy
    HAS_SNAPPY = True
except ImportError:
    snappy = None
    HAS_SNAPPY = False


def detect_compression(path: str) -> Optional[str]:
    """Detect compression type from file extension.

    Args:
        path: File path to check for compression extension.

    Returns:
        Compression type string ('gzip', 'zstd', 'lz4', 'snappy', 'bz2') or None.
    """
    path_lower = path.lower()
    if path_lower.endswith('.gz') or path_lower.endswith('.gzip'):
        return 'gzip'
    elif path_lower.endswith('.zst') or path_lower.endswith('.zstd'):
        return 'zstd'
    elif path_lower.endswith('.lz4'):
        return 'lz4'
    elif path_lower.endswith('.snappy'):
        return 'snappy'
    elif path_lower.endswith('.bz2'):
        return 'bz2'
    return None


def decompress_stream(stream: Union[BinaryIO, bytes], compression: str) -> io.BytesIO:
    """Decompress a stream based on compression type.

    Args:
        stream: File-like object or bytes to decompress.
        compression: Compression type ('gzip', 'zstd', 'lz4', 'snappy', 'bz2').

    Returns:
        BytesIO object containing decompressed data.

    Raises:
        ValueError: If required compression library is not installed.
    """
    if hasattr(stream, 'read'):
        data = stream.read()
    else:
        data = stream

    if compression == 'gzip':
        decompressed = gzip.decompress(data)
    elif compression == 'zstd':
        if not HAS_ZSTD:
            raise ValueError("zstandard package is required for .zst files. Install with: pip install zstandard")
        dctx = zstd.ZstdDecompressor()
        decompressed = dctx.decompress(data)
    elif compression == 'lz4':
        if not HAS_LZ4:
            raise ValueError("lz4 package is required for .lz4 files. Install with: pip install lz4")
        decompressed = lz4.decompress(data)
    elif compression == 'snappy':
        if not HAS_SNAPPY:
            raise ValueError("python-snappy package is required for .snappy files. Install with: pip install python-snappy")
        decompressed = snappy.decompress(data)
    elif compression == 'bz2':
        decompressed = bz2.decompress(data)
    else:
        # No compression or unknown - return original as BytesIO
        if hasattr(stream, 'read'):
            stream.seek(0)
            return stream
        return io.BytesIO(data)

    return io.BytesIO(decompressed)


def strip_compression_extension(path: str) -> str:
    """Remove compression extension from path to get the actual file extension.

    Args:
        path: File path that may have a compression extension.

    Returns:
        Path with compression extension removed.
    """
    path_lower = path.lower()
    for ext in COMPRESSION_EXTENSIONS:
        if path_lower.endswith(ext):
            return path[:-len(ext)]
    return path


def supports_streaming_decompression(compression: str) -> bool:
    """Check if a compression format supports streaming decompression.

    Args:
        compression: Compression type string.

    Returns:
        True if the format can be decompressed in a streaming fashion.
    """
    # Snappy does not support streaming decompression
    return compression in ('gzip', 'bz2', 'zstd', 'lz4')


def get_streaming_decompressor(
    stream: BinaryIO,
    compression: str
) -> Tuple[BinaryIO, bool]:
    """Get a streaming decompressor for the given compression type.

    This wraps the stream with a decompressor that reads data on-demand,
    rather than decompressing everything upfront. This enables early
    termination for row-limited queries.

    Args:
        stream: File-like object containing compressed data.
        compression: Compression type ('gzip', 'zstd', 'lz4', 'snappy', 'bz2').

    Returns:
        Tuple of (decompressed_stream, is_streaming) where is_streaming
        indicates whether true streaming is used (False for snappy which
        requires full decompression).

    Raises:
        ValueError: If required compression library is not installed.
    """
    if compression == 'gzip':
        # gzip.GzipFile wraps a stream and decompresses on-demand
        return gzip.GzipFile(fileobj=stream, mode='rb'), True

    elif compression == 'bz2':
        # BZ2File can wrap a stream for streaming decompression
        return bz2.BZ2File(stream, mode='rb'), True

    elif compression == 'zstd':
        if not HAS_ZSTD:
            raise ValueError(
                "zstandard package is required for .zst files. "
                "Install with: pip install zstandard"
            )
        # ZstdDecompressor.stream_reader() provides streaming decompression
        dctx = zstd.ZstdDecompressor()
        return dctx.stream_reader(stream), True

    elif compression == 'lz4':
        if not HAS_LZ4:
            raise ValueError(
                "lz4 package is required for .lz4 files. "
                "Install with: pip install lz4"
            )
        # lz4.frame.open() can wrap a stream for streaming decompression
        # We need to use LZ4FrameDecompressor for streaming
        return lz4.open(stream, mode='rb'), True

    elif compression == 'snappy':
        if not HAS_SNAPPY:
            raise ValueError(
                "python-snappy package is required for .snappy files. "
                "Install with: pip install python-snappy"
            )
        # Snappy does NOT support streaming - must decompress fully
        data = stream.read()
        decompressed = snappy.decompress(data)
        return io.BytesIO(decompressed), False

    else:
        # Unknown compression - return original stream
        return stream, True
