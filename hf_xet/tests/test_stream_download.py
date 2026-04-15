"""
Tests for XetDownloadStreamGroup: ordered and unordered streaming downloads.

_LARGE_SIZE = 300 KB is used throughout to exercise the full reconstruction
path while keeping tests fast.

Covers:
  - Full-file ordered stream (small and large)
  - Bounded range (start + end) on large files
  - Open-ended range (start only, end only) on large files
  - cancel()
  - Multiple concurrent streams from the same group
  - Full-file unordered stream (reassemble from offsets), small and large
  - Bounded range unordered on large files
  - Open-ended range unordered on large files
  - Builder double-build error

Not covered here (require a real CAS server):
  - with_token_info / with_token_refresh_url / with_custom_headers on the builder
"""

import pytest

import hf_xet
from conftest import upload_bytes_get_info


# ── Shared data ───────────────────────────────────────────────────────────────

DATA = b"0123456789abcdef"  # 16 bytes — known content for slice assertions

_LARGE_SIZE = 300 * 1024  # 300 KB — well above the 128 KB max chunk size
# Deterministic byte pattern: 0x00..0xFF repeating, so the content varies
# throughout the file and won't collapse into a single deduplicated chunk.
_LARGE_DATA = (bytes(range(256)) * (_LARGE_SIZE // 256 + 1))[:_LARGE_SIZE]

# Offsets chosen to land strictly inside the file and well away from both ends,
# so byte-range slicing is exercised without needing multiple stream yields.
_RANGE_START = 50_000
_RANGE_END = 250_000


@pytest.fixture(scope="module")
def large_file_endpoint(tmp_path_factory):
    """Upload _LARGE_DATA once per module and return (XetFileInfo, endpoint)."""
    tmp = tmp_path_factory.mktemp("large_stream")
    ep = f"local://{tmp / 'cas'}"
    info = upload_bytes_get_info(ep, _LARGE_DATA)
    return info, ep


# ── XetDownloadStreamGroup (ordered) ─────────────────────────────────────────

class TestDownloadStream:
    # ── small-file correctness ────────────────────────────────────────────────

    def test_full_file_reassembles(self, endpoint):
        data = b"ordered stream content"
        info = upload_bytes_get_info(endpoint, data)
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint).build()
        chunks = list(group.download_stream(info))
        assert b"".join(chunks) == data

    def test_bounded_range(self, endpoint):
        info = upload_bytes_get_info(endpoint, DATA)
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint).build()
        chunks = list(group.download_stream(info, start=4, end=12))
        assert b"".join(chunks) == DATA[4:12]

    def test_open_ended_start(self, endpoint):
        """start=N with no end streams from N to EOF."""
        info = upload_bytes_get_info(endpoint, DATA)
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint).build()
        chunks = list(group.download_stream(info, start=8))
        assert b"".join(chunks) == DATA[8:]

    def test_open_ended_end(self, endpoint):
        """end=N with no start streams from 0 to N."""
        info = upload_bytes_get_info(endpoint, DATA)
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint).build()
        chunks = list(group.download_stream(info, end=8))
        assert b"".join(chunks) == DATA[:8]

    def test_cancel_stops_iteration(self, endpoint):
        data = b"cancel me"
        info = upload_bytes_get_info(endpoint, data)
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint).build()
        stream = group.download_stream(info)
        stream.cancel()
        chunks = list(stream)
        assert isinstance(chunks, list)  # no crash; may be empty or partial

    def test_multiple_concurrent_streams_same_group(self, endpoint):
        """The same group object can serve multiple independent streams."""
        data_a = b"stream A content"
        data_b = b"stream B content"
        info_a = upload_bytes_get_info(endpoint, data_a)
        info_b = upload_bytes_get_info(endpoint, data_b)
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint).build()
        result_a = b"".join(group.download_stream(info_a))
        result_b = b"".join(group.download_stream(info_b))
        assert result_a == data_a
        assert result_b == data_b

    # ── large-file multi-chunk paths ──────────────────────────────────────────

    def test_large_file_full_reassembles(self, large_file_endpoint):
        """300 KB file spans multiple chunks; ordered stream must reassemble all bytes."""
        info, ep = large_file_endpoint
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(ep).build()
        result = b"".join(group.download_stream(info))
        assert result == _LARGE_DATA

    def test_large_file_bounded_range(self, large_file_endpoint):
        """Range [50 000, 250 000] on a 300 KB file; must return exact slice."""
        info, ep = large_file_endpoint
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(ep).build()
        result = b"".join(group.download_stream(info, start=_RANGE_START, end=_RANGE_END))
        assert result == _LARGE_DATA[_RANGE_START:_RANGE_END]

    def test_large_file_open_ended_start(self, large_file_endpoint):
        """start=N on a large file streams from N to EOF."""
        info, ep = large_file_endpoint
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(ep).build()
        result = b"".join(group.download_stream(info, start=_RANGE_START))
        assert result == _LARGE_DATA[_RANGE_START:]

    def test_large_file_open_ended_end(self, large_file_endpoint):
        """end=N on a large file streams from 0 to N."""
        info, ep = large_file_endpoint
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(ep).build()
        result = b"".join(group.download_stream(info, end=_RANGE_END))
        assert result == _LARGE_DATA[:_RANGE_END]


# ── XetDownloadStreamGroup (unordered) ───────────────────────────────────────

class TestDownloadUnorderedStream:
    def _reassemble(self, chunks_iter, total: int) -> bytes:
        buf = bytearray(total)
        for offset, chunk in chunks_iter:
            buf[offset:offset + len(chunk)] = chunk
        return bytes(buf)

    def _reassemble_range(self, chunks_iter) -> bytes:
        pieces = {}
        for offset, chunk in chunks_iter:
            pieces[offset] = chunk
        return b"".join(pieces[k] for k in sorted(pieces))

    # ── small-file correctness ────────────────────────────────────────────────

    def test_full_file_reassembles(self, endpoint):
        data = b"unordered stream content"
        info = upload_bytes_get_info(endpoint, data)
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint).build()
        result = self._reassemble(group.download_unordered_stream(info), len(data))
        assert result == data

    def test_bounded_range(self, endpoint):
        info = upload_bytes_get_info(endpoint, DATA)
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint).build()
        assembled = self._reassemble_range(group.download_unordered_stream(info, start=2, end=10))
        assert assembled == DATA[2:10]

    def test_open_ended_start(self, endpoint):
        """start=N with no end streams from N to EOF."""
        info = upload_bytes_get_info(endpoint, DATA)
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint).build()
        assembled = self._reassemble_range(group.download_unordered_stream(info, start=8))
        assert assembled == DATA[8:]

    def test_open_ended_end(self, endpoint):
        """end=N with no start streams from 0 to N."""
        info = upload_bytes_get_info(endpoint, DATA)
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint).build()
        assembled = self._reassemble_range(group.download_unordered_stream(info, end=8))
        assert assembled == DATA[:8]

    # ── large-file multi-chunk paths ──────────────────────────────────────────

    def test_large_file_full_reassembles(self, large_file_endpoint):
        """300 KB unordered stream must reassemble to the original bytes."""
        info, ep = large_file_endpoint
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(ep).build()
        result = self._reassemble(group.download_unordered_stream(info), _LARGE_SIZE)
        assert result == _LARGE_DATA

    def test_large_file_offsets_are_valid(self, large_file_endpoint):
        """Every (offset, chunk) pair must lie within the file bounds."""
        info, ep = large_file_endpoint
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(ep).build()
        for offset, chunk in group.download_unordered_stream(info):
            assert 0 <= offset < _LARGE_SIZE
            assert offset + len(chunk) <= _LARGE_SIZE

    def test_large_file_bounded_range(self, large_file_endpoint):
        """Range [50 000, 250 000] unordered on a 300 KB file must reassemble to the correct slice."""
        info, ep = large_file_endpoint
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(ep).build()
        assembled = self._reassemble_range(
            group.download_unordered_stream(info, start=_RANGE_START, end=_RANGE_END)
        )
        assert assembled == _LARGE_DATA[_RANGE_START:_RANGE_END]

    def test_large_file_open_ended_start(self, large_file_endpoint):
        """start=N unordered on a large file streams N..EOF correctly."""
        info, ep = large_file_endpoint
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(ep).build()
        assembled = self._reassemble_range(group.download_unordered_stream(info, start=_RANGE_START))
        assert assembled == _LARGE_DATA[_RANGE_START:]

    def test_large_file_open_ended_end(self, large_file_endpoint):
        """end=N unordered on a large file streams 0..N correctly."""
        info, ep = large_file_endpoint
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(ep).build()
        assembled = self._reassemble_range(group.download_unordered_stream(info, end=_RANGE_END))
        assert assembled == _LARGE_DATA[:_RANGE_END]


# ── XetDownloadStreamGroupBuilder ────────────────────────────────────────────

class TestDownloadStreamGroupBuilder:
    def test_double_build_raises(self, endpoint):
        builder = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint)
        builder.build()
        try:
            builder.build()
            assert False, "expected ValueError on second build()"
        except Exception as e:
            assert "already consumed" in str(e)
