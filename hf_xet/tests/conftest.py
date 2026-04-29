"""
Shared pytest fixtures and upload helpers used across all test modules.

Run after building the extension:
    cd hf_xet && maturin develop
    pytest tests/ -v
"""

import pytest
import hf_xet


@pytest.fixture
def endpoint(tmp_path):
    """Local CAS endpoint backed by a per-test temp directory."""
    return f"local://{tmp_path / 'cas'}"


# ── Upload helpers ────────────────────────────────────────────────────────────

def upload_bytes_get_info(endpoint: str, data: bytes) -> hf_xet.XetFileInfo:
    """Upload raw bytes, commit, and return the resulting XetFileInfo."""
    commit = hf_xet.XetSession().new_upload_commit(endpoint=endpoint)
    h = commit.start_upload_bytes(data, sha256=hf_xet.SKIP_SHA256)
    commit.wait_to_finish()
    return h.result().xet_info


def upload_file_get_info(endpoint: str, tmp_path, data: bytes) -> hf_xet.XetFileInfo:
    """Write data to a temp file, upload it, and return the resulting XetFileInfo."""
    src = tmp_path / "upload_src.bin"
    src.write_bytes(data)
    commit = hf_xet.XetSession().new_upload_commit(endpoint=endpoint)
    h = commit.start_upload_file(str(src), sha256=hf_xet.SKIP_SHA256)
    commit.wait_to_finish()
    return h.result().xet_info


def upload_stream_get_info(endpoint: str, data: bytes) -> hf_xet.XetFileInfo:
    """Upload data via upload_stream and return the resulting XetFileInfo."""
    commit = hf_xet.XetSession().new_upload_commit(endpoint=endpoint)
    stream = commit.start_upload_stream()
    stream.write(data)
    r = stream.finish()
    commit.wait_to_finish()
    return r.xet_info
