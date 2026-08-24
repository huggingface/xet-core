"""
End-to-end tests for XetRangeUploadCommit against HuggingFace Hub Storage Buckets.

Run after building the extension:
    cd hf_xet && maturin develop
    pytest tests/test_range_upload.py -v --tb=short

These tests require:
  - A HF token with write access to the bucket (stored at ~/.cache/huggingface/token)
  - A bucket named "lhoestq/range-upload-test" on HuggingFace Hub
"""

import hf_xet


# ── Helpers ──────────────────────────────────────────────────────────────────

def read_hf_token() -> str:
    """Read the HuggingFace token from the local cache."""
    import os
    token_path = os.path.expanduser("~/.cache/huggingface/token")
    with open(token_path) as f:
        return f.read().strip()


def write_token_url(repo_id: str) -> str:
    """Build the Xet write token refresh URL for a bucket repo."""
    return f"https://huggingface.co/api/buckets/{repo_id}/xet-write-token"


def read_token_url(repo_id: str) -> str:
    """Build the Xet read token refresh URL for a bucket repo."""
    return f"https://huggingface.co/api/buckets/{repo_id}/xet-read-token"


_AUTH_HEADERS = {"Authorization": f"Bearer {read_hf_token()}"}


def _upload_via_commit(session: hf_xet.XetSession, data: bytes, name: str) -> hf_xet.XetFileInfo:
    """Upload raw bytes via the regular upload commit and return XetFileInfo."""
    commit = session.new_upload_commit(
        token_refresh_url=write_token_url("lhoestq/range-upload-test"),
        token_refresh_headers=_AUTH_HEADERS,
    )
    h = commit.start_upload_bytes(data, name=name, sha256=hf_xet.SKIP_SHA256)
    commit.wait_to_finish()
    return h.result().xet_info


def _download_via_group(
    session: hf_xet.XetSession,
    file_info: hf_xet.XetFileInfo,
    dest_path: str,
) -> hf_xet.XetFileInfo:
    """Download a file via the regular file download group and return XetFileInfo."""
    group = session.new_file_download_group(
        token_refresh_url=read_token_url("lhoestq/range-upload-test"),
        token_refresh_headers=_AUTH_HEADERS,
    )
    h = group.start_download_file(file_info, dest_path)
    report = group.wait_to_finish()
    return report.downloads[h.task_id()].file_info


# ── Edit (replace bytes) ────────────────────────────────────────────────────

class TestRangeUploadEdit:
    """Test: upload original, edit bytes 0..13, verify composed result."""

    def test_e2e_range_upload_edit(self):
        original_data = b"Hello, World! This is a test file for range upload."
        assert len(original_data) == 51

        session = hf_xet.XetSession()

        # Step 1: upload original
        original_info = _upload_via_commit(session, original_data, "edit_test.txt")
        assert original_info.file_size == 51

        # Step 2: edit bytes 0..13 ("Hello, World!") -> "Universe! " (10 bytes)
        # Expected new size: 51 - 13 + 10 = 48
        commit = session.new_range_upload(
            original_info.hash,
            original_info.file_size,
            token_refresh_url=write_token_url("lhoestq/range-upload-test"),
            token_refresh_headers=_AUTH_HEADERS,
        )

        edit = commit.edit((0, 13), 10)
        edit.write(b"Universe! ")
        # No need to call finish() explicitly — commit handles it

        report = commit.commit()
        assert report.file_info.file_size == 48

        # Step 3: download and verify content
        dest_path = "/tmp/range_upload_edit_test.txt"
        _download_via_group(session, report.file_info, dest_path)
        with open(dest_path, "rb") as f:
            content = f.read()

        assert content == b"Universe!  This is a test file for range upload."


# ── Insert ──────────────────────────────────────────────────────────────────

class TestRangeUploadInsert:
    """Test: upload original, insert bytes at position, verify result."""

    def test_e2e_range_upload_insert(self):
        original_data = b"ABCDEF"
        assert len(original_data) == 6

        session = hf_xet.XetSession()

        # Step 1: upload original
        original_info = _upload_via_commit(session, original_data, "insert_test.txt")
        assert original_info.file_size == 6

        # Step 2: insert "XYZ" at position 2 (between B and C)
        # Expected new size: 6 + 3 = 9
        commit = session.new_range_upload(
            original_info.hash,
            original_info.file_size,
            token_refresh_url=write_token_url("lhoestq/range-upload-test"),
            token_refresh_headers=_AUTH_HEADERS,
        )

        edit = commit.insert(2, 3)
        edit.write(b"XYZ")
        # No need to call finish() explicitly — commit handles it

        report = commit.commit()
        assert report.file_info.file_size == 9

        # Step 3: download and verify content
        dest_path = "/tmp/range_upload_insert_test.txt"
        _download_via_group(session, report.file_info, dest_path)
        with open(dest_path, "rb") as f:
            content = f.read()

        assert content == b"ABXYZCDEF"


# ── Delete ──────────────────────────────────────────────────────────────────

class TestRangeUploadDelete:
    """Test: upload original, delete bytes, verify result."""

    def test_e2e_range_upload_delete(self):
        original_data = b"Hello, World!"
        assert len(original_data) == 13

        session = hf_xet.XetSession()

        # Step 1: upload original
        original_info = _upload_via_commit(session, original_data, "delete_test.txt")
        assert original_info.file_size == 13

        # Step 2: delete bytes 5..12 (", World") — 7 bytes removed
        # Expected new size: 13 - 7 = 6
        commit = session.new_range_upload(
            original_info.hash,
            original_info.file_size,
            token_refresh_url=write_token_url("lhoestq/range-upload-test"),
            token_refresh_headers=_AUTH_HEADERS,
        )

        edit = commit.delete(5, 12)
        # No need to call finish() — delete edits have no data to write

        report = commit.commit()
        assert report.file_info.file_size == 6

        # Step 3: download and verify content
        dest_path = "/tmp/range_upload_delete_test.txt"
        _download_via_group(session, report.file_info, dest_path)
        with open(dest_path, "rb") as f:
            content = f.read()

        assert content == b"Hello!"


# ── Append ──────────────────────────────────────────────────────────────────

class TestRangeUploadAppend:
    """Test: upload original, append bytes at end, verify result."""

    def test_e2e_range_upload_append(self):
        original_data = b"Hello, "
        assert len(original_data) == 7

        session = hf_xet.XetSession()

        # Step 1: upload original
        original_info = _upload_via_commit(session, original_data, "append_test.txt")
        assert original_info.file_size == 7

        # Step 2: append "World!" (6 bytes) at end
        # Expected new size: 7 + 6 = 13
        commit = session.new_range_upload(
            original_info.hash,
            original_info.file_size,
            token_refresh_url=write_token_url("lhoestq/range-upload-test"),
            token_refresh_headers=_AUTH_HEADERS,
        )

        edit = commit.append(6)
        edit.write(b"World!")
        # No need to call finish() explicitly — commit handles it

        report = commit.commit()
        assert report.file_info.file_size == 13

        # Step 3: download and verify content
        dest_path = "/tmp/range_upload_append_test.txt"
        _download_via_group(session, report.file_info, dest_path)
        with open(dest_path, "rb") as f:
            content = f.read()

        assert content == b"Hello, World!"


# ── Multiple edits ──────────────────────────────────────────────────────────

class TestRangeUploadMultipleEdits:
    """Test: upload original, apply multiple edits in one commit."""

    def test_e2e_range_upload_multiple_edits(self):
        original_data = b"0123456789ABCDEF"  # 16 bytes
        session = hf_xet.XetSession()

        # Step 1: upload original
        original_info = _upload_via_commit(session, original_data, "multi_test.txt")
        assert original_info.file_size == 16

        # Step 2: apply multiple edits
        # - edit 0..4 ("0123" -> "XXXX") - keep same length
        # - insert 8, 3 ("---") - 3 bytes added
        # - delete 14..16 ("EF") — 2 bytes removed
        # Expected: "XXXX4567---89ABCD" = 17 bytes
        commit = session.new_range_upload(
            original_info.hash,
            original_info.file_size,
            token_refresh_url=write_token_url("lhoestq/range-upload-test"),
            token_refresh_headers=_AUTH_HEADERS,
        )

        edit1 = commit.edit((0, 4), 4)
        edit1.write(b"XXXX")

        edit2 = commit.insert(8, 3)
        edit2.write(b"---")

        edit3 = commit.delete(14, 16)

        report = commit.commit()
        assert report.file_info.file_size == 17

        # Step 3: download and verify
        dest_path = "/tmp/range_upload_multi_test.txt"
        _download_via_group(session, report.file_info, dest_path)
        with open(dest_path, "rb") as f:
            content = f.read()

        assert content == b"XXXX4567---89ABCD"
