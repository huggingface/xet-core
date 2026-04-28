"""
Tests for progress callbacks on XetUploadCommit and XetFileDownloadGroup.

The callback receives:
  arg1: GroupProgressReport  — aggregate bytes (total_bytes, total_bytes_completed, …)
  arg2: dict[UniqueID, ItemProgressReport]  — per-file (item_name, bytes_completed, total_bytes)
"""

import threading

import hf_xet
from conftest import upload_bytes_get_info


# ── Upload progress ───────────────────────────────────────────────────────────

class TestUploadProgressCallback:
    def test_callback_receives_correct_argument_types(self, endpoint):
        calls = []
        fired = threading.Event()

        def on_progress(group, items):
            calls.append((group, items))
            fired.set()

        (hf_xet.XetSession()
         .new_upload_commit(endpoint=endpoint, progress_callback=on_progress, progress_interval_ms=10)
         .commit())

        fired.wait(timeout=1.0)
        assert len(calls) > 0
        group_report, item_reports = calls[0]
        assert hasattr(group_report, "total_bytes_completed")
        assert hasattr(group_report, "total_bytes")
        assert isinstance(item_reports, dict)

    def test_item_names_match_uploaded_names(self, endpoint):
        seen_names = set()
        fired = threading.Event()

        def on_progress(_, items):
            for item in items.values():
                seen_names.add(item.item_name)
            fired.set()

        commit = hf_xet.XetSession().new_upload_commit(
            endpoint=endpoint, progress_callback=on_progress, progress_interval_ms=10
        )
        commit.upload_bytes(b"file a", name="a.bin", sha256=hf_xet.SKIP_SHA256)
        commit.upload_bytes(b"file b", name="b.bin", sha256=hf_xet.SKIP_SHA256)
        commit.commit()

        fired.wait(timeout=1.0)
        assert "a.bin" in seen_names
        assert "b.bin" in seen_names

    def test_item_progress_has_expected_fields(self, endpoint):
        items_seen = []
        fired = threading.Event()

        def on_progress(_, items):
            for item in items.values():
                items_seen.append(item)
            fired.set()

        commit = hf_xet.XetSession().new_upload_commit(
            endpoint=endpoint, progress_callback=on_progress, progress_interval_ms=10
        )
        commit.upload_bytes(b"progress fields", name="p.bin", sha256=hf_xet.SKIP_SHA256)
        commit.commit()

        fired.wait(timeout=1.0)
        assert len(items_seen) > 0
        item = items_seen[0]
        assert hasattr(item, "item_name")
        assert hasattr(item, "bytes_completed")
        assert hasattr(item, "total_bytes")


# ── Download progress ─────────────────────────────────────────────────────────

class TestDownloadProgressCallback:
    def test_callback_receives_correct_argument_types(self, endpoint, tmp_path):
        data = b"progress download content"
        info = upload_bytes_get_info(endpoint, data)
        dest = tmp_path / "out.bin"

        calls = []
        fired = threading.Event()

        def on_progress(group, items):
            calls.append((group, items))
            fired.set()

        with hf_xet.XetSession().new_file_download_group(
            endpoint=endpoint, progress_callback=on_progress, progress_interval_ms=10
        ) as group:
            group.download_file(info, str(dest))

        fired.wait(timeout=1.0)
        assert len(calls) > 0
        group_report, item_reports = calls[0]
        assert hasattr(group_report, "total_bytes_completed")
        assert hasattr(group_report, "total_bytes")
        assert isinstance(item_reports, dict)

    def test_item_count_matches_file_count(self, endpoint, tmp_path):
        payloads = {f"f{i}.bin": f"content {i}".encode() for i in range(3)}
        infos = {name: upload_bytes_get_info(endpoint, data) for name, data in payloads.items()}

        seen_names = set()
        fired = threading.Event()

        def on_progress(_, items):
            for item in items.values():
                seen_names.add(item.item_name)
            fired.set()

        with hf_xet.XetSession().new_file_download_group(
            endpoint=endpoint, progress_callback=on_progress, progress_interval_ms=10
        ) as group:
            for name, info in infos.items():
                group.download_file(info, str(tmp_path / name))

        fired.wait(timeout=1.0)
        assert len(seen_names) == len(payloads)
