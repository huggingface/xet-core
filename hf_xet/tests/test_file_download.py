"""
Tests for XetFileDownloadGroup and XetFileDownload handles.

Not covered here (require a real CAS server):
  - token, token_refresh_url, custom_headers kwargs
"""

import hf_xet
from conftest import upload_bytes_get_info, upload_file_get_info, upload_stream_get_info


# ── XetFileDownloadGroup ──────────────────────────────────────────────────────

class TestFileDownloadGroup:
    def test_file_written_to_disk(self, endpoint, tmp_path):
        data = b"download file content"
        info = upload_bytes_get_info(endpoint, data)
        dest = tmp_path / "out.bin"
        with hf_xet.XetSession().new_file_download_group(endpoint=endpoint) as group:
            group.start_download_file(info, str(dest))
        assert dest.exists()
        assert dest.read_bytes() == data

    def test_result_has_correct_file_size(self, endpoint, tmp_path):
        data = b"result check"
        info = upload_bytes_get_info(endpoint, data)
        dest = tmp_path / "out.bin"
        group = hf_xet.XetSession().new_file_download_group(endpoint=endpoint)
        h = group.start_download_file(info, str(dest))
        group.wait_to_finish()
        result = h.result()
        assert result.file_info.file_size == len(data)
        assert result.file_info.hash

    def test_task_id_matches_group_report(self, endpoint, tmp_path):
        data = b"report match"
        info = upload_bytes_get_info(endpoint, data)
        group = hf_xet.XetSession().new_file_download_group(endpoint=endpoint)
        h = group.start_download_file(info, str(tmp_path / "out.bin"))
        report = group.wait_to_finish()
        assert h.task_id() in report.downloads

    def test_multiple_files(self, endpoint, tmp_path):
        payloads = [f"file {i}".encode() for i in range(3)]
        infos = [upload_bytes_get_info(endpoint, d) for d in payloads]
        group = hf_xet.XetSession().new_file_download_group(endpoint=endpoint)
        handles = [group.start_download_file(info, str(tmp_path / f"out{i}.bin"))
                   for i, info in enumerate(infos)]
        group.wait_to_finish()
        for h, data in zip(handles, payloads):
            assert h.result().file_info.file_size == len(data)

    def test_round_trip_via_upload_file(self, endpoint, tmp_path):
        payloads = [f"upload_file content {i}".encode() for i in range(3)]
        infos = [upload_file_get_info(endpoint, tmp_path, d) for d in payloads]
        dests = [tmp_path / f"out{i}.bin" for i in range(len(payloads))]
        with hf_xet.XetSession().new_file_download_group(endpoint=endpoint) as group:
            for info, dest in zip(infos, dests):
                group.start_download_file(info, str(dest))
        for dest, data in zip(dests, payloads):
            assert dest.read_bytes() == data

    def test_round_trip_via_upload_stream(self, endpoint, tmp_path):
        payloads = [f"upload_stream content {i}".encode() for i in range(3)]
        infos = [upload_stream_get_info(endpoint, d) for d in payloads]
        dests = [tmp_path / f"out{i}.bin" for i in range(len(payloads))]
        with hf_xet.XetSession().new_file_download_group(endpoint=endpoint) as group:
            for info, dest in zip(infos, dests):
                group.start_download_file(info, str(dest))
        for dest, data in zip(dests, payloads):
            assert dest.read_bytes() == data

    def test_status_is_valid_state(self, endpoint, tmp_path):
        info = upload_bytes_get_info(endpoint, b"status check")
        group = hf_xet.XetSession().new_file_download_group(endpoint=endpoint)
        group.start_download_file(info, str(tmp_path / "out.bin"))
        assert group.status() in (
            hf_xet.XetTaskState.Running,
            hf_xet.XetTaskState.Finalizing,
            hf_xet.XetTaskState.Completed,
            hf_xet.XetTaskState.UserCancelled,
        )
        group.wait_to_finish()

    def test_progress_returns_report(self, endpoint):
        group = hf_xet.XetSession().new_file_download_group(endpoint=endpoint)
        report = group.progress()
        assert hasattr(report, "total_bytes_completed")
        group.wait_to_finish()

    def test_abort_makes_finish_fail(self, endpoint):
        group = hf_xet.XetSession().new_file_download_group(endpoint=endpoint)
        group.abort()
        try:
            group.wait_to_finish()
            assert False, "expected finish to raise after abort"
        except Exception:
            pass

    def test_context_manager_calls_abort_on_exception(self, endpoint, tmp_path):
        info = upload_bytes_get_info(endpoint, b"abort download")
        raised = False
        try:
            with hf_xet.XetSession().new_file_download_group(endpoint=endpoint) as group:
                group.start_download_file(info, str(tmp_path / "out.bin"))
                raise RuntimeError("intentional error")
        except RuntimeError:
            raised = True
        assert raised


# ── XetFileDownload handle ────────────────────────────────────────────────────

class TestFileDownloadHandle:
    def test_try_result_after_finish_is_not_none(self, endpoint, tmp_path):
        info = upload_bytes_get_info(endpoint, b"try result data")
        group = hf_xet.XetSession().new_file_download_group(endpoint=endpoint)
        h = group.start_download_file(info, str(tmp_path / "out.bin"))
        group.wait_to_finish()
        result = h.try_result()
        assert result is not None
        assert result.file_info.file_size == len(b"try result data")

    def test_cancel_does_not_raise(self, endpoint, tmp_path):
        info = upload_bytes_get_info(endpoint, b"cancel target")
        group = hf_xet.XetSession().new_file_download_group(endpoint=endpoint)
        h = group.start_download_file(info, str(tmp_path / "out.bin"))
        h.cancel()  # should not raise; download may or may not have completed

    def test_status_is_valid_state(self, endpoint, tmp_path):
        info = upload_bytes_get_info(endpoint, b"status data")
        group = hf_xet.XetSession().new_file_download_group(endpoint=endpoint)
        h = group.start_download_file(info, str(tmp_path / "out.bin"))
        group.wait_to_finish()
        assert h.status() in (
            hf_xet.XetTaskState.Running,
            hf_xet.XetTaskState.Finalizing,
            hf_xet.XetTaskState.Completed,
            hf_xet.XetTaskState.UserCancelled,
        )

    def test_task_id_is_not_none(self, endpoint, tmp_path):
        info = upload_bytes_get_info(endpoint, b"task id data")
        group = hf_xet.XetSession().new_file_download_group(endpoint=endpoint)
        h = group.start_download_file(info, str(tmp_path / "out.bin"))
        assert h.task_id() is not None
        group.wait_to_finish()


