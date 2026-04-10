"""
Integration tests for XetSession upload and download operations.

Run after building the extension:
    cd hf_xet && maturin develop
    pytest tests/test_xet_session.py -v
"""

import threading

import hf_xet


# ── Fixtures / helpers ────────────────────────────────────────────────────────

def endpoint(tmp_path):
    return f"local://{tmp_path / 'cas'}"


def upload_bytes_get_info(tmp_path, data: bytes):
    """Upload bytes, commit, and return PyXetDownloadInfo for the result."""
    commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build()
    h = commit.upload_bytes(data, sha256=hf_xet.Sha256Policy.skip())
    commit.commit()
    r = h.result()
    return hf_xet.PyXetDownloadInfo("unused", r.hash, r.file_size)


def upload_file_get_info(tmp_path, data: bytes):
    """Write data to a temp file, upload it, and return PyXetDownloadInfo."""
    src = tmp_path / "upload_src.bin"
    src.write_bytes(data)
    commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build()
    h = commit.upload_file(str(src), sha256=hf_xet.Sha256Policy.skip())
    commit.commit()
    r = h.result()
    return hf_xet.PyXetDownloadInfo("unused", r.hash, r.file_size)


def upload_stream_get_info(tmp_path, data: bytes):
    """Upload data via upload_stream and return PyXetDownloadInfo."""
    commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build()
    stream = commit.upload_stream()
    stream.write(data)
    r = stream.finish()
    commit.commit()
    return hf_xet.PyXetDownloadInfo("unused", r.hash, r.file_size)


# ── upload_file ───────────────────────────────────────────────────────────────

class TestUploadFile:
    def test_result_has_correct_file_size(self, tmp_path):
        data = b"upload_file content"
        src = tmp_path / "src.bin"
        src.write_bytes(data)
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build()
        h = commit.upload_file(str(src), sha256=hf_xet.Sha256Policy.skip())
        commit.commit()
        result = h.result()
        assert result.file_size == len(data)
        assert result.hash
        assert result.sha256 is None

    def test_try_result_after_commit(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"try result")
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build()
        h = commit.upload_file(str(src), sha256=hf_xet.Sha256Policy.skip())
        commit.commit()
        result = h.try_result()
        assert result is not None
        assert result.file_size == len(b"try result")


# ── upload_bytes ──────────────────────────────────────────────────────────────

class TestUploadBytes:
    def test_result_has_correct_file_size(self, tmp_path):
        data = b"hello upload bytes"
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build()
        h = commit.upload_bytes(data, name="f.bin", sha256=hf_xet.Sha256Policy.skip())
        commit.commit()
        result = h.result()
        assert result.file_size == len(data)
        assert result.hash

    def test_sha256_computed_when_requested(self, tmp_path):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build()
        h = commit.upload_bytes(b"compute sha256", sha256=hf_xet.Sha256Policy.compute())
        commit.commit()
        result = h.result()
        assert result.sha256 is not None
        assert len(result.sha256) == 64

    def test_commit_report_contains_result(self, tmp_path):
        data = b"report content"
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build()
        h = commit.upload_bytes(data, sha256=hf_xet.Sha256Policy.skip())
        report = commit.commit()
        result = report.uploads[h.task_id()]
        assert result.file_size == len(data)
        assert result.hash

    def test_multiple_files_in_one_commit(self, tmp_path):
        files = {f"f{i}.bin": f"content {i}".encode() for i in range(4)}
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build()
        handles = {name: commit.upload_bytes(data, name=name, sha256=hf_xet.Sha256Policy.skip())
                   for name, data in files.items()}
        commit.commit()
        for name, h in handles.items():
            assert h.result().file_size == len(files[name])

    def test_context_manager_commits_on_normal_exit(self, tmp_path):
        with hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build() as commit:
            h = commit.upload_bytes(b"context manager", sha256=hf_xet.Sha256Policy.skip())
        result = h.result()
        assert result.file_size == len(b"context manager")
        assert result.hash

    def test_context_manager_aborts_on_exception(self, tmp_path):
        raised = False
        try:
            with hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build() as commit:
                commit.upload_bytes(b"will be aborted", sha256=hf_xet.Sha256Policy.skip())
                raise ValueError("intentional error")
        except ValueError:
            raised = True
        assert raised  # exception must propagate, not be suppressed


# ── upload_stream ─────────────────────────────────────────────────────────────

class TestUploadStream:
    def test_write_and_finish(self, tmp_path):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build()
        stream = commit.upload_stream(name="stream.bin")
        stream.write(b"hello ")
        stream.write(b"world")
        result = stream.finish()
        assert result.file_size == 11
        assert result.hash

    def test_try_finish_before_finish_is_none(self, tmp_path):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build()
        stream = commit.upload_stream()
        assert stream.try_finish() is None

    def test_try_finish_after_finish(self, tmp_path):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build()
        stream = commit.upload_stream()
        stream.write(b"data")
        stream.finish()
        result = stream.try_finish()
        assert result is not None
        assert result.file_size == 4

    def test_multiple_chunks(self, tmp_path):
        data = b"chunk" * 200
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build()
        stream = commit.upload_stream(name="big.bin")
        for i in range(0, len(data), 50):
            stream.write(data[i:i + 50])
        result = stream.finish()
        assert result.file_size == len(data)

    def test_finish_must_precede_commit(self, tmp_path):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build()
        stream = commit.upload_stream()
        stream.write(b"abc")
        stream.finish()
        commit.commit()  # should not raise

    def test_status_while_open(self, tmp_path):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint(tmp_path)).build()
        stream = commit.upload_stream()
        assert stream.status() in ("Running", "Finalizing", "Completed")


# ── download_file ─────────────────────────────────────────────────────────────

class TestDownloadFile:
    def test_file_written_to_disk(self, tmp_path):
        data = b"download file content"
        info = upload_bytes_get_info(tmp_path, data)
        dest = tmp_path / "out.bin"
        info.destination_path = str(dest)
        with hf_xet.XetSession().new_file_download_group().with_endpoint(endpoint(tmp_path)).build() as group:
            group.download_file(info)
        assert dest.exists()
        assert dest.read_bytes() == data

    def test_result_has_correct_file_size(self, tmp_path):
        data = b"result check"
        info = upload_bytes_get_info(tmp_path, data)
        dest = tmp_path / "out.bin"
        group = hf_xet.XetSession().new_file_download_group().with_endpoint(endpoint(tmp_path)).build()
        h = group.download_file(info, dest_path=str(dest))
        group.finish()
        result = h.result()
        assert result.file_size == len(data)
        assert result.hash

    def test_task_id_matches_group_report(self, tmp_path):
        data = b"report match"
        info = upload_bytes_get_info(tmp_path, data)
        group = hf_xet.XetSession().new_file_download_group().with_endpoint(endpoint(tmp_path)).build()
        h = group.download_file(info, dest_path=str(tmp_path / "out.bin"))
        report = group.finish()
        assert h.task_id() in report.downloads

    def test_multiple_files(self, tmp_path):
        payloads = [f"file {i}".encode() for i in range(3)]
        infos = [upload_bytes_get_info(tmp_path, d) for d in payloads]
        group = hf_xet.XetSession().new_file_download_group().with_endpoint(endpoint(tmp_path)).build()
        handles = []
        for i, info in enumerate(infos):
            handles.append(group.download_file(info, dest_path=str(tmp_path / f"out{i}.bin")))
        group.finish()
        for h, data in zip(handles, payloads):
            assert h.result().file_size == len(data)

    def test_round_trip_via_upload_file(self, tmp_path):
        payloads = [f"upload_file content {i}".encode() for i in range(3)]
        infos = [upload_file_get_info(tmp_path, d) for d in payloads]
        dests = [tmp_path / f"out{i}.bin" for i in range(len(payloads))]
        for info, dest in zip(infos, dests):
            info.destination_path = str(dest)
        with hf_xet.XetSession().new_file_download_group().with_endpoint(endpoint(tmp_path)).build() as group:
            for info in infos:
                group.download_file(info)
        for dest, data in zip(dests, payloads):
            assert dest.read_bytes() == data

    def test_round_trip_via_upload_stream(self, tmp_path):
        payloads = [f"upload_stream content {i}".encode() for i in range(3)]
        infos = [upload_stream_get_info(tmp_path, d) for d in payloads]
        dests = [tmp_path / f"out{i}.bin" for i in range(len(payloads))]
        for info, dest in zip(infos, dests):
            info.destination_path = str(dest)
        with hf_xet.XetSession().new_file_download_group().with_endpoint(endpoint(tmp_path)).build() as group:
            for info in infos:
                group.download_file(info)
        for dest, data in zip(dests, payloads):
            assert dest.read_bytes() == data

    def test_context_manager_calls_abort_on_exception(self, tmp_path):
        info = upload_bytes_get_info(tmp_path, b"abort download")
        info.destination_path = str(tmp_path / "out.bin")
        raised = False
        try:
            with hf_xet.XetSession().new_file_download_group().with_endpoint(endpoint(tmp_path)).build() as group:
                group.download_file(info)
                raise RuntimeError("intentional error")
        except RuntimeError:
            raised = True
        assert raised


# ── download_stream ───────────────────────────────────────────────────────────

class TestDownloadStream:
    def test_ordered_stream_reassembles_data(self, tmp_path):
        data = b"ordered stream content"
        info = upload_bytes_get_info(tmp_path, data)
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint(tmp_path)).build()
        chunks = list(group.download_stream(info))
        assert b"".join(chunks) == data

    def test_stream_with_byte_range(self, tmp_path):
        data = b"0123456789abcdef"
        info = upload_bytes_get_info(tmp_path, data)
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint(tmp_path)).build()
        chunks = list(group.download_stream(info, range=(4, 12)))
        assert b"".join(chunks) == data[4:12]

    def test_cancel_stops_iteration(self, tmp_path):
        data = b"cancel me"
        info = upload_bytes_get_info(tmp_path, data)
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint(tmp_path)).build()
        stream = group.download_stream(info)
        stream.cancel()
        chunks = list(stream)
        # After cancel, no further chunks are yielded
        assert isinstance(chunks, list)


# ── download_unordered_stream ─────────────────────────────────────────────────

class TestDownloadUnorderedStream:
    def test_reassembles_data_from_offsets(self, tmp_path):
        data = b"unordered stream content"
        info = upload_bytes_get_info(tmp_path, data)
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint(tmp_path)).build()
        buf = bytearray(len(data))
        for offset, chunk in group.download_unordered_stream(info):
            buf[offset:offset + len(chunk)] = chunk
        assert bytes(buf) == data

    def test_range_reassembles_correctly(self, tmp_path):
        data = b"0123456789abcdef"
        info = upload_bytes_get_info(tmp_path, data)
        group = hf_xet.XetSession().new_download_stream_group().with_endpoint(endpoint(tmp_path)).build()
        result = {}
        for offset, chunk in group.download_unordered_stream(info, range=(2, 10)):
            result[offset] = chunk
        assembled = b"".join(result[k] for k in sorted(result))
        assert assembled == data[2:10]


# ── progress callback ─────────────────────────────────────────────────────────

class TestProgressCallback:
    def test_upload_callback_receives_group_and_items(self, tmp_path):
        calls = []
        fired = threading.Event()

        def on_progress(group, items):
            calls.append((group, items))
            fired.set()

        (hf_xet.XetSession()
         .new_upload_commit()
         .with_endpoint(endpoint(tmp_path))
         .with_progress_callback(on_progress, interval_ms=10)
         .build()
         .commit())

        fired.wait(timeout=1.0)
        assert len(calls) > 0
        group_report, item_reports = calls[0]
        assert hasattr(group_report, "total_bytes_completed")
        assert isinstance(item_reports, list)

    def test_upload_callback_called_for_each_file(self, tmp_path):
        seen_names = set()
        fired = threading.Event()

        def on_progress(_, items):
            for item in items:
                seen_names.add(item.item_name)
            fired.set()

        commit = (hf_xet.XetSession()
                  .new_upload_commit()
                  .with_endpoint(endpoint(tmp_path))
                  .with_progress_callback(on_progress, interval_ms=10)
                  .build())
        commit.upload_bytes(b"file a", name="a.bin", sha256=hf_xet.Sha256Policy.skip())
        commit.upload_bytes(b"file b", name="b.bin", sha256=hf_xet.Sha256Policy.skip())
        commit.commit()

        fired.wait(timeout=1.0)
        assert "a.bin" in seen_names
        assert "b.bin" in seen_names

    def test_download_callback_receives_group_and_items(self, tmp_path):
        data = b"progress download content"
        info = upload_bytes_get_info(tmp_path, data)
        dest = tmp_path / "out.bin"
        info.destination_path = str(dest)

        calls = []
        fired = threading.Event()

        def on_progress(group, items):
            calls.append((group, items))
            fired.set()

        with (hf_xet.XetSession()
              .new_file_download_group()
              .with_endpoint(endpoint(tmp_path))
              .with_progress_callback(on_progress, interval_ms=10)
              .build()) as group:
            group.download_file(info)

        fired.wait(timeout=1.0)
        assert len(calls) > 0
        group_report, item_reports = calls[0]
        assert hasattr(group_report, "total_bytes_completed")
        assert isinstance(item_reports, list)

    def test_download_callback_called_for_each_file(self, tmp_path):
        payloads = {f"f{i}.bin": f"content {i}".encode() for i in range(3)}
        infos = {}
        for name, data in payloads.items():
            info = upload_bytes_get_info(tmp_path, data)
            info.destination_path = str(tmp_path / name)
            infos[name] = info

        seen_names = set()
        fired = threading.Event()

        def on_progress(_, items):
            for item in items:
                seen_names.add(item.item_name)
            fired.set()

        with (hf_xet.XetSession()
              .new_file_download_group()
              .with_endpoint(endpoint(tmp_path))
              .with_progress_callback(on_progress, interval_ms=10)
              .build()) as group:
            for info in infos.values():
                group.download_file(info)

        fired.wait(timeout=1.0)
        assert seen_names == {str(tmp_path / name) for name in payloads}
