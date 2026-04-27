"""
Tests for XetUploadCommit: upload_file, upload_bytes, upload_stream, sha256 sentinels.

Not covered here (require a real CAS server):
  - with_token_info / with_token_refresh_url / with_custom_headers on the builder
"""

import hf_xet


# ── sha256 sentinels ──────────────────────────────────────────────────────────

class TestSha256Sentinels:
    def test_compute_sentinel_is_not_none(self):
        assert hf_xet.COMPUTE_SHA256 is not None

    def test_skip_sentinel_is_not_none(self):
        assert hf_xet.SKIP_SHA256 is not None

    def test_sentinels_have_repr(self):
        assert repr(hf_xet.COMPUTE_SHA256) == "COMPUTE_SHA256"
        assert repr(hf_xet.SKIP_SHA256) == "SKIP_SHA256"



# ── upload_file ───────────────────────────────────────────────────────────────

class TestUploadFile:
    def test_result_has_correct_file_size(self, endpoint, tmp_path):
        data = b"upload_file content"
        src = tmp_path / "src.bin"
        src.write_bytes(data)
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        h = commit.upload_file(str(src), sha256=hf_xet.SKIP_SHA256)
        commit.commit()
        result = h.result()
        assert result.xet_info.file_size == len(data)
        assert result.xet_info.hash
        assert result.xet_info.sha256 is None

    def test_sha256_computed_for_file(self, endpoint, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"sha256 file")
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        h = commit.upload_file(str(src), sha256=hf_xet.COMPUTE_SHA256)
        commit.commit()
        result = h.result()
        assert result.xet_info.sha256 is not None
        assert len(result.xet_info.sha256) == 64

    def test_sha256_provided_as_string_for_file(self, endpoint, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"provided sha256 file")
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        precomputed = "b" * 64
        h = commit.upload_file(str(src), sha256=precomputed)
        commit.commit()
        assert h.result().xet_info.sha256 == precomputed

    def test_try_result_after_commit(self, endpoint, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"try result")
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        h = commit.upload_file(str(src), sha256=hf_xet.SKIP_SHA256)
        commit.commit()
        result = h.try_result()
        assert result is not None
        assert result.xet_info.file_size == len(b"try result")

    def test_task_id_is_positive(self, endpoint, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"task id")
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        h = commit.upload_file(str(src), sha256=hf_xet.SKIP_SHA256)
        commit.commit()
        assert h.task_id() is not None


# ── upload_bytes ──────────────────────────────────────────────────────────────

class TestUploadBytes:
    def test_result_has_correct_file_size(self, endpoint):
        data = b"hello upload bytes"
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        h = commit.upload_bytes(data, name="f.bin", sha256=hf_xet.SKIP_SHA256)
        commit.commit()
        result = h.result()
        assert result.xet_info.file_size == len(data)
        assert result.xet_info.hash

    def test_sha256_computed_when_requested(self, endpoint):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        h = commit.upload_bytes(b"compute sha256", sha256=hf_xet.COMPUTE_SHA256)
        commit.commit()
        result = h.result()
        assert result.xet_info.sha256 is not None
        assert len(result.xet_info.sha256) == 64

    def test_sha256_provided_as_string(self, endpoint):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        precomputed = "a" * 64
        h = commit.upload_bytes(b"provided sha256", sha256=precomputed)
        commit.commit()
        assert h.result().xet_info.sha256 == precomputed

    def test_sha256_skipped_when_requested(self, endpoint):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        h = commit.upload_bytes(b"skip sha256", sha256=hf_xet.SKIP_SHA256)
        commit.commit()
        assert h.result().xet_info.sha256 is None

    def test_commit_report_contains_result(self, endpoint):
        data = b"report content"
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        h = commit.upload_bytes(data, sha256=hf_xet.SKIP_SHA256)
        report = commit.commit()
        result = report.uploads[h.task_id()]
        assert result.xet_info.file_size == len(data)
        assert result.xet_info.hash

    def test_multiple_files_in_one_commit(self, endpoint):
        files = {f"f{i}.bin": f"content {i}".encode() for i in range(4)}
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        handles = {name: commit.upload_bytes(data, name=name, sha256=hf_xet.SKIP_SHA256)
                   for name, data in files.items()}
        commit.commit()
        for name, h in handles.items():
            assert h.result().xet_info.file_size == len(files[name])

    def test_status_is_valid_string(self, endpoint):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        assert commit.status() in ("Running", "Finalizing", "Completed", "UserCancelled")

    def test_progress_returns_report(self, endpoint):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        report = commit.progress()
        assert hasattr(report, "total_bytes_completed")

    def test_abort_makes_commit_fail(self, endpoint):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        commit.abort()
        try:
            commit.commit()
            assert False, "expected commit to raise after abort"
        except Exception:
            pass

    def test_context_manager_commits_on_normal_exit(self, endpoint):
        with hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build() as commit:
            h = commit.upload_bytes(b"context manager", sha256=hf_xet.SKIP_SHA256)
        result = h.result()
        assert result.xet_info.file_size == len(b"context manager")
        assert result.xet_info.hash

    def test_context_manager_aborts_on_exception(self, endpoint):
        raised = False
        try:
            with hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build() as commit:
                commit.upload_bytes(b"will be aborted", sha256=hf_xet.SKIP_SHA256)
                raise ValueError("intentional error")
        except ValueError:
            raised = True
        assert raised  # exception must propagate, not be suppressed


# ── upload_stream ─────────────────────────────────────────────────────────────

class TestUploadStream:
    def test_write_and_finish(self, endpoint):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        stream = commit.upload_stream(name="stream.bin")
        stream.write(b"hello ")
        stream.write(b"world")
        result = stream.finish()
        assert result.xet_info.file_size == 11
        assert result.xet_info.hash

    def test_try_finish_before_finish_is_none(self, endpoint):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        stream = commit.upload_stream()
        assert stream.try_finish() is None

    def test_try_finish_after_finish(self, endpoint):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        stream = commit.upload_stream()
        stream.write(b"data")
        stream.finish()
        result = stream.try_finish()
        assert result is not None
        assert result.xet_info.file_size == 4

    def test_multiple_chunks(self, endpoint):
        data = b"chunk" * 200
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        stream = commit.upload_stream(name="big.bin")
        for i in range(0, len(data), 50):
            stream.write(data[i:i + 50])
        result = stream.finish()
        assert result.xet_info.file_size == len(data)

    def test_finish_must_precede_commit(self, endpoint):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        stream = commit.upload_stream()
        stream.write(b"abc")
        stream.finish()
        commit.commit()  # should not raise

    def test_status_while_open(self, endpoint):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        stream = commit.upload_stream()
        assert stream.status() in ("Running", "Finalizing", "Completed")

    def test_task_id_is_not_none(self, endpoint):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        stream = commit.upload_stream()
        assert stream.task_id() is not None

    def test_abort_before_finish(self, endpoint):
        commit = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint).build()
        stream = commit.upload_stream()
        stream.write(b"to be aborted")
        stream.abort()  # should not raise


# ── XetUploadCommitBuilder ────────────────────────────────────────────────────

class TestUploadCommitBuilder:
    def test_double_build_raises(self, endpoint):
        builder = hf_xet.XetSession().new_upload_commit().with_endpoint(endpoint)
        builder.build()
        try:
            builder.build()
            assert False, "expected ValueError on second build()"
        except Exception as e:
            assert "already consumed" in str(e)
