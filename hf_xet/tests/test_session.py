"""
Tests for XetSession itself: status, abort, sigint_abort, and builder creation.
"""

import hf_xet


class TestXetSession:
    def test_status_returns_valid_string(self):
        session = hf_xet.XetSession()
        assert session.status() in ("Running", "Finalizing", "Completed", "UserCancelled")

    def test_abort_does_not_raise(self):
        session = hf_xet.XetSession()
        session.abort()  # should not raise

    def test_sigint_abort_does_not_raise(self):
        # Uses a dedicated session; sigint_abort shuts down the internal runtime.
        session = hf_xet.XetSession()
        session.sigint_abort()  # should not raise

    def test_new_upload_commit_returns_builder(self, endpoint):
        builder = hf_xet.XetSession().new_upload_commit()
        assert builder is not None
        # Chain with_endpoint to verify the builder is usable.
        builder.with_endpoint(endpoint).build()

    def test_new_file_download_group_returns_builder(self, endpoint):
        builder = hf_xet.XetSession().new_file_download_group()
        assert builder is not None
        builder.with_endpoint(endpoint).build()

    def test_new_download_stream_group_returns_builder(self, endpoint):
        builder = hf_xet.XetSession().new_download_stream_group()
        assert builder is not None
        builder.with_endpoint(endpoint).build()
