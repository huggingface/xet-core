//! Integration tests for range-based downloads using a LocalTestServer.
//!
//! Exercises all range variants (`start..end`, `start..`, `..end`, `..`) across
//! the three download paths: file, writer, and streaming.

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use tempfile::TempDir;
    use xet_client::cas_client::{LocalTestServer, LocalTestServerBuilder};
    use xet_data::processing::configurations::TranslatorConfig;
    use xet_data::processing::{FileDownloadSession, FileUploadSession, Sha256Policy, XetFileInfo};

    async fn upload_bytes(upload_session: &Arc<FileUploadSession>, name: &str, data: &[u8]) -> XetFileInfo {
        let (_id, mut cleaner) = upload_session
            .start_clean(Some(name.into()), Some(data.len() as u64), Sha256Policy::Compute)
            .unwrap();
        cleaner.add_data(data).await.unwrap();
        let (xfi, _metrics) = cleaner.finish().await.unwrap();
        xfi
    }

    struct TestHarness {
        _server: LocalTestServer,
        _base_dir: TempDir,
        session: Arc<FileDownloadSession>,
        xfi: XetFileInfo,
        data: Vec<u8>,
    }

    async fn setup() -> TestHarness {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let data: Vec<u8> = (0..=255u8).cycle().take(8192).collect();
        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi = upload_bytes(&upload_session, "range_test", &data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config).await.unwrap();
        TestHarness {
            _server: server,
            _base_dir: base_dir,
            session: download_session,
            xfi,
            data,
        }
    }

    // ── Writer helpers ───────────────────────────────────────────────────────

    async fn writer_download(
        session: &FileDownloadSession,
        xfi: &XetFileInfo,
        range: impl std::ops::RangeBounds<u64>,
    ) -> (u64, Vec<u8>) {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        // Keep the NamedTempFile alive so the path remains valid.
        let file = tmp.reopen().unwrap();
        let (_id, n_bytes) = session.download_to_writer(xfi, range, file).await.unwrap();
        let contents = fs::read(&path).unwrap();
        (n_bytes, contents)
    }

    // ── Stream helpers ───────────────────────────────────────────────────────

    async fn stream_download(
        session: &FileDownloadSession,
        xfi: &XetFileInfo,
        range: impl std::ops::RangeBounds<u64>,
    ) -> Vec<u8> {
        let (_id, mut stream) = session.download_stream_range(xfi, range).await.unwrap();
        let mut collected = Vec::new();
        while let Some(chunk) = stream.next().await.unwrap() {
            collected.extend_from_slice(&chunk);
        }
        collected
    }

    // ── download_to_writer with various range types ──────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_writer_bounded_range() {
        let h = setup().await;
        let (n_bytes, buf) = writer_download(&h.session, &h.xfi, 100..200).await;
        assert_eq!(n_bytes, 100);
        assert_eq!(buf, h.data[100..200]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_writer_range_from() {
        let h = setup().await;
        let (n_bytes, buf) = writer_download(&h.session, &h.xfi, 8000..).await;
        assert_eq!(n_bytes, 192);
        assert_eq!(buf, h.data[8000..]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_writer_range_to() {
        let h = setup().await;
        let (n_bytes, buf) = writer_download(&h.session, &h.xfi, ..128).await;
        assert_eq!(n_bytes, 128);
        assert_eq!(buf, h.data[..128]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_writer_full_range() {
        let h = setup().await;
        let (n_bytes, buf) = writer_download(&h.session, &h.xfi, ..).await;
        assert_eq!(n_bytes, h.data.len() as u64);
        assert_eq!(buf, h.data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_writer_inclusive_range() {
        let h = setup().await;
        let (n_bytes, buf) = writer_download(&h.session, &h.xfi, 50..=149).await;
        assert_eq!(n_bytes, 100);
        assert_eq!(buf, h.data[50..=149]);
    }

    // ── download_stream_range with various range types ───────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_stream_bounded_range() {
        let h = setup().await;
        let collected = stream_download(&h.session, &h.xfi, 100..200).await;
        assert_eq!(collected, h.data[100..200]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_stream_range_from() {
        let h = setup().await;
        let collected = stream_download(&h.session, &h.xfi, 8000..).await;
        assert_eq!(collected, h.data[8000..]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_stream_range_to() {
        let h = setup().await;
        let collected = stream_download(&h.session, &h.xfi, ..128).await;
        assert_eq!(collected, h.data[..128]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_stream_full_range() {
        let h = setup().await;
        let collected = stream_download(&h.session, &h.xfi, ..).await;
        assert_eq!(collected, h.data);
    }

    // ── download_file with unknown size ──────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_download_file_unknown_size() {
        let h = setup().await;
        let xfi_no_size = XetFileInfo::new_hash_only(h.xfi.hash().to_string());

        let out_path = h._base_dir.path().join("unknown_size.bin");
        let (_id, n_bytes) = h.session.download_file(&xfi_no_size, &out_path).await.unwrap();

        assert_eq!(n_bytes, h.data.len() as u64);
        assert_eq!(fs::read(&out_path).unwrap(), h.data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_download_stream_unknown_size() {
        let h = setup().await;
        let xfi_no_size = XetFileInfo::new_hash_only(h.xfi.hash().to_string());
        let collected = stream_download(&h.session, &xfi_no_size, ..).await;
        assert_eq!(collected, h.data);
    }

    // ── size mismatch validation ─────────────────────────────────────────────

    // SizeMismatch is caught after reconstruction completes, but debug
    // assertions inside the progress tracker fire first, so this test only
    // passes in release/test profile without debug_assertions.
    #[cfg(not(debug_assertions))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_download_file_size_mismatch() {
        let h = setup().await;
        let wrong_size = XetFileInfo::new(h.xfi.hash().to_string(), 42);

        let out_path = h._base_dir.path().join("mismatch.bin");
        let err = h.session.download_file(&wrong_size, &out_path).await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("mismatch"), "Expected size mismatch error, got: {msg}");
    }

    // ── range download with unknown file size ────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_writer_range_from_unknown_size() {
        let h = setup().await;
        let xfi_no_size = XetFileInfo::new_hash_only(h.xfi.hash().to_string());
        let (n_bytes, buf) = writer_download(&h.session, &xfi_no_size, 8000..).await;
        assert_eq!(n_bytes, 192);
        assert_eq!(buf, h.data[8000..]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_stream_range_from_unknown_size() {
        let h = setup().await;
        let xfi_no_size = XetFileInfo::new_hash_only(h.xfi.hash().to_string());
        let collected = stream_download(&h.session, &xfi_no_size, 8000..).await;
        assert_eq!(collected, h.data[8000..]);
    }
}
