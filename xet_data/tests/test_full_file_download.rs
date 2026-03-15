//! Regression test for the 416 Range Not Satisfiable bug.
//!
//! When downloading a full file without an explicit byte range, `FileReconstructor`
//! used `FileRange::full()` (0..u64::MAX), causing `ReconstructionTermManager` to
//! speculatively prefetch blocks beyond EOF. This made the CAS server return 416
//! for small files.
//!
//! These tests upload files of various sizes through a `LocalTestServer` (real HTTP)
//! and download them without an explicit range, verifying no errors occur.

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use tempfile::TempDir;
    use ulid::Ulid;
    use xet_client::cas_client::LocalTestServerBuilder;
    use xet_data::processing::configurations::TranslatorConfig;
    use xet_data::processing::Sha256Policy;
    use xet_data::processing::{FileDownloadSession, FileUploadSession, XetFileInfo};

    async fn setup() -> (xet_client::cas_client::LocalTestServer, TempDir, Arc<TranslatorConfig>) {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config =
            Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());
        (server, base_dir, config)
    }

    async fn upload_bytes(
        upload_session: &Arc<FileUploadSession>,
        name: &str,
        data: &[u8],
    ) -> XetFileInfo {
        let mut cleaner = upload_session
            .start_clean(
                Some(name.into()),
                data.len() as u64,
                Sha256Policy::Compute,
                Ulid::new(),
            )
            .await;
        cleaner.add_data(data).await.unwrap();
        let (xfi, _metrics) = cleaner.finish().await.unwrap();
        xfi
    }

    /// Downloads a full file (no explicit byte range) through LocalTestServer and
    /// verifies no 416 error occurs. This is the exact scenario that triggered the
    /// bug: small files downloaded without a range caused speculative prefetch
    /// beyond EOF.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_full_file_download_small_file_no_416() {
        let (_server, base_dir, config) = setup().await;

        let upload_session = FileUploadSession::new(config.clone(), None).await.unwrap();
        let small_data = b"hello world";
        let xfi = upload_bytes(&upload_session, "small.txt", small_data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config, None).await.unwrap();
        let out_path = base_dir.path().join("downloaded_small.txt");
        let n_bytes = download_session
            .download_file(&xfi, &out_path, Ulid::new())
            .await
            .unwrap();

        assert_eq!(n_bytes, small_data.len() as u64);
        assert_eq!(fs::read(&out_path).unwrap(), small_data);
    }

    /// Same check for a variety of file sizes, including edge cases.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_full_file_download_various_sizes_no_416() {
        let (_server, base_dir, config) = setup().await;

        let test_sizes: Vec<(&str, Vec<u8>)> = vec![
            ("one_byte", vec![0x42]),
            ("few_bytes", b"short".to_vec()),
            ("medium", vec![0xAB; 4096]),
            ("larger", vec![0xCD; 64 * 1024]),
        ];

        let download_session = FileDownloadSession::new(config.clone(), None).await.unwrap();

        for (name, data) in &test_sizes {
            let upload_session = FileUploadSession::new(config.clone(), None).await.unwrap();
            let xfi = upload_bytes(&upload_session, name, data).await;
            upload_session.finalize().await.unwrap();

            let out_path = base_dir.path().join(format!("out_{name}"));
            let n_bytes = download_session
                .download_file(&xfi, &out_path, Ulid::new())
                .await
                .unwrap();

            assert_eq!(n_bytes, data.len() as u64, "size mismatch for {name}");
            assert_eq!(fs::read(&out_path).unwrap(), *data, "content mismatch for {name}");
        }
    }

    /// Verify that streaming download (no explicit range) also works without 416.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_full_file_download_stream_no_416() {
        let (_server, _base_dir, config) = setup().await;

        let upload_session = FileUploadSession::new(config.clone(), None).await.unwrap();
        let data = b"stream download regression test";
        let xfi = upload_bytes(&upload_session, "stream.txt", data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config, None).await.unwrap();
        let mut stream = download_session
            .download_stream(&xfi, Ulid::new())
            .unwrap();

        let mut collected = Vec::new();
        while let Some(chunk) = stream.next().await.unwrap() {
            collected.extend_from_slice(&chunk);
        }

        assert_eq!(collected, data);
    }
}
