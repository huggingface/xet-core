//! Regression test for the 416 Range Not Satisfiable bug.
//!
//! When downloading a full file without an explicit byte range, `FileReconstructor`
//! used `FileRange::full()` (0..u64::MAX), causing `ReconstructionTermManager` to
//! speculatively prefetch blocks beyond EOF. This made the CAS server return 416
//! for small files.

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use tempfile::TempDir;
    use ulid::Ulid;
    use xet_client::cas_client::LocalTestServerBuilder;
    use xet_data::processing::configurations::TranslatorConfig;
    use xet_data::processing::{FileDownloadSession, FileUploadSession, Sha256Policy, XetFileInfo};

    async fn upload_bytes(upload_session: &Arc<FileUploadSession>, name: &str, data: &[u8]) -> XetFileInfo {
        let mut cleaner = upload_session
            .start_clean(Some(name.into()), Some(data.len() as u64), Sha256Policy::Compute, Ulid::new())
            .await;
        cleaner.add_data(data).await.unwrap();
        let (xfi, _metrics) = cleaner.finish().await.unwrap();
        xfi
    }

    /// Uploads files of various sizes through a LocalTestServer and downloads them
    /// without an explicit byte range, verifying no 416 errors occur.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_full_file_download_no_416() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let test_cases: &[(&str, &[u8])] = &[
            ("one_byte", &[0x42]),
            ("small", b"hello world"),
            ("medium", &vec![0xAB; 4096]),
            ("larger", &vec![0xCD; 64 * 1024]),
        ];

        let download_session = FileDownloadSession::new(config.clone(), None).await.unwrap();

        for (name, data) in test_cases {
            let upload_session = FileUploadSession::new(config.clone(), None).await.unwrap();
            let xfi = upload_bytes(&upload_session, name, data).await;
            upload_session.finalize().await.unwrap();

            let out_path = base_dir.path().join(format!("out_{name}"));
            let n_bytes = download_session.download_file(&xfi, &out_path, Ulid::new()).await.unwrap();

            assert_eq!(n_bytes, data.len() as u64, "size mismatch for {name}");
            assert_eq!(fs::read(&out_path).unwrap(), *data, "content mismatch for {name}");
        }
    }
}
