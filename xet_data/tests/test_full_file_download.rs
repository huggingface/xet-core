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

    use xet_data::processing::test_utils::TestEnvironment;
    use xet_data::processing::{FileDownloadSession, FileUploadSession, Sha256Policy, XetFileInfo};

    async fn upload_bytes(upload_session: &Arc<FileUploadSession>, name: &str, data: &[u8]) -> XetFileInfo {
        let (_id, mut cleaner) = upload_session
            .start_clean(Some(name.into()), Some(data.len() as u64), Sha256Policy::Compute)
            .unwrap();
        cleaner.add_data(data).await.unwrap();
        let (xfi, _metrics) = cleaner.finish().await.unwrap();
        xfi
    }

    /// Uploads files of various sizes and downloads them without an explicit byte
    /// range, verifying no 416 errors occur.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_full_file_download_no_416() {
        let env = TestEnvironment::new().await;

        let test_cases: &[(&str, &[u8])] = &[
            ("one_byte", &[0x42]),
            ("small", b"hello world"),
            ("medium", &vec![0xAB; 4096]),
            ("larger", &vec![0xCD; 64 * 1024]),
        ];

        // Upload all files first in a single session.
        let upload_session = FileUploadSession::new(env.config.clone()).await.unwrap();
        let xfis: Vec<_> = {
            let mut v = Vec::new();
            for (name, data) in test_cases {
                v.push((*name, *data, upload_bytes(&upload_session, name, data).await));
            }
            v
        };
        upload_session.finalize().await.unwrap();

        // Now download and verify each file.
        let download_session = FileDownloadSession::new(env.config.clone()).await.unwrap();
        for (name, data, xfi) in &xfis {
            let out_path = env.base_dir.join(format!("out_{name}"));
            let (_id, n_bytes) = download_session.download_file(xfi, &out_path).await.unwrap();

            assert_eq!(n_bytes, data.len() as u64, "size mismatch for {name}");
            assert_eq!(fs::read(&out_path).unwrap(), *data, "content mismatch for {name}");
        }
    }
}
