//! Integration tests for downloading a file while reusing the bytes already present at the
//! destination path (`FileDownloadSession::download_file_reusing_existing`).
//!
//! Small chunk / xorb sizes make a ~200 KB file span many segments, so partial reuse can be
//! observed. With the `simulation` feature the download goes through the HTTP test server
//! (`RemoteClient`, verification hashes carried in the reconstruction response); without it,
//! through the local client directly.

use xet_data::deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS, TARGET_CHUNK_SIZE};
use xet_runtime::test_set_constants;

test_set_constants! {
    TARGET_CHUNK_SIZE = 1024;
    MAX_XORB_BYTES = 16 * (*TARGET_CHUNK_SIZE);
    MAX_XORB_CHUNKS = 16;
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use rand::prelude::*;
    use xet_data::processing::test_utils::TestEnvironment;
    use xet_data::processing::{FileDownloadSession, FileUploadSession, Sha256Policy, XetFileInfo};

    const FILE_SIZE: usize = 200 * 1024;

    fn random_bytes(seed: u64, len: usize) -> Vec<u8> {
        let mut data = vec![0u8; len];
        StdRng::seed_from_u64(seed).fill_bytes(&mut data);
        data
    }

    struct Harness {
        env: TestEnvironment,
        xfi: XetFileInfo,
        data: Vec<u8>,
    }

    struct Outcome {
        content: Vec<u8>,
        transfer_bytes: u64,
        resume_check_bytes: u64,
        resume_check_bytes_completed: u64,
    }

    async fn setup() -> Harness {
        let env = TestEnvironment::new().await;
        let data = random_bytes(0, FILE_SIZE);

        let upload_session = FileUploadSession::new(env.config.clone()).await.unwrap();
        let (_id, mut cleaner) = upload_session
            .start_clean(Some("file".into()), Some(data.len() as u64), Sha256Policy::Compute)
            .unwrap();
        cleaner.add_data(&data).await.unwrap();
        let (xfi, _metrics) = cleaner.finish().await.unwrap();
        upload_session.finalize().await.unwrap();

        Harness { env, xfi, data }
    }

    impl Harness {
        /// Writes `existing` (if any) at the destination, then downloads with reuse.
        async fn download_reusing(&self, name: &str, existing: Option<&[u8]>) -> Outcome {
            let path = self.env.base_dir.join(name);
            if let Some(existing) = existing {
                fs::write(&path, existing).unwrap();
            }

            // A fresh session per download so that transfer counters are per case.
            let session: Arc<FileDownloadSession> =
                FileDownloadSession::new(self.env.config.clone(), None).await.unwrap();
            let (id, n_bytes) = session.download_file_reusing_existing(&self.xfi, &path).await.unwrap();
            assert_eq!(n_bytes, self.data.len() as u64);

            let item = session.item_report(id).unwrap();
            assert_eq!(item.bytes_completed, item.total_bytes);
            let group = session.report();
            assert_eq!(group.total_bytes_completed, group.total_bytes);
            assert_eq!(group.total_transfer_bytes_completed, group.total_transfer_bytes);

            Outcome {
                content: fs::read(&path).unwrap(),
                transfer_bytes: group.total_transfer_bytes,
                resume_check_bytes: item.resume_check_bytes,
                resume_check_bytes_completed: item.resume_check_bytes_completed,
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_reuse_existing() {
        let h = setup().await;

        // Missing file: regular full download, no check.
        let full = h.download_reusing("missing", None).await;
        assert_eq!(full.content, h.data);
        assert_eq!(full.resume_check_bytes, 0);
        assert!(full.transfer_bytes > 0);

        // Empty file: same as missing.
        let empty = h.download_reusing("empty", Some(&[])).await;
        assert_eq!(empty.content, h.data);
        assert_eq!(empty.resume_check_bytes, 0);

        // Interrupted download (prefix ending in the middle of a segment): only the tail is fetched.
        let prefix_len = FILE_SIZE * 2 / 5 + 123;
        let prefix = h.download_reusing("prefix", Some(&h.data[..prefix_len])).await;
        assert_eq!(prefix.content, h.data);
        assert!(prefix.resume_check_bytes > 0 && prefix.resume_check_bytes <= prefix_len as u64);
        assert_eq!(prefix.resume_check_bytes_completed, prefix.resume_check_bytes);
        assert!(prefix.transfer_bytes < full.transfer_bytes * 3 / 4);

        // Already complete: everything is checked, nothing is transferred.
        let complete = h.download_reusing("complete", Some(&h.data)).await;
        assert_eq!(complete.content, h.data);
        assert_eq!(complete.resume_check_bytes, FILE_SIZE as u64);
        assert_eq!(complete.transfer_bytes, 0);

        // One corrupted byte: only the segment containing it is fetched again.
        let mut corrupted = h.data.clone();
        corrupted[FILE_SIZE / 2] ^= 0xFF;
        let corrupted = h.download_reusing("corrupted", Some(&corrupted)).await;
        assert_eq!(corrupted.content, h.data);
        assert!(corrupted.transfer_bytes > 0 && corrupted.transfer_bytes < full.transfer_bytes / 4);

        // Trailing garbage: dropped.
        let mut longer = h.data.clone();
        longer.extend_from_slice(b"trailing garbage");
        let longer = h.download_reusing("longer", Some(&longer)).await;
        assert_eq!(longer.content, h.data);
        assert_eq!(longer.transfer_bytes, 0);

        // Unrelated file of the same size: nothing matches, everything is fetched.
        let unrelated = h.download_reusing("unrelated", Some(&random_bytes(1, FILE_SIZE))).await;
        assert_eq!(unrelated.content, h.data);
        assert_eq!(unrelated.transfer_bytes, full.transfer_bytes);
    }
}
