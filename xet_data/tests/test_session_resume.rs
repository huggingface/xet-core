use std::time::Duration;

// Run tests that determine deduplication, especially across different test subjects.
use tempfile::TempDir;
use xet_client::cas_client::LocalTestServerBuilder;
use xet_data::deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS, TARGET_CHUNK_SIZE};
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::{FileUploadSession, Sha256Policy};
use xet_runtime::{test_set_config, test_set_constants};

// Runs this test suite with small chunks and xorbs so that we can make sure that all the different edge
// cases are hit.
test_set_constants! {
    TARGET_CHUNK_SIZE = 1024;
    MAX_XORB_CHUNKS = 2;
}

test_set_config! {
    data {
        // Disable the periodic aggregation in the file upload sessions.
        progress_update_interval = Duration::ZERO;

        // Set the maximum xorb flush count to 1 so that every xorb gets flushed to the temporary session
        // pool.
        session_xorb_metadata_flush_max_count = 1;
    }
    metadata_shard {
        target_size = 1024u64;
    }
}

// Test the deduplication framework.
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use more_asserts::*;
    use rand::prelude::*;
    use xet_data::deduplication::constants::MAX_CHUNK_SIZE;
    use xet_data::processing::test_utils::{HydrateDehydrateTest, create_random_file, create_random_files};

    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_simple_resume() {
        // Ensure the deduplication numbers are approximately accurate.

        let n = 8 * 1024;
        let half_n = n / 2;
        let max_deviance = (*MAX_XORB_BYTES + *MAX_CHUNK_SIZE) as u64;

        // Get a sizable block of random data
        let mut data = vec![0u8; n];
        let mut rng = StdRng::seed_from_u64(0);
        rng.fill(&mut data[..]);

        let server = LocalTestServerBuilder::new().start().await;
        let shard_base = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), shard_base.path()).unwrap());

        {
            let file_upload_session = FileUploadSession::new(config.clone()).await.unwrap();

            // Feed it half the data, and checkpoint.
            let (_id, mut cleaner) = file_upload_session
                .start_clean(Some("data".into()), Some(data.len() as u64), Sha256Policy::Compute)
                .unwrap();
            cleaner.add_data(&data[..half_n]).await.unwrap();
            cleaner.checkpoint().await.unwrap();

            // Checkpoint to ensure all xorbs get uploaded.
            file_upload_session.checkpoint().await.unwrap();
            let report = file_upload_session.report();
            assert_eq!(report.total_bytes, n as u64);
            assert_le!(report.total_bytes_completed, half_n as u64 + *MAX_CHUNK_SIZE as u64);
            assert_le!(report.total_transfer_bytes_completed, report.total_transfer_bytes);

            // Break without closing down the file session; we should resume partway through.
        }

        // Now try again to test the resume.
        {
            let file_upload_session = FileUploadSession::new(config).await.unwrap();

            // Feed it half the data, and checkpoint.
            let (_id, mut cleaner) = file_upload_session
                .start_clean(Some("data".into()), Some(data.len() as u64), Sha256Policy::Compute)
                .unwrap();

            // Add all the data.  Roughly the first half should dedup.
            cleaner.add_data(&data).await.unwrap();
            cleaner.finish().await.unwrap();

            let report = file_upload_session.report();
            assert!(report.total_bytes > 0);
            assert_le!(report.total_bytes_completed, report.total_bytes);
            assert_le!(report.total_transfer_bytes_completed, report.total_transfer_bytes);
            assert_le!(report.total_transfer_bytes, half_n as u64 + max_deviance);
            assert_le!(report.total_transfer_bytes_completed, half_n as u64 + max_deviance);
            file_upload_session.finalize().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple_resume() {
        // Ensure the deduplication numbers are approximately accurate.

        let n = 256 * 1024;
        let resume_n = [16 * 1024, 16 * 1024, 64 * 1024, 128 * 1024, 240 * 1024];

        // Get a sizable block of random data
        let mut data = vec![0u8; n];
        let mut rng = StdRng::seed_from_u64(0);
        rng.fill(&mut data[..]);

        let server = LocalTestServerBuilder::new().start().await;
        let shard_base = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), shard_base.path()).unwrap());
        let max_deviance = (*MAX_XORB_BYTES + *MAX_CHUNK_SIZE) as u64;

        let mut prev_rn = 0;

        for rn in resume_n {
            let file_upload_session = FileUploadSession::new(config.clone()).await.unwrap();

            // Feed it half the data, and checkpoint.
            let (_id, mut cleaner) = file_upload_session
                .start_clean(Some("data".into()), Some(data.len() as u64), Sha256Policy::Compute)
                .unwrap();
            cleaner.add_data(&data[..rn]).await.unwrap();
            cleaner.checkpoint().await.unwrap();

            // Checkpoint to ensure all xorbs get uploaded.
            file_upload_session.checkpoint().await.unwrap();
            let report = file_upload_session.report();
            assert_eq!(report.total_bytes, n as u64);
            assert_le!(report.total_bytes_completed, rn as u64 + *MAX_CHUNK_SIZE as u64);
            assert_le!(report.total_transfer_bytes_completed, report.total_transfer_bytes);

            // To test the next round.
            prev_rn = rn as u64;

            // Break without closing down the file session; we should resume partway through.
        }

        // Now try again to test the resume.
        {
            let file_upload_session = FileUploadSession::new(config).await.unwrap();

            // Feed it half the data, and checkpoint.
            let (_id, mut cleaner) = file_upload_session
                .start_clean(Some("data".into()), Some(data.len() as u64), Sha256Policy::Compute)
                .unwrap();

            // Add all the data.  Roughly the first half should dedup.
            cleaner.add_data(&data).await.unwrap();
            cleaner.finish().await.unwrap();

            let report = file_upload_session.report();
            assert!(report.total_bytes > 0);
            assert_le!(report.total_bytes_completed, report.total_bytes);
            assert_le!(report.total_transfer_bytes_completed, report.total_transfer_bytes);
            assert_le!(report.total_transfer_bytes, prev_rn + max_deviance);
            assert_le!(report.total_transfer_bytes_completed, prev_rn + max_deviance);
            file_upload_session.finalize().await.unwrap();
        }
    }

    /// 3) many files, each with a unique portion plus a large common portion bigger than MAX_XORB_BYTES/2.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_partial_directory_upload_with_rehydrate() {
        let mut ts = HydrateDehydrateTest::default();

        create_random_files(
            &ts.src_dir,
            &[
                ("f1", 16 * 1024),
                ("f2", 16 * 1024),
                ("f3", 16 * 1024),
                ("f4", 16 * 1024),
            ],
            0,
        );

        // Clean the files present, but drop the upload session.
        {
            let upload_session = ts.new_upload_session().await;
            ts.clean_all_files(&upload_session, false).await;

            upload_session.checkpoint().await.unwrap();
            let report = upload_session.report();
            assert!(report.total_bytes > 0);
            assert_le!(report.total_bytes_completed, report.total_bytes);
            assert_le!(report.total_transfer_bytes_completed, report.total_transfer_bytes);

            // Now interrupt the session and don't call finalize
        }

        // Add more files.
        create_random_files(
            &ts.src_dir,
            &[
                ("f5", 16 * 1024),
                ("f6", 16 * 1024),
                ("f7", 16 * 1024),
                ("f8", 16 * 1024),
            ],
            1, // new seed
        );

        // Test these files and actually call finalize.
        {
            let upload_session = ts.new_upload_session().await;
            ts.clean_all_files(&upload_session, false).await;

            let report = upload_session.report();
            assert!(report.total_bytes > 0);
            assert_le!(report.total_bytes_completed, report.total_bytes);
            assert_le!(report.total_transfer_bytes_completed, report.total_transfer_bytes);
            upload_session.finalize().await.unwrap();
        }

        // Finally, verify that hydration works successfully.
        ts.hydrate().await;
        ts.verify_src_dest_match();
        ts.hydrate_partitioned_writers(4).await;
        ts.verify_src_dest_match();
    }

    /// 4) A single tiny file
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tiny_file_resume() {
        let mut ts = HydrateDehydrateTest::default();

        create_random_file(ts.src_dir.join("f1"), 128, 0);

        // Clean the files present, but drop the upload session.
        {
            let upload_session = ts.new_upload_session().await;
            ts.clean_all_files(&upload_session, false).await;

            upload_session.checkpoint().await.unwrap();
            let report = upload_session.report();
            assert!(report.total_bytes > 0);
            assert_le!(report.total_bytes_completed, report.total_bytes);
            assert_le!(report.total_transfer_bytes_completed, report.total_transfer_bytes);

            // Now interrupt the session and don't call finalize
        }

        create_random_file(ts.src_dir.join("f2"), 128, 1);

        // Test these files and actually call finalize.
        {
            let upload_session = ts.new_upload_session().await;
            ts.clean_all_files(&upload_session, false).await;

            let report = upload_session.report();
            assert!(report.total_bytes > 0);
            assert_le!(report.total_bytes_completed, report.total_bytes);
            assert_le!(report.total_transfer_bytes_completed, report.total_transfer_bytes);
            upload_session.finalize().await.unwrap();
        }

        // Finally, verify that hydration works successfully.
        ts.hydrate().await;
        ts.verify_src_dest_match();
        ts.hydrate_partitioned_writers(4).await;
        ts.verify_src_dest_match();
    }
}
