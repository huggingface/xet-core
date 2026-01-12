use std::time::Duration;

// Run tests that determine deduplication, especially across different test subjects.
use data::FileUploadSession;
use data::configurations::TranslatorConfig;
use deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS, TARGET_CHUNK_SIZE};
use tempfile::TempDir;
use utils::{test_set_config, test_set_constants};

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
    mdb_shard {
        target_size = 1024u64;
    }
}

// Test the deduplication framework.
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data::test_utils::{HydrateDehydrateTest, create_random_file, create_random_files};
    use deduplication::constants::MAX_CHUNK_SIZE;
    use more_asserts::*;
    use progress_tracking::aggregator::AggregatingProgressUpdater;
    use rand::prelude::*;

    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_simple_resume() {
        // Ensure the deduplication numbers are approximately accurate.

        let n = 8 * 1024;
        let half_n = n / 2;

        let hn = half_n as u64;

        // Get a sizable block of random data
        let mut data = vec![0u8; n];
        let mut rng = StdRng::seed_from_u64(0);
        rng.fill(&mut data[..]);

        // Set a temporary directory for the endpoint.
        let cas_dir = TempDir::new().unwrap();

        let config = Arc::new(TranslatorConfig::local_config(cas_dir).unwrap());

        {
            let progress_tracker = AggregatingProgressUpdater::new_aggregation_only();
            let file_upload_session = FileUploadSession::new(config.clone(), Some(progress_tracker.clone()))
                .await
                .unwrap();

            // Feed it half the data, and checkpoint.
            let mut cleaner = file_upload_session
                .start_clean(Some("data".into()), data.len() as u64, None)
                .await;
            cleaner.add_data(&data[..half_n]).await.unwrap();
            cleaner.checkpoint().await.unwrap();

            // Checkpoint to ensure all xorbs get uploaded.
            file_upload_session.checkpoint().await.unwrap();

            // Break without closing down the file session; we should resume partway through.
        }

        // Now try again to test the resume.
        {
            let progress_tracker = AggregatingProgressUpdater::new_aggregation_only();
            let file_upload_session = FileUploadSession::new(config, Some(progress_tracker.clone())).await.unwrap();

            // Feed it half the data, and checkpoint.
            let mut cleaner = file_upload_session
                .start_clean(Some("data".into()), data.len() as u64, None)
                .await;

            // Add all the data.  Roughly the first half should dedup.
            cleaner.add_data(&data).await.unwrap();
            cleaner.finish().await.unwrap();

            // Finalize everything
            file_upload_session.finalize().await.unwrap();

            let progress = progress_tracker.get_aggregated_state().await;

            let max_deviance = (*MAX_XORB_BYTES + *MAX_CHUNK_SIZE) as u64;

            let n = n as u64;

            // Check things. The checkpoint above pushes everything through.
            assert_eq!(progress.total_bytes_completed, n);
            assert_eq!(progress.total_bytes, n);

            // The difference is the amount deduplicated; the half_n pass above should have
            // left quite a bit to deduplicate.
            assert_le!(progress.total_transfer_bytes, hn + max_deviance);
            assert_le!(progress.total_transfer_bytes_completed, hn + max_deviance);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple_resume() {
        // Ensure the deduplication numbers are approximately accurate.

        let n = 256 * 1024;
        let resume_n = [16 * 1024, 16 * 1024, 64 * 1024, 128 * 1024, 240 * 1024];

        let max_deviance = (*MAX_XORB_BYTES + *MAX_CHUNK_SIZE) as u64;

        // Get a sizable block of random data
        let mut data = vec![0u8; n];
        let mut rng = StdRng::seed_from_u64(0);
        rng.fill(&mut data[..]);

        // Set a temporary directory for the endpoint.
        let cas_dir = TempDir::new().unwrap();

        let config = Arc::new(TranslatorConfig::local_config(cas_dir).unwrap());

        let mut prev_rn = 0;

        for rn in resume_n {
            let progress_tracker = AggregatingProgressUpdater::new_aggregation_only();
            let file_upload_session = FileUploadSession::new(config.clone(), Some(progress_tracker.clone()))
                .await
                .unwrap();

            // Feed it half the data, and checkpoint.
            let mut cleaner = file_upload_session
                .start_clean(Some("data".into()), data.len() as u64, None)
                .await;
            cleaner.add_data(&data[..rn]).await.unwrap();
            cleaner.checkpoint().await.unwrap();

            // Checkpoint to ensure all xorbs get uploaded.
            file_upload_session.checkpoint().await.unwrap();

            if prev_rn > 0 {
                let progress = progress_tracker.get_aggregated_state().await;

                // Because some of it may remain in the chunker, so it won't be exact.
                assert_le!(progress.total_bytes_completed, (rn + *MAX_CHUNK_SIZE) as u64);

                // Make sure the total number of transfering bytes isn't fully
                assert_le!(progress.total_transfer_bytes, prev_rn + max_deviance + *MAX_CHUNK_SIZE as u64);
                assert_le!(progress.total_transfer_bytes_completed, prev_rn + max_deviance + *MAX_CHUNK_SIZE as u64);
            }

            // To test the next round.
            prev_rn = rn as u64;

            // Break without closing down the file session; we should resume partway through.
        }

        // Now try again to test the resume.
        {
            let progress_tracker = AggregatingProgressUpdater::new_aggregation_only();
            let file_upload_session = FileUploadSession::new(config, Some(progress_tracker.clone())).await.unwrap();

            // Feed it half the data, and checkpoint.
            let mut cleaner = file_upload_session
                .start_clean(Some("data".into()), data.len() as u64, None)
                .await;

            // Add all the data.  Roughly the first half should dedup.
            cleaner.add_data(&data).await.unwrap();
            cleaner.finish().await.unwrap();

            // Finalize everything
            file_upload_session.finalize().await.unwrap();

            let progress = progress_tracker.get_aggregated_state().await;

            let n = n as u64;

            // Check things. The checkpoint above pushes everything through.
            assert_eq!(progress.total_bytes_completed, n);
            assert_eq!(progress.total_bytes, n);

            // The difference is the amount deduplicated; the half_n pass above should have
            // left quite a bit to deduplicate.
            assert_le!(progress.total_transfer_bytes, prev_rn + max_deviance);
            assert_le!(progress.total_transfer_bytes_completed, prev_rn + max_deviance);
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
            let progress_tracker = AggregatingProgressUpdater::new_aggregation_only();
            let upload_session = ts.new_upload_session(Some(progress_tracker.clone())).await;
            ts.clean_all_files(&upload_session, false).await;

            upload_session.checkpoint().await.unwrap();

            let progress = progress_tracker.get_aggregated_state().await;

            // Check things. The checkpoint above pushes everything through, even though we don't finalize.
            assert_eq!(progress.total_bytes, 64 * 1024);
            assert_eq!(progress.total_bytes_completed, 64 * 1024);

            // Here, all the files would have completed, meaning that all their bytes and xorbs are transfered.
            assert_eq!(progress.total_transfer_bytes, 64 * 1024);
            assert_eq!(progress.total_transfer_bytes_completed, 64 * 1024);

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
            let progress_tracker = AggregatingProgressUpdater::new_aggregation_only();
            let upload_session = ts.new_upload_session(Some(progress_tracker.clone())).await;
            ts.clean_all_files(&upload_session, false).await;

            // Finalize things this time.
            upload_session.finalize().await.unwrap();

            let progress = progress_tracker.get_aggregated_state().await;

            // Check things. The checkpoint above pushes everything through, even though we don't finalize.
            assert_eq!(progress.total_bytes, 128 * 1024);
            assert_eq!(progress.total_bytes_completed, 128 * 1024);

            // Here, all the previous files would have been deduped against, so only the new content would be uploaded.
            assert_eq!(progress.total_transfer_bytes, 64 * 1024);
            assert_eq!(progress.total_transfer_bytes_completed, 64 * 1024);
        }

        // Finally, verify that hydration works successfully.
        ts.hydrate().await;
        ts.verify_src_dest_match();
    }

    /// 4) A single tiny file
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tiny_file_resume() {
        let mut ts = HydrateDehydrateTest::default();

        create_random_file(&ts.src_dir.join("f1"), 128, 0);

        // Clean the files present, but drop the upload session.
        {
            let progress_tracker = AggregatingProgressUpdater::new_aggregation_only();
            let upload_session = ts.new_upload_session(Some(progress_tracker.clone())).await;
            ts.clean_all_files(&upload_session, false).await;

            upload_session.checkpoint().await.unwrap();

            let progress = progress_tracker.get_aggregated_state().await;

            // Check things. The checkpoint above pushes everything through, even though we don't finalize.
            assert_eq!(progress.total_bytes, 128);
            assert_eq!(progress.total_bytes_completed, 128);

            // Here, all the files would have completed, meaning that all their bytes and xorbs are transfered.
            assert_eq!(progress.total_transfer_bytes, 128);
            assert_eq!(progress.total_transfer_bytes_completed, 128);

            // Now interrupt the session and don't call finalize
        }

        create_random_file(&ts.src_dir.join("f2"), 128, 1);

        // Test these files and actually call finalize.
        {
            let progress_tracker = AggregatingProgressUpdater::new_aggregation_only();
            let upload_session = ts.new_upload_session(Some(progress_tracker.clone())).await;
            ts.clean_all_files(&upload_session, false).await;

            // Finalize things this time.
            upload_session.finalize().await.unwrap();

            let progress = progress_tracker.get_aggregated_state().await;

            // Check things. The checkpoint above pushes everything through, even though we don't finalize.
            assert_eq!(progress.total_bytes, 256);
            assert_eq!(progress.total_bytes_completed, 256);

            // Here, all the previous files would have been deduped against, so only the new content would be uploaded.
            assert_eq!(progress.total_transfer_bytes, 128);
            assert_eq!(progress.total_transfer_bytes_completed, 128);
        }

        // Finally, verify that hydration works successfully.
        ts.hydrate().await;
        ts.verify_src_dest_match();
    }
}
