use std::sync::Arc;

use cas_client::Client;
use cas_types::FileRange;
use merklehash::MerkleHash;
use progress_tracking::download_tracking::DownloadTaskUpdater;
use tracing::{debug, info};
use utils::ResourceSemaphore;
use xet_config::ReconstructionConfig;
use xet_runtime::{XetRuntime, xet_config};

use crate::data_writer::{DataOutput, new_data_writer};
use crate::error::{FileReconstructionError, Result};
use crate::reconstruction_terms::ReconstructionTermManager;

/// Reconstructs a file from its content-addressed chunks by downloading xorb blocks
/// and writing the reassembled data to an output. Supports byte range requests and
/// uses memory-limited buffering with adaptive prefetching.
pub struct FileReconstructor {
    client: Arc<dyn Client>,
    file_hash: MerkleHash,
    byte_range: Option<FileRange>,
    output: DataOutput,
    progress_updater: Option<Arc<DownloadTaskUpdater>>,
    config: Arc<ReconstructionConfig>,

    /// Custom buffer semaphore for testing or specialized use cases.
    custom_buffer_semaphore: Option<ResourceSemaphore>,
}

impl FileReconstructor {
    pub fn new(client: &Arc<dyn Client>, file_hash: MerkleHash, output: DataOutput) -> Self {
        Self {
            client: client.clone(),
            file_hash,
            byte_range: None,
            output,
            progress_updater: {
                #[cfg(debug_assertions)]
                {
                    // When debug assertions are enabled, we want to verify that all the reconstruction
                    // information is correct.  However, we don't want to pay the cost of the verification
                    // in release mode.
                    Some(DownloadTaskUpdater::correctness_verification_tracker())
                }
                #[cfg(not(debug_assertions))]
                {
                    None
                }
            },
            config: Arc::new(xet_config().reconstruction.clone()),
            custom_buffer_semaphore: None,
        }
    }

    pub fn with_byte_range(self, byte_range: FileRange) -> Self {
        Self {
            byte_range: Some(byte_range),
            ..self
        }
    }

    pub fn with_progress_updater(self, progress_updater: Arc<DownloadTaskUpdater>) -> Self {
        Self {
            progress_updater: Some(progress_updater),
            ..self
        }
    }

    pub fn with_config(self, config: impl AsRef<ReconstructionConfig>) -> Self {
        Self {
            config: Arc::new(config.as_ref().clone()),
            ..self
        }
    }

    /// Sets a custom buffer semaphore for controlling download buffer memory usage.
    /// This is primarily useful for testing scenarios where you want to control
    /// the timing of term fetches by limiting buffer capacity.
    pub fn with_buffer_semaphore(self, semaphore: ResourceSemaphore) -> Self {
        Self {
            custom_buffer_semaphore: Some(semaphore),
            ..self
        }
    }

    /// Runs the file reconstruction.
    /// Returns the number of bytes written.
    pub async fn run(self) -> Result<u64> {
        info!(
            file_hash = %self.file_hash,
            byte_range = ?self.byte_range,
            "Starting file reconstruction"
        );

        let Self {
            client,
            file_hash,
            byte_range,
            output,
            progress_updater,
            config,
            custom_buffer_semaphore,
        } = self;

        let requested_range = byte_range.unwrap_or_else(FileRange::full);

        let mut term_manager = ReconstructionTermManager::new(
            config.clone(),
            client.clone(),
            file_hash,
            requested_range,
            progress_updater.clone(),
        )
        .await?;

        let data_writer = new_data_writer(output, requested_range.start, &config, progress_updater.clone())?;

        // Determine buffer semaphore based on whether a custom one is provided.
        let download_buffer_semaphore = custom_buffer_semaphore.unwrap_or_else(|| {
            let rt = XetRuntime::current();
            rt.common().reconstruction_download_buffer.clone()
        });

        // The range start offset - we need to adjust byte ranges to be relative to this.
        let range_start_offset = requested_range.start;

        // Tracking for summary stats.
        let mut total_terms_processed: u64 = 0;
        let mut total_bytes_scheduled: u64 = 0;
        let mut block_count: u64 = 0;

        // Outer loop: retrieve blocks of file terms.
        while let Some(file_terms) = term_manager.next_file_terms().await? {
            let block_term_count = file_terms.len();
            let block_start = file_terms.first().map(|t| t.byte_range.start).unwrap_or(0);
            let block_end = file_terms.last().map(|t| t.byte_range.end).unwrap_or(0);
            let block_bytes = block_end.saturating_sub(block_start);

            block_count += 1;
            info!(
                block_number = block_count,
                term_count = block_term_count,
                block_byte_range = ?(block_start, block_end),
                block_bytes,
                "Begin processing on block of file terms."
            );

            // Inner loop: process each file term in the block.
            for file_term in file_terms {
                // Calculate the number of bytes this term will use.
                let term_size = file_term.byte_range.end - file_term.byte_range.start;

                debug!(
                    xorb_hash = %file_term.xorb_block.xorb_hash,
                    term_byte_range = ?(file_term.byte_range.start, file_term.byte_range.end),
                    term_size,
                    "Processing file term"
                );

                // Acquire a permit from the memory limiter for the memory needed for
                // the next term. The ResourceSemaphore handles scaling and clamping.
                let buffer_permit = download_buffer_semaphore.acquire_owned(term_size).await.map_err(|e| {
                    FileReconstructionError::InternalError(format!("Error acquiring download buffer permit: {e}"))
                })?;

                // Get the data future that will retrieve the term's data.
                let data_future = file_term.get_data_task(client.clone(), progress_updater.clone()).await?;

                #[cfg(debug_assertions)]
                {
                    let refs = &file_term.xorb_block.references;
                    assert!(refs.iter().any(|r| r.term_chunks == file_term.xorb_chunk_range));
                }

                // Adjust byte range to be relative to the requested range start (writer expects 0-based ranges).
                let relative_byte_range = FileRange::new(
                    file_term.byte_range.start - range_start_offset,
                    file_term.byte_range.end - range_start_offset,
                );

                // Set the data source for this term, passing the buffer permit
                // so it gets released after the data is written.
                data_writer
                    .set_next_term_data_source(relative_byte_range, Some(buffer_permit), data_future)
                    .await?;

                total_terms_processed += 1;
                total_bytes_scheduled += term_size;
            }
        }

        info!(
            block_count,
            total_terms_processed, total_bytes_scheduled, "All term blocks received and scheduled for writing"
        );

        // Finish the data writer and wait for all data to be written.
        let bytes_written = data_writer.finish().await?;

        debug_assert_eq!(
            bytes_written, total_bytes_scheduled,
            "Bytes written ({bytes_written}) should match total bytes scheduled ({total_bytes_scheduled})"
        );

        info!(bytes_written, "File reconstruction completed successfully");

        #[cfg(debug_assertions)]
        {
            // This verifies that the when the user asked us for a byte range, we actually downloaded and
            // recorded the correct byte range for that part.
            if let Some(ref updater) = progress_updater {
                updater.assert_complete();
                if let Some(byte_range) = byte_range {
                    assert_eq!(updater.total_bytes_completed(), byte_range.end - byte_range.start);
                }
            }
        }

        Ok(bytes_written)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use cas_client::{ClientTestingUtils, DirectAccessClient, LocalClient, RandomFileContents};
    use cas_types::FileRange;
    use progress_tracking::NoOpProgressUpdater;
    use progress_tracking::download_tracking::DownloadProgressTracker;

    use super::*;

    const TEST_CHUNK_SIZE: usize = 101;

    /// Creates a test config with small fetch sizes to force multiple iterations.
    fn test_config() -> ReconstructionConfig {
        let mut config = ReconstructionConfig::default();
        // Use small fetch sizes to force multiple prefetch iterations
        config.min_reconstruction_fetch_size = utils::ByteSize::from("100");
        config.max_reconstruction_fetch_size = utils::ByteSize::from("400");
        config.min_prefetch_buffer = utils::ByteSize::from("800");
        config
    }

    /// Creates a test client and uploads a random file with the given term specification.
    async fn setup_test_file(term_spec: &[(u64, (u64, u64))]) -> (Arc<LocalClient>, RandomFileContents) {
        let client = LocalClient::temporary().await.unwrap();
        let file_contents = client.upload_random_file(term_spec, TEST_CHUNK_SIZE).await.unwrap();
        (client, file_contents)
    }

    /// Reconstructs a file (or byte range) using a writer and returns the reconstructed data.
    async fn reconstruct_to_vec(
        client: &Arc<LocalClient>,
        file_hash: MerkleHash,
        byte_range: Option<FileRange>,
        config: &ReconstructionConfig,
    ) -> Result<Vec<u8>> {
        let buffer = Arc::new(std::sync::Mutex::new(Cursor::new(Vec::new())));
        let writer = StaticCursorWriter(buffer.clone());

        let mut reconstructor =
            FileReconstructor::new(&(client.clone() as Arc<dyn Client>), file_hash, DataOutput::writer(writer))
                .with_config(config);

        if let Some(range) = byte_range {
            reconstructor = reconstructor.with_byte_range(range);
        }

        reconstructor.run().await?;

        let data = buffer.lock().unwrap().get_ref().clone();
        Ok(data)
    }

    /// Reconstructs to a file (using DataOutput::file) and returns the reconstructed data.
    /// Creates a temp file, reconstructs to it, then reads the relevant portion back.
    async fn reconstruct_to_file(
        client: &Arc<LocalClient>,
        file_hash: MerkleHash,
        byte_range: Option<FileRange>,
        config: &ReconstructionConfig,
    ) -> Result<Vec<u8>> {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("output.bin");

        let mut reconstructor = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_hash,
            DataOutput::write_in_file(&file_path),
        )
        .with_config(config);

        if let Some(range) = byte_range {
            reconstructor = reconstructor.with_byte_range(range);
        }

        reconstructor.run().await?;

        // Read back the data from the file at the expected location.
        let file_data = std::fs::read(&file_path)?;
        let start = byte_range.map(|r| r.start as usize).unwrap_or(0);
        Ok(file_data[start..].to_vec())
    }

    /// Reconstructs to a file at offset 0 (using DataOutput::file_at_offset) and returns the data.
    /// This tests writing to the beginning of a file regardless of the byte range.
    async fn reconstruct_to_file_at_specific_offset(
        client: &Arc<LocalClient>,
        file_hash: MerkleHash,
        byte_range: Option<FileRange>,
        config: &ReconstructionConfig,
    ) -> Result<Vec<u8>> {
        let offset = 9;

        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("output.bin");

        let mut reconstructor = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_hash,
            DataOutput::write_file_at_offset(&file_path, offset as u64), // Write at offset 9.
        )
        .with_config(config);

        if let Some(range) = byte_range {
            reconstructor = reconstructor.with_byte_range(range);
        }

        reconstructor.run().await?;

        // Read back all file data (it starts at offset 0).
        let file_data = std::fs::read(&file_path)?;
        Ok(file_data[offset..].to_vec())
    }

    /// Reconstructs to a file at offset 0 (using DataOutput::file_at_offset) and returns the data.
    /// This tests writing to the beginning of a file regardless of the byte range.
    async fn reconstruct_to_file_at_offset_zero(
        client: &Arc<LocalClient>,
        file_hash: MerkleHash,
        byte_range: Option<FileRange>,
        config: &ReconstructionConfig,
    ) -> Result<Vec<u8>> {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("output.bin");

        let mut reconstructor = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_hash,
            DataOutput::write_file_at_offset(&file_path, 0), // Write at offset 9.
        )
        .with_config(config);

        if let Some(range) = byte_range {
            reconstructor = reconstructor.with_byte_range(range);
        }

        reconstructor.run().await?;

        // Read back all file data (it starts at offset 0).
        let file_data = std::fs::read(&file_path)?;
        Ok(file_data)
    }

    /// A wrapper that allows writing to a shared Vec; needed for testing
    /// with the 'static cursor writer present in the code.
    struct StaticCursorWriter(Arc<std::sync::Mutex<Cursor<Vec<u8>>>>);

    impl std::io::Write for StaticCursorWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.0.lock().unwrap().flush()
        }
    }

    /// Reconstructs and verifies the full file using all output methods and vectored/non-vectored writes.
    async fn reconstruct_and_verify_full(
        client: &Arc<LocalClient>,
        file_contents: &RandomFileContents,
        base_config: ReconstructionConfig,
    ) {
        let expected = &file_contents.data;
        let h = file_contents.file_hash;

        // Test both vectored and non-vectored write paths.
        for use_vectored in [false, true] {
            let mut config = base_config.clone();
            config.use_vectored_write = use_vectored;

            // Test 1: reconstruct_to_vec (DataOutput::writer)
            let vec_result = reconstruct_to_vec(client, h, None, &config).await.unwrap();
            assert_eq!(vec_result, *expected, "vec failed (vectored={use_vectored})");

            // Test 2: reconstruct_to_file (DataOutput::file)
            let file_result = reconstruct_to_file(client, h, None, &config).await.unwrap();
            assert_eq!(file_result, *expected, "file failed (vectored={use_vectored})");

            // Test 3: reconstruct_to_file_at_offset_zero (DataOutput::file_at_offset)
            let file_offset_result = reconstruct_to_file_at_offset_zero(client, h, None, &config).await.unwrap();
            assert_eq!(file_offset_result, *expected, "file_at_offset_zero failed (vectored={use_vectored})");

            // Test 4: reconstruct_to_file_at_specific_offset
            let file_specific_result = reconstruct_to_file_at_specific_offset(client, h, None, &config).await.unwrap();
            assert_eq!(file_specific_result, *expected, "file_at_specific_offset failed (vectored={use_vectored})");
        }
    }

    /// Reconstructs and verifies a byte range using all output methods and vectored/non-vectored writes.
    async fn reconstruct_and_verify_range(
        client: &Arc<LocalClient>,
        file_contents: &RandomFileContents,
        range: FileRange,
        base_config: ReconstructionConfig,
    ) {
        let expected = &file_contents.data[range.start as usize..range.end as usize];

        // Test both vectored and non-vectored write paths.
        for use_vectored in [false, true] {
            let mut config = base_config.clone();
            config.use_vectored_write = use_vectored;

            // Test 1: reconstruct_to_vec (DataOutput::writer)
            let vec_result = reconstruct_to_vec(client, file_contents.file_hash, Some(range), &config)
                .await
                .expect("reconstruct_to_vec should succeed");
            assert_eq!(vec_result, expected, "vec failed (vectored={use_vectored})");

            // Test 2: reconstruct_to_file (DataOutput::file)
            let file_result = reconstruct_to_file(client, file_contents.file_hash, Some(range), &config)
                .await
                .expect("reconstruct_to_file should succeed");
            assert_eq!(file_result, expected, "file failed (vectored={use_vectored})");

            // Test 3: reconstruct_to_file_at_offset_zero (DataOutput::file_at_offset)
            let file_offset_result =
                reconstruct_to_file_at_offset_zero(client, file_contents.file_hash, Some(range), &config)
                    .await
                    .expect("reconstruct_to_file_at_offset_zero should succeed");
            assert_eq!(file_offset_result, expected, "file_at_offset failed (vectored={use_vectored})");
        }
    }

    // ==================== Full File Reconstruction Tests ====================

    #[tokio::test]
    async fn test_single_term_full_reconstruction() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 3))]).await;
        reconstruct_and_verify_full(&client, &file_contents, test_config()).await;
    }

    #[tokio::test]
    async fn test_multiple_terms_same_xorb_full_reconstruction() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 2)), (1, (2, 4)), (1, (4, 6))]).await;
        reconstruct_and_verify_full(&client, &file_contents, test_config()).await;
    }

    #[tokio::test]
    async fn test_multiple_xorbs_full_reconstruction() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 3)), (2, (0, 2)), (3, (0, 4))]).await;
        reconstruct_and_verify_full(&client, &file_contents, test_config()).await;
    }

    #[tokio::test]
    async fn test_large_file_many_terms_full_reconstruction() {
        // Create a file large enough to require multiple prefetch iterations
        let term_spec: Vec<(u64, (u64, u64))> = (1..=10).map(|i| (i, (0, 5))).collect();
        let (client, file_contents) = setup_test_file(&term_spec).await;
        reconstruct_and_verify_full(&client, &file_contents, test_config()).await;
    }

    #[tokio::test]
    async fn test_interleaved_xorbs_full_reconstruction() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 2)), (2, (0, 2)), (1, (2, 4)), (2, (2, 4))]).await;
        reconstruct_and_verify_full(&client, &file_contents, test_config()).await;
    }

    #[tokio::test]
    async fn test_single_chunk_file() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 1))]).await;
        reconstruct_and_verify_full(&client, &file_contents, test_config()).await;
    }

    #[tokio::test]
    async fn test_many_small_terms_different_xorbs() {
        let term_spec: Vec<(u64, (u64, u64))> = (1..=20).map(|i| (i, (0, 1))).collect();
        let (client, file_contents) = setup_test_file(&term_spec).await;
        reconstruct_and_verify_full(&client, &file_contents, test_config()).await;
    }

    // ==================== Progress tracker tests ====================

    #[tokio::test]
    async fn test_progress_tracker_records_full_reconstruction_bytes() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 3)), (2, (0, 2))]).await;
        let config = test_config();
        let buffer = Arc::new(std::sync::Mutex::new(Cursor::new(Vec::new())));
        let writer = StaticCursorWriter(buffer.clone());

        let progress_updater =
            DownloadProgressTracker::new(NoOpProgressUpdater::new()).new_download_task(Arc::from("file"));
        let bytes_written = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
            DataOutput::writer(writer),
        )
        .with_config(&config)
        .with_progress_updater(progress_updater.clone())
        .run()
        .await
        .unwrap();

        assert_eq!(bytes_written, file_contents.data.len() as u64);
    }

    #[tokio::test]
    async fn test_progress_tracker_records_partial_range_bytes() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 10))]).await;
        let config = test_config();
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(file_len / 4, file_len * 3 / 4);
        let expected_bytes = range.end - range.start;

        let buffer = Arc::new(std::sync::Mutex::new(Cursor::new(Vec::new())));
        let writer = StaticCursorWriter(buffer.clone());

        let progress_updater =
            DownloadProgressTracker::new(NoOpProgressUpdater::new()).new_download_task(Arc::from("file"));
        let bytes_written = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
            DataOutput::writer(writer),
        )
        .with_config(&config)
        .with_byte_range(range)
        .with_progress_updater(progress_updater.clone())
        .run()
        .await
        .unwrap();

        assert_eq!(bytes_written, expected_bytes);
    }

    /// Verifies the external progress tracker flow without a known file size:
    /// totals are discovered incrementally by the ReconstructionTermManager.
    #[tokio::test]
    async fn test_external_progress_tracker_incremental_discovery() {
        let term_spec: Vec<(u64, (u64, u64))> = (1..=5).map(|i| (i, (0, 3))).collect();
        let (client, file_contents) = setup_test_file(&term_spec).await;
        let config = test_config();

        let tracker = DownloadProgressTracker::new(NoOpProgressUpdater::new());
        let task = tracker.new_download_task(Arc::from("test_file.bin"));

        let buffer = Arc::new(std::sync::Mutex::new(Cursor::new(Vec::new())));
        let writer = StaticCursorWriter(buffer.clone());

        let bytes_written = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
            DataOutput::writer(writer),
        )
        .with_config(&config)
        .with_progress_updater(task.clone())
        .run()
        .await
        .unwrap();

        assert_eq!(bytes_written, file_contents.data.len() as u64);

        task.assert_complete();
        assert_eq!(task.total_bytes_completed(), file_contents.data.len() as u64);

        tracker.assert_complete();
    }

    /// Verifies the data_client.rs flow: file size is known upfront (is_final=true),
    /// then the manager discovers transfer sizes and also tries to update_item_size
    /// (which is ignored since final was already set).
    #[tokio::test]
    async fn test_external_progress_tracker_final_size_upfront() {
        let term_spec: Vec<(u64, (u64, u64))> = (1..=5).map(|i| (i, (0, 3))).collect();
        let (client, file_contents) = setup_test_file(&term_spec).await;
        let config = test_config();
        let file_size = file_contents.data.len() as u64;

        let tracker = DownloadProgressTracker::new(NoOpProgressUpdater::new());
        let task = tracker.new_download_task(Arc::from("test_file.bin"));

        // Simulate data_client.rs: set final size before reconstruction.
        task.update_item_size(file_size, true);

        let buffer = Arc::new(std::sync::Mutex::new(Cursor::new(Vec::new())));
        let writer = StaticCursorWriter(buffer.clone());

        let bytes_written = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
            DataOutput::writer(writer),
        )
        .with_config(&config)
        .with_progress_updater(task.clone())
        .run()
        .await
        .unwrap();

        assert_eq!(bytes_written, file_size);

        // item_bytes should still be file_size (manager's update_item_size calls were ignored).
        assert_eq!(task.total_bytes_completed(), file_size);

        task.assert_complete();
        tracker.assert_complete();
    }

    // ==================== Byte Range Reconstruction Tests ====================

    #[tokio::test]
    async fn test_range_first_half() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 10))]).await;
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(0, file_len / 2);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    #[tokio::test]
    async fn test_range_second_half() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 10))]).await;
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(file_len / 2, file_len);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    #[tokio::test]
    async fn test_range_middle() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 10))]).await;
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(file_len / 4, file_len * 3 / 4);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    #[tokio::test]
    async fn test_range_single_byte_start() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 5))]).await;
        let range = FileRange::new(0, 1);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    #[tokio::test]
    async fn test_range_single_byte_end() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 5))]).await;
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(file_len - 1, file_len);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    #[tokio::test]
    async fn test_range_single_byte_middle() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 5))]).await;
        let file_len = file_contents.data.len() as u64;
        let mid = file_len / 2;
        let range = FileRange::new(mid, mid + 1);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    #[tokio::test]
    async fn test_range_few_bytes_from_start() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 5))]).await;
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(3, file_len);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    #[tokio::test]
    async fn test_range_few_bytes_before_end() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 5))]).await;
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(0, file_len - 3);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    #[tokio::test]
    async fn test_range_small_slice_in_middle() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 10))]).await;
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(file_len / 3, file_len / 3 + 10);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    // ==================== Multi-term Range Tests ====================

    #[tokio::test]
    async fn test_range_spanning_multiple_terms() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 3)), (2, (0, 3)), (3, (0, 3))]).await;
        let file_len = file_contents.data.len() as u64;
        // Range that spans all three terms but not full file
        let range = FileRange::new(10, file_len - 10);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    #[tokio::test]
    async fn test_range_within_single_term() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 10)), (2, (0, 10))]).await;
        // First term size
        let first_term_size = file_contents.terms[0].data.len() as u64;
        // Range within the first term only
        let range = FileRange::new(5, first_term_size - 5);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    #[tokio::test]
    async fn test_range_crossing_term_boundary() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 5)), (2, (0, 5))]).await;
        let first_term_size = file_contents.terms[0].data.len() as u64;
        // Range that straddles the boundary between terms
        let range = FileRange::new(first_term_size - 10, first_term_size + 10);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    // ==================== Edge Cases with Multiple Prefetch Iterations ====================

    #[tokio::test]
    async fn test_large_file_range_first_portion() {
        // Large file to ensure multiple prefetch iterations
        let term_spec: Vec<(u64, (u64, u64))> = (1..=15).map(|i| (i, (0, 4))).collect();
        let (client, file_contents) = setup_test_file(&term_spec).await;
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(0, file_len / 3);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    #[tokio::test]
    async fn test_large_file_range_last_portion() {
        let term_spec: Vec<(u64, (u64, u64))> = (1..=15).map(|i| (i, (0, 4))).collect();
        let (client, file_contents) = setup_test_file(&term_spec).await;
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(file_len * 2 / 3, file_len);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    #[tokio::test]
    async fn test_large_file_range_middle_portion() {
        let term_spec: Vec<(u64, (u64, u64))> = (1..=15).map(|i| (i, (0, 4))).collect();
        let (client, file_contents) = setup_test_file(&term_spec).await;
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(file_len / 3, file_len * 2 / 3);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    // ==================== Complex File Structures ====================

    #[tokio::test]
    async fn test_complex_mixed_pattern_full() {
        let term_spec = &[
            (1, (0, 3)),
            (2, (0, 2)),
            (1, (3, 5)),
            (3, (1, 4)),
            (2, (4, 6)),
            (1, (0, 2)),
        ];
        let (client, file_contents) = setup_test_file(term_spec).await;
        reconstruct_and_verify_full(&client, &file_contents, test_config()).await;
    }

    #[tokio::test]
    async fn test_complex_mixed_pattern_partial_range() {
        let term_spec = &[
            (1, (0, 3)),
            (2, (0, 2)),
            (1, (3, 5)),
            (3, (1, 4)),
            (2, (4, 6)),
            (1, (0, 2)),
        ];
        let (client, file_contents) = setup_test_file(term_spec).await;
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(file_len / 4, file_len * 3 / 4);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    #[tokio::test]
    async fn test_overlapping_chunk_ranges() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 5)), (1, (1, 3)), (1, (2, 4))]).await;
        reconstruct_and_verify_full(&client, &file_contents, test_config()).await;
    }

    #[tokio::test]
    async fn test_non_contiguous_chunks() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 2)), (1, (4, 6))]).await;
        reconstruct_and_verify_full(&client, &file_contents, test_config()).await;
    }

    // ==================== Default Config Tests ====================

    #[tokio::test]
    async fn test_default_config_full_reconstruction() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 5)), (2, (0, 3))]).await;
        // Use default config (larger fetch sizes)
        reconstruct_and_verify_full(&client, &file_contents, ReconstructionConfig::default()).await;
    }

    #[tokio::test]
    async fn test_default_config_partial_range() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 5)), (2, (0, 3))]).await;
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(file_len / 4, file_len * 3 / 4);
        reconstruct_and_verify_range(&client, &file_contents, range, ReconstructionConfig::default()).await;
    }

    // ==================== URL Refresh Tests ====================
    //
    // These tests verify that URL refresh logic works correctly when URLs expire.
    // We use tokio's time advancement (start_paused = true) to control time precisely.

    /// A writer that advances tokio time after each write, causing URL expiration.
    /// This forces the reconstruction logic to refresh URLs for subsequent fetches.
    struct TimeAdvancingWriter {
        buffer: Arc<std::sync::Mutex<Vec<u8>>>,
        advance_duration: Duration,
        write_count: Arc<AtomicUsize>,
    }

    impl TimeAdvancingWriter {
        fn new(advance_duration: Duration) -> Self {
            Self {
                buffer: Arc::new(std::sync::Mutex::new(Vec::new())),
                advance_duration,
                write_count: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl Write for TimeAdvancingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let bytes_written = self.buffer.lock().unwrap().write(buf)?;

            // Increment write count
            self.write_count.fetch_add(1, Ordering::Relaxed);

            // Advance tokio time to cause URL expiration for next fetch.
            // Use Handle::block_on directly since we're in a spawn_blocking context
            // (block_in_place is not allowed from blocking threads).
            let advance_duration = self.advance_duration;
            tokio::runtime::Handle::current().block_on(async {
                tokio::time::advance(advance_duration).await;
            });

            Ok(bytes_written)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Creates a config with very small fetch sizes to ensure we get multiple terms.
    fn url_refresh_test_config() -> ReconstructionConfig {
        let mut config = ReconstructionConfig::default();
        // Very small fetch sizes to force multiple term blocks
        config.min_reconstruction_fetch_size = utils::ByteSize::from("50");
        config.max_reconstruction_fetch_size = utils::ByteSize::from("100");
        config.min_prefetch_buffer = utils::ByteSize::from("50");
        config
    }

    /// Test that URL refresh works correctly when URLs expire between term fetches.
    /// Uses a tiny buffer semaphore (1 byte) to force sequential term processing,
    /// and advances time after each write to cause URL expiration.
    #[tokio::test(start_paused = true)]
    async fn test_url_refresh_on_expiration() {
        // Create a file with multiple terms from multiple xorbs
        let term_spec = &[(1, (0, 2)), (2, (0, 2)), (3, (0, 2))];
        let (client, file_contents) = setup_test_file(term_spec).await;

        // Set a short URL expiration (1 second)
        let url_expiration = Duration::from_secs(1);
        client.set_fetch_term_url_expiration(url_expiration);

        // Create a writer that advances time by more than the expiration after each write
        let time_advance = Duration::from_secs(2);
        let writer = TimeAdvancingWriter::new(time_advance);
        let writer_buffer = writer.buffer.clone();
        let write_count = writer.write_count.clone();

        // Create a tiny semaphore (1 permit) to force sequential processing
        // This ensures each term is fully written before the next is fetched
        let tiny_semaphore = ResourceSemaphore::new(1);

        let reconstructor = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
            DataOutput::SequentialWriter(Box::new(writer)),
        )
        .with_config(url_refresh_test_config())
        .with_buffer_semaphore(tiny_semaphore);

        // Run reconstruction - this should trigger URL refreshes
        reconstructor
            .run()
            .await
            .expect("Reconstruction should succeed with URL refresh");

        // Verify the reconstructed data is correct
        let reconstructed = writer_buffer.lock().unwrap().clone();
        assert_eq!(reconstructed.len(), file_contents.data.len());
        assert_eq!(reconstructed, file_contents.data);

        // Verify we had multiple writes (one per term at minimum)
        assert!(write_count.load(Ordering::Relaxed) >= term_spec.len());
    }

    /// Test URL refresh with a single xorb but multiple terms.
    /// This tests the case where the cached xorb data should still be valid
    /// but the URL needs refreshing.
    #[tokio::test(start_paused = true)]
    async fn test_url_refresh_same_xorb_multiple_terms() {
        // Create multiple terms from the same xorb
        let term_spec = &[(1, (0, 2)), (1, (2, 4)), (1, (4, 6))];
        let (client, file_contents) = setup_test_file(term_spec).await;

        // Set a short URL expiration
        client.set_fetch_term_url_expiration(Duration::from_secs(1));

        // Create a writer that advances time
        let writer = TimeAdvancingWriter::new(Duration::from_secs(2));
        let writer_buffer = writer.buffer.clone();

        let tiny_semaphore = ResourceSemaphore::new(1);

        let reconstructor = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
            DataOutput::SequentialWriter(Box::new(writer)),
        )
        .with_config(url_refresh_test_config())
        .with_buffer_semaphore(tiny_semaphore);

        reconstructor.run().await.expect("Reconstruction should succeed");

        let reconstructed = writer_buffer.lock().unwrap().clone();
        assert_eq!(reconstructed, file_contents.data);
    }

    /// Test URL refresh with a larger file that requires multiple prefetch blocks.
    #[tokio::test(start_paused = true)]
    async fn test_url_refresh_large_file_multiple_blocks() {
        // Create a larger file with many terms
        let term_spec: Vec<(u64, (u64, u64))> = (1..=5).map(|i| (i, (0, 3))).collect();
        let (client, file_contents) = setup_test_file(&term_spec).await;

        // Set a short URL expiration
        client.set_fetch_term_url_expiration(Duration::from_secs(1));

        let writer = TimeAdvancingWriter::new(Duration::from_secs(2));
        let writer_buffer = writer.buffer.clone();

        let tiny_semaphore = ResourceSemaphore::new(1);

        let reconstructor = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
            DataOutput::SequentialWriter(Box::new(writer)),
        )
        .with_config(url_refresh_test_config())
        .with_buffer_semaphore(tiny_semaphore);

        reconstructor.run().await.expect("Reconstruction should succeed");

        let reconstructed = writer_buffer.lock().unwrap().clone();
        assert_eq!(reconstructed, file_contents.data);
    }

    /// Test that reconstruction works when URLs don't expire (control test).
    #[tokio::test(start_paused = true)]
    async fn test_no_url_expiration_control() {
        let term_spec = &[(1, (0, 2)), (2, (0, 2)), (3, (0, 2))];
        let (client, file_contents) = setup_test_file(term_spec).await;

        // Set a long URL expiration that won't trigger
        client.set_fetch_term_url_expiration(Duration::from_secs(3600));

        // Advance time only slightly (less than expiration)
        let writer = TimeAdvancingWriter::new(Duration::from_millis(100));
        let writer_buffer = writer.buffer.clone();

        let tiny_semaphore = ResourceSemaphore::new(1);

        let reconstructor = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
            DataOutput::SequentialWriter(Box::new(writer)),
        )
        .with_config(url_refresh_test_config())
        .with_buffer_semaphore(tiny_semaphore);

        reconstructor.run().await.expect("Reconstruction should succeed");

        let reconstructed = writer_buffer.lock().unwrap().clone();
        assert_eq!(reconstructed, file_contents.data);
    }

    /// Test partial range reconstruction with URL refresh.
    #[tokio::test(start_paused = true)]
    async fn test_url_refresh_partial_range() {
        let term_spec = &[(1, (0, 5)), (2, (0, 5))];
        let (client, file_contents) = setup_test_file(term_spec).await;
        let file_len = file_contents.data.len() as u64;

        client.set_fetch_term_url_expiration(Duration::from_secs(1));

        let writer = TimeAdvancingWriter::new(Duration::from_secs(2));
        let writer_buffer = writer.buffer.clone();

        let tiny_semaphore = ResourceSemaphore::new(1);

        let range = FileRange::new(file_len / 4, file_len * 3 / 4);

        let reconstructor = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
            DataOutput::SequentialWriter(Box::new(writer)),
        )
        .with_byte_range(range)
        .with_config(url_refresh_test_config())
        .with_buffer_semaphore(tiny_semaphore);

        reconstructor.run().await.expect("Reconstruction should succeed");

        let reconstructed = writer_buffer.lock().unwrap().clone();
        let expected = &file_contents.data[range.start as usize..range.end as usize];
        assert_eq!(reconstructed, expected);
    }

    // ==================== File Output Specific Tests ====================
    // Note: Basic file output is tested via reconstruct_and_verify_full/range.
    // These tests cover file-specific scenarios like multiple writes to the same file.

    /// Helper to reconstruct to a specific file path (for multi-write tests).
    async fn reconstruct_range_to_file_path(
        client: &Arc<LocalClient>,
        file_hash: MerkleHash,
        file_path: &std::path::Path,
        range: FileRange,
        config: ReconstructionConfig,
    ) -> Result<u64> {
        FileReconstructor::new(&(client.clone() as Arc<dyn Client>), file_hash, DataOutput::write_in_file(file_path))
            .with_byte_range(range)
            .with_config(config)
            .run()
            .await
    }

    #[tokio::test]
    async fn test_file_concurrent_non_overlapping_range_writes() {
        // Test 16 concurrent writers writing non-overlapping ranges to a ~1MB file.
        const NUM_WRITERS: usize = 16;
        const LARGE_CHUNK_SIZE: usize = 4096;

        // Create a large file (~1MB) with many xorbs.
        // Each xorb has ~64KB of data (16 chunks * 4KB), giving us ~1MB total with 16 xorbs.
        let term_spec: Vec<(u64, (u64, u64))> = (1..=16).map(|i| (i, (0, 16))).collect();

        let client = LocalClient::temporary().await.unwrap();
        let file_contents = client.upload_random_file(&term_spec, LARGE_CHUNK_SIZE).await.unwrap();
        let file_len = file_contents.data.len() as u64;

        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("output.bin");

        // Pre-create the file with zeros.
        std::fs::write(&file_path, vec![0u8; file_len as usize]).unwrap();

        // Use a config with larger fetch sizes for the concurrent test.
        let mut config = ReconstructionConfig::default();
        config.min_reconstruction_fetch_size = utils::ByteSize::from("32kb");
        config.max_reconstruction_fetch_size = utils::ByteSize::from("128kb");

        // Create 16 non-overlapping ranges.
        let chunk_size = file_len / NUM_WRITERS as u64;
        let ranges: Vec<FileRange> = (0..NUM_WRITERS)
            .map(|i| {
                let start = i as u64 * chunk_size;
                let end = if i == NUM_WRITERS - 1 {
                    file_len
                } else {
                    (i as u64 + 1) * chunk_size
                };
                FileRange::new(start, end)
            })
            .collect();

        // Spawn all writers concurrently using a JoinSet.
        let mut join_set = tokio::task::JoinSet::new();

        for range in ranges {
            let client = client.clone();
            let file_hash = file_contents.file_hash;
            let file_path = file_path.clone();
            let config = config.clone();

            join_set.spawn(async move {
                FileReconstructor::new(&(client as Arc<dyn Client>), file_hash, DataOutput::write_in_file(&file_path))
                    .with_byte_range(range)
                    .with_config(config)
                    .run()
                    .await
            });
        }

        // Wait for all writers to complete.
        while let Some(result) = join_set.join_next().await {
            result.unwrap().unwrap();
        }

        // Verify the complete file.
        let reconstructed = std::fs::read(&file_path).unwrap();
        assert_eq!(reconstructed.len(), file_contents.data.len());
        assert_eq!(reconstructed, file_contents.data);
    }

    #[tokio::test]
    async fn test_file_writes_preserve_existing_content() {
        // Test that writing a range doesn't affect content outside that range.
        let (client, file_contents) = setup_test_file(&[(1, (0, 10))]).await;
        let file_len = file_contents.data.len() as u64;

        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("output.bin");

        // Pre-create the file with a specific pattern.
        let pattern: Vec<u8> = (0..file_len).map(|i| (i % 251) as u8).collect();
        std::fs::write(&file_path, &pattern).unwrap();

        // Write only the middle third.
        let start = file_len / 3;
        let end = 2 * file_len / 3;
        let range = FileRange::new(start, end);

        reconstruct_range_to_file_path(&client, file_contents.file_hash, &file_path, range, test_config())
            .await
            .unwrap();

        let result = std::fs::read(&file_path).unwrap();

        // First and last thirds should still have the pattern.
        assert_eq!(&result[..start as usize], &pattern[..start as usize]);
        assert_eq!(&result[end as usize..], &pattern[end as usize..]);

        // Middle third should have reconstructed data.
        assert_eq!(&result[start as usize..end as usize], &file_contents.data[start as usize..end as usize]);
    }
}
