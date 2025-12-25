use std::sync::Arc;

use cas_client::{
    CacheConfig, Client, FileReconstructorV1, SeekingOutputProvider, sequential_output_from_filepath,
    sequential_output_from_writer,
};
use cas_types::FileRange;
use merklehash::MerkleHash;
use progress_tracking::item_tracking::SingleItemProgressUpdater;
use tokio::sync::Semaphore;
use tracing::{debug, info};
use xet_config::ReconstructionConfig;
use xet_runtime::{GlobalSemaphoreHandle, XetRuntime, global_semaphore_handle, xet_config};

use crate::data_writer::{DataOutput, new_data_writer};
use crate::error::{FileReconstructionError, Result};
use crate::reconstruction_terms::ReconstructionTermManager;

lazy_static::lazy_static! {

    /// A global limit on how much memory can be used for buffering data during reconstruction.  This could be overridden by
    /// a custom semaphore, but by default all reconstructions share a common memory limit.  By default, set to 8gb.
    static ref RECONSTRUCTION_DOWNLOAD_BUFFER_SIZE: usize = xet_config().reconstruction.download_buffer_size.as_u64().min(usize::MAX as u64) as usize;
    static ref RECONSTRUCTION_DOWNLOAD_BUFFER_LIMITER: GlobalSemaphoreHandle =
        global_semaphore_handle!(*RECONSTRUCTION_DOWNLOAD_BUFFER_SIZE);
}

/// Reconstructs a file from its content-addressed chunks by downloading xorb blocks
/// and writing the reassembled data to an output. Supports byte range requests and
/// uses memory-limited buffering with adaptive prefetching.
pub struct FileReconstructor {
    client: Arc<dyn Client>,
    file_hash: MerkleHash,
    byte_range: Option<FileRange>,
    output: DataOutput,
    #[allow(dead_code)]
    progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    config: Arc<ReconstructionConfig>,
    /// Custom buffer semaphore (semaphore, total_permits) for testing or specialized use cases.
    custom_buffer_semaphore: Option<(Arc<Semaphore>, usize)>,
    /// Cache configuration for V1 reconstruction.
    cache_config: Option<CacheConfig>,
    /// Override for whether to use V1 reconstruction algorithm.
    use_v1_reconstructor: Option<bool>,
}

impl FileReconstructor {
    pub fn new(client: &Arc<dyn Client>, file_hash: MerkleHash, output: DataOutput) -> Self {
        Self {
            client: client.clone(),
            file_hash,
            byte_range: None,
            output,
            progress_updater: None,
            config: Arc::new(xet_config().reconstruction.clone()),
            custom_buffer_semaphore: None,
            cache_config: None,
            use_v1_reconstructor: None,
        }
    }

    pub fn with_byte_range(self, byte_range: FileRange) -> Self {
        Self {
            byte_range: Some(byte_range),
            ..self
        }
    }

    pub fn with_progress_updater(self, progress_updater: Arc<SingleItemProgressUpdater>) -> Self {
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
    ///
    /// # Arguments
    /// * `semaphore` - The semaphore to use for buffer limiting
    /// * `total_permits` - The total number of permits in the semaphore (used to cap requests)
    pub fn with_buffer_semaphore(self, semaphore: Arc<Semaphore>, total_permits: usize) -> Self {
        Self {
            custom_buffer_semaphore: Some((semaphore, total_permits)),
            ..self
        }
    }

    /// Sets the cache configuration for V1 reconstruction.
    /// This is used when running with the V1 reconstruction algorithm.
    pub fn with_cache(self, cache_config: &CacheConfig) -> Self {
        Self {
            cache_config: Some(cache_config.clone()),
            ..self
        }
    }

    /// Overrides whether to use the V1 reconstruction algorithm.
    /// When true, uses the old FileReconstructorV1 algorithm.
    /// When false, uses the new V2 algorithm.
    /// If not set, defaults to the config value `reconstruction.use_v1_reconstruction`.
    pub fn use_v1_reconstructor(self, use_v1: bool) -> Self {
        Self {
            use_v1_reconstructor: Some(use_v1),
            ..self
        }
    }

    /// Runs the file reconstruction using the appropriate algorithm.
    /// Uses V1 or V2 based on the `use_v1_reconstruction` override or config setting.
    /// Returns the number of bytes written.
    pub async fn run(self) -> Result<u64> {
        let use_v1 = self.use_v1_reconstructor.unwrap_or(self.config.use_v1);

        info!(
            file_hash = %self.file_hash,
            byte_range = ?self.byte_range,
            algorithm = if use_v1 { "v1" } else { "v2" },
            "Starting file reconstruction"
        );

        let result = if use_v1 {
            self.run_v1().await
        } else {
            self.run_v2().await
        };

        match &result {
            Ok(bytes_written) => info!(bytes_written, "File reconstruction completed successfully"),
            Err(e) => info!(error = %e, "File reconstruction failed"),
        }

        result
    }

    /// Runs file reconstruction using the V1 algorithm (FileReconstructorV1).
    /// Returns the number of bytes written.
    pub async fn run_v1(self) -> Result<u64> {
        let Self {
            client,
            file_hash,
            byte_range,
            output,
            progress_updater,
            config: _,
            custom_buffer_semaphore: _,
            cache_config,
            use_v1_reconstructor: _,
        } = self;

        let v1_reconstructor = FileReconstructorV1::new(client, &cache_config);

        let bytes_written = match output {
            DataOutput::File { path, offset: _ } => {
                if xet_config().client.reconstruct_write_sequentially {
                    let output = sequential_output_from_filepath(&path).map_err(FileReconstructionError::from)?;
                    v1_reconstructor
                        .get_file_with_sequential_writer(&file_hash, byte_range, output, progress_updater)
                        .await?
                } else {
                    let writer = SeekingOutputProvider::new_file_provider(path);
                    v1_reconstructor
                        .get_file_with_parallel_writer(&file_hash, byte_range, &writer, progress_updater)
                        .await?
                }
            },
            DataOutput::SequentialWriter(writer) => {
                let output = sequential_output_from_writer(writer);
                v1_reconstructor
                    .get_file_with_sequential_writer(&file_hash, byte_range, output, progress_updater)
                    .await?
            },
        };

        Ok(bytes_written)
    }

    /// Runs through the file reconstruction process using the new controlled method.
    /// Returns the number of bytes written.
    pub async fn run_v2(self) -> Result<u64> {
        let Self {
            client,
            file_hash,
            byte_range,
            output,
            progress_updater: _,
            config,
            custom_buffer_semaphore,
            cache_config: _,
            use_v1_reconstructor: _,
        } = self;

        let requested_range = byte_range.unwrap_or_else(FileRange::full);

        let mut term_manager =
            ReconstructionTermManager::new(config.clone(), client.clone(), file_hash, requested_range).await?;

        let data_writer = new_data_writer(output, requested_range.start, &config)?;

        // Determine buffer size and semaphore based on whether a custom one is provided.
        // Tuple is (semaphore, total_permits).
        let (download_buffer_semaphore, buffer_size) = custom_buffer_semaphore.unwrap_or_else(|| {
            let global_semaphore = XetRuntime::current().global_semaphore(*RECONSTRUCTION_DOWNLOAD_BUFFER_LIMITER);
            let default_buffer_size = *RECONSTRUCTION_DOWNLOAD_BUFFER_SIZE;
            (global_semaphore, default_buffer_size)
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
                let term_size = (file_term.byte_range.end - file_term.byte_range.start) as u32;

                debug!(
                    xorb_hash = %file_term.xorb_block.xorb_hash,
                    term_byte_range = ?(file_term.byte_range.start, file_term.byte_range.end),
                    term_size,
                    "Processing file term"
                );

                // Don't request more permits than the buffer size. Term size is limited to
                // u32::MAX, but if the buffer is tiny, then we could deadlock if we request a
                // larger number of permits.
                let max_request_size = buffer_size.min(u32::MAX as usize) as u32;
                let requested_permit_size = term_size.min(max_request_size);

                // Acquire a permit from the memory limiter for using the memory needed for
                // the next term.
                let buffer_permit = download_buffer_semaphore
                    .clone()
                    .acquire_many_owned(requested_permit_size)
                    .await
                    .map_err(|e| {
                        FileReconstructionError::InternalError(format!("Error acquiring download buffer permit: {e}"))
                    })?;

                // Get the data future that will retrieve the term's data.
                let data_future = file_term.get_data_task(client.clone()).await?;

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
                total_bytes_scheduled += term_size as u64;
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

        Ok(bytes_written)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use cas_client::LocalClient;
    use cas_client::client_testing_utils::{ClientTestingUtils, RandomFileContents};
    use cas_types::FileRange;

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

            // Advance tokio time to cause URL expiration for next fetch
            // Note: We use tokio::time::advance which only works with start_paused = true
            let advance_duration = self.advance_duration;
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    tokio::time::advance(advance_duration).await;
                });
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
        let tiny_semaphore = Arc::new(Semaphore::new(1));

        let reconstructor = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
            DataOutput::SequentialWriter(Box::new(writer)),
        )
        .with_config(url_refresh_test_config())
        .with_buffer_semaphore(tiny_semaphore, 1);

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

        let tiny_semaphore = Arc::new(Semaphore::new(1));

        let reconstructor = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
            DataOutput::SequentialWriter(Box::new(writer)),
        )
        .with_config(url_refresh_test_config())
        .with_buffer_semaphore(tiny_semaphore, 1);

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

        let tiny_semaphore = Arc::new(Semaphore::new(1));

        let reconstructor = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
            DataOutput::SequentialWriter(Box::new(writer)),
        )
        .with_config(url_refresh_test_config())
        .with_buffer_semaphore(tiny_semaphore, 1);

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

        let tiny_semaphore = Arc::new(Semaphore::new(1));

        let reconstructor = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
            DataOutput::SequentialWriter(Box::new(writer)),
        )
        .with_config(url_refresh_test_config())
        .with_buffer_semaphore(tiny_semaphore, 1);

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

        let tiny_semaphore = Arc::new(Semaphore::new(1));

        let range = FileRange::new(file_len / 4, file_len * 3 / 4);

        let reconstructor = FileReconstructor::new(
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
            DataOutput::SequentialWriter(Box::new(writer)),
        )
        .with_byte_range(range)
        .with_config(url_refresh_test_config())
        .with_buffer_semaphore(tiny_semaphore, 1);

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
