#[cfg(not(target_family = "wasm"))]
use std::fs::OpenOptions;
use std::io::Write;
#[cfg(not(target_family = "wasm"))]
use std::io::{Seek, SeekFrom};
#[cfg(not(target_family = "wasm"))]
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use xet_client::cas_client::Client;
use xet_client::cas_types::FileRange;
use xet_client::chunk_cache::ChunkCache;
use xet_core_structures::merklehash::MerkleHash;
use xet_runtime::config::ReconstructionConfig;
use xet_runtime::core::XetContext;
use xet_runtime::utils::ClosureGuard;
use xet_runtime::utils::adjustable_semaphore::AdjustableSemaphore;

use super::data_writer::{DataWriter, DownloadStream, SequentialWriter, UnorderedDownloadStream};
use super::error::{FileReconstructionError, Result};
use super::reconstruction_terms::ReconstructionTermManager;
use super::run_state::{RunError, RunState};
use crate::progress_tracking::ItemProgressUpdater;

/// Reconstructs a file from its content-addressed chunks by downloading xorb blocks
/// and writing the reassembled data to an output. Supports byte range requests and
/// uses memory-limited buffering with adaptive prefetching.
pub struct FileReconstructor {
    ctx: XetContext,
    client: Arc<dyn Client>,
    file_hash: MerkleHash,
    file_size: Option<u64>,
    byte_range: Option<FileRange>,
    progress_updater: Option<Arc<ItemProgressUpdater>>,
    config: Arc<ReconstructionConfig>,

    /// Optional on-disk chunk cache for cross-file deduplication.
    chunk_cache: Option<Arc<dyn ChunkCache>>,

    /// Custom buffer semaphore for testing or specialized use cases.
    custom_buffer_semaphore: Option<Arc<AdjustableSemaphore>>,

    /// Cancellation token checked at each major step of the reconstruction loop.
    /// When cancelled, reconstruction stops at its next check point. Long waits
    /// (such as semaphore acquisition) use `tokio::select!` so they abort promptly.
    cancellation_token: CancellationToken,
}

impl FileReconstructor {
    pub fn new(ctx: &XetContext, client: &Arc<dyn Client>, file_hash: MerkleHash) -> Self {
        Self {
            ctx: ctx.clone(),
            client: client.clone(),
            file_hash,
            file_size: None,
            byte_range: None,
            progress_updater: default_progress_updater(),
            config: Arc::new(ctx.config.reconstruction.clone()),
            chunk_cache: None,
            custom_buffer_semaphore: None,
            cancellation_token: CancellationToken::new(),
        }
    }

    /// Sets the known file size, capping reconstruction requests even when a byte range is supplied.
    /// Ranges starting at or beyond this size produce no data or reconstruction requests.
    pub fn with_file_size(self, file_size: u64) -> Self {
        Self {
            file_size: Some(file_size),
            ..self
        }
    }

    pub fn with_byte_range(self, byte_range: FileRange) -> Self {
        Self {
            byte_range: Some(byte_range),
            ..self
        }
    }

    pub fn with_progress_updater(self, progress_updater: Arc<ItemProgressUpdater>) -> Self {
        Self {
            progress_updater: Some(progress_updater),
            ..self
        }
    }

    pub fn with_chunk_cache(self, cache: Arc<dyn ChunkCache>) -> Self {
        Self {
            chunk_cache: Some(cache),
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
    pub fn with_buffer_semaphore(self, semaphore: Arc<AdjustableSemaphore>) -> Self {
        Self {
            custom_buffer_semaphore: Some(semaphore),
            ..self
        }
    }

    /// Replaces the default cancellation token with the given one. This is used
    /// when external code needs to share the same token for coordinated
    /// cancellation.
    pub fn with_cancellation_token(self, token: CancellationToken) -> Self {
        Self {
            cancellation_token: token,
            ..self
        }
    }

    /// Reconstructs the file and writes it to the given path.
    ///
    /// The file is opened with read/write access. When `truncate_file` is `true`
    /// the file is truncated to the reconstructed length; when `false` the file
    /// is left at its existing size, allowing multiple concurrent reconstructions
    /// to write to different regions of the same file.
    ///
    /// When `write_offset` is `Some(offset)`, writing begins at that byte
    /// position regardless of the byte range. When `None`, writing begins at
    /// the byte range start (or 0 for a full-file reconstruction).
    #[cfg(not(target_family = "wasm"))]
    pub async fn reconstruct_to_file(self, path: &Path, write_offset: Option<u64>, truncate_file: bool) -> Result<u64> {
        info!(
            file_hash = %self.file_hash,
            byte_range = ?self.byte_range,
            path = %path.display(),
            write_offset = ?write_offset,
            "Reconstructing file to disk"
        );

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new().write(true).create(true).truncate(truncate_file).open(path)?;

        let default_write_position = self.byte_range.map_or(0, |r| r.start);
        let seek_position = write_offset.unwrap_or(default_write_position);
        if seek_position > 0 {
            file.seek(SeekFrom::Start(seek_position))?;
        }

        let run_state = RunState::new(self.cancellation_token.clone(), self.file_hash, self.progress_updater.clone());

        let data_writer = SequentialWriter::new(&self.ctx, file, self.config.use_vectored_write, run_state.clone());

        self.run(data_writer, run_state, false).await
    }

    /// Reconstructs the file and writes it to the given writer.
    ///
    /// The writer receives data starting from its current position (position 0
    /// for a fresh writer), regardless of the byte range being reconstructed.
    #[cfg(not(target_family = "wasm"))]
    pub async fn reconstruct_to_writer<W: Write + Send + 'static>(self, writer: W) -> Result<u64> {
        info!(
            file_hash = %self.file_hash,
            byte_range = ?self.byte_range,
            "Reconstructing file to writer"
        );

        let run_state = RunState::new(self.cancellation_token.clone(), self.file_hash, self.progress_updater.clone());
        let data_writer = SequentialWriter::new(&self.ctx, writer, self.config.use_vectored_write, run_state.clone());
        self.run(data_writer, run_state, false).await
    }

    /// Reconstructs the file and writes it to the given writer.
    ///
    /// The writer receives data starting from its current position (position 0
    /// for a fresh writer), regardless of the byte range being reconstructed.
    ///
    /// On wasm there is no background writer thread (wasm cannot block the host
    /// thread), so this drives the streaming reconstruction path and copies each
    /// chunk into the writer inline as it arrives.
    #[cfg(target_family = "wasm")]
    pub async fn reconstruct_to_writer<W: Write + Send + 'static>(self, mut writer: W) -> Result<u64> {
        info!(
            file_hash = %self.file_hash,
            byte_range = ?self.byte_range,
            "Reconstructing file to writer"
        );

        let mut stream = self.reconstruct_to_stream();
        let mut total: u64 = 0;
        while let Some(chunk) = stream.next().await? {
            writer
                .write_all(&chunk)
                .map_err(|e| FileReconstructionError::IoError(Arc::new(e)))?;
            total += chunk.len() as u64;
        }
        writer.flush().map_err(|e| FileReconstructionError::IoError(Arc::new(e)))?;
        Ok(total)
    }

    /// Reconstructs the file as a stream, returning a [`DownloadStream`] that
    /// yields data chunks as they become available.
    ///
    /// The reconstruction task is spawned immediately but pauses on an
    /// internal [`tokio::sync::Notify`] until [`DownloadStream::start`] is
    /// called (or the first [`DownloadStream::next`] /
    /// [`DownloadStream::blocking_next`]).
    ///
    /// # Panics
    ///
    /// Panics if called outside a tokio runtime context (the constructor
    /// uses [`tokio::spawn`]).
    pub fn reconstruct_to_stream(self) -> DownloadStream {
        let run_state = RunState::new(self.cancellation_token.clone(), self.file_hash, self.progress_updater.clone());

        DownloadStream::new(self, run_state)
    }

    /// Reconstructs the file as an unordered stream, returning an
    /// [`UnorderedDownloadStream`] that yields `(offset, Bytes)` chunks
    /// in whatever order they complete.
    ///
    /// The reconstruction task is spawned immediately but pauses on an
    /// internal [`tokio::sync::Notify`] until
    /// [`UnorderedDownloadStream::start`] is called (or the first
    /// [`UnorderedDownloadStream::next`] /
    /// [`UnorderedDownloadStream::blocking_next`]).
    ///
    /// # Panics
    ///
    /// Panics if called outside a tokio runtime context (the constructor
    /// uses [`tokio::spawn`]).
    pub fn reconstruct_to_unordered_stream(self) -> UnorderedDownloadStream {
        let run_state = RunState::new(self.cancellation_token.clone(), self.file_hash, self.progress_updater.clone());

        UnorderedDownloadStream::new(self, run_state)
    }

    /// Runs the file reconstruction with error handling and cancellation support.
    /// Returns the number of bytes written.
    ///
    /// When `is_streaming` is true, the progress completion assertions at the end
    /// of reconstruction are skipped because the stream consumer reports bytes
    /// asynchronously after this method returns.
    pub(crate) async fn run(
        self,
        data_writer: Box<dyn DataWriter>,
        run_state: Arc<RunState>,
        is_streaming: bool,
    ) -> Result<u64> {
        match self.run_impl(data_writer, &run_state, is_streaming).await {
            Ok(v) => Ok(v),
            Err(RunError::Cancelled) => {
                run_state.check_error()?;
                Ok(0)
            },
            Err(RunError::Error(e)) => {
                run_state.set_error(e.clone());
                Err(e)
            },
        }
    }

    async fn run_impl(
        self,
        mut data_writer: Box<dyn DataWriter>,
        run_state: &Arc<RunState>,
        _is_streaming: bool,
    ) -> std::result::Result<u64, RunError> {
        let Self {
            ctx,
            client,
            file_size,
            byte_range,
            config,
            chunk_cache,
            custom_buffer_semaphore,
            ..
        } = self;

        run_state.check_run_state()?;

        let file_hash = *run_state.file_hash();
        let mut requested_range = byte_range.unwrap_or_else(FileRange::full);
        if let Some(file_size) = file_size {
            requested_range.end = requested_range.end.min(file_size);
            requested_range.start = requested_range.start.min(requested_range.end);
        }

        let mut term_manager = ReconstructionTermManager::new(
            ctx.clone(),
            config.clone(),
            client.clone(),
            file_hash,
            requested_range,
            run_state.progress_updater().cloned(),
        )
        .await?;

        let using_global_memory_limit = custom_buffer_semaphore.is_none();
        let download_buffer_semaphore =
            custom_buffer_semaphore.unwrap_or_else(|| ctx.common.reconstruction_download_buffer.clone());

        // Dynamic buffer scaling: the target buffer size grows with the number of active
        // downloads: target = (base + n * perfile).min(limit). On start we increment to
        // the new target, possibly getting back a virtual permit that lets this download begin
        // immediately without queuing behind existing acquires. On exit, the ClosureGuard
        // recomputes the target for the reduced download count and shrinks back if needed.
        let mut seed_buffer_permit;
        let _download_count_decrement_guard;

        if using_global_memory_limit {
            let active_downloads = ctx.common.active_downloads.clone();
            let n = active_downloads.fetch_add(1, Ordering::Relaxed) + 1;

            let base = config.download_buffer_size.as_u64();
            let perfile = config.download_buffer_perfile_size.as_u64();
            let limit = config.download_buffer_limit.as_u64();

            let target = base.saturating_add(n.saturating_mul(perfile)).min(limit);
            seed_buffer_permit = download_buffer_semaphore.increment_permits_to_target(target);

            let buffer_sem = download_buffer_semaphore.clone();
            _download_count_decrement_guard = Some(ClosureGuard::new(move || {
                let n = active_downloads.fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
                let target = base.saturating_add(n.saturating_mul(perfile)).min(limit);
                buffer_sem.decrement_permits_to_target(target);
            }));
        } else {
            seed_buffer_permit = None;
            _download_count_decrement_guard = None;
        }

        // The range start offset - we need to adjust byte ranges to be relative to this.
        let range_start_offset = requested_range.start;

        // Outer loop: retrieve blocks of file terms.
        // Use select! so a background error (which cancels the token) wakes this
        // up immediately rather than waiting for the network round-trip to finish.
        loop {
            let maybe_file_terms = tokio::select! {
                biased;
                _ = run_state.cancelled() => {
                    return run_state.check_run_state().map(|_| 0);
                }
                result = term_manager.next_file_terms() => result?
            };

            let Some(file_terms) = maybe_file_terms else {
                break;
            };

            run_state.check_run_state()?;

            run_state.record_new_block();

            // Inner loop: process each file term in the block.
            for file_term in file_terms {
                run_state.check_run_state()?;

                let term_size = file_term.byte_range.end - file_term.byte_range.start;

                debug!(
                    file_hash = %file_hash,
                    xorb_hash = %file_term.xorb_block.xorb_hash,
                    term_byte_range = ?(file_term.byte_range.start, file_term.byte_range.end),
                    term_size,
                    "Processing file term"
                );

                // Try to split from the reserved (virtual) permit first, giving this
                // download immediate access without waiting in the FIFO queue.
                // Fall back to the shared semaphore if the seed permit has been exhausted.
                let buffer_permit = match seed_buffer_permit.as_mut().and_then(|rp| rp.split(term_size)) {
                    Some(split) => split,
                    None => {
                        seed_buffer_permit = None;

                        // Use tokio::select! to abort promptly if the run state fires.
                        tokio::select! {
                            biased;
                            _ = run_state.cancelled() => {
                                return run_state.check_run_state().map(|_| 0);
                            }
                            result = download_buffer_semaphore.acquire_many(term_size) => {
                                result.map_err(|e| {
                                    FileReconstructionError::InternalError(format!(
                                        "Error acquiring download buffer permit: {e}"
                                    ))
                                })?
                            }
                        }
                    },
                };

                // Starting a block's download waits for a download permit; doing it here, in term order, makes
                // blocks download in file order.
                let reader = tokio::select! {
                    biased;
                    _ = run_state.cancelled() => {
                        return run_state.check_run_state().map(|_| 0);
                    }
                    result = file_term.start_read(
                        ctx.clone(),
                        client.clone(),
                        run_state.progress_updater().cloned(),
                        chunk_cache.clone(),
                        run_state.clone(),
                    ) => result?,
                };

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

                data_writer
                    .set_next_term_data_source(relative_byte_range, Some(buffer_permit), reader)
                    .await?;

                run_state.record_new_term(term_size);
            }
        }

        run_state.log_progress("All term blocks received and scheduled for writing");

        // Finish the data writer and wait for all data to be written.
        let bytes_written = data_writer.finish().await?;
        let total_bytes_scheduled = run_state.total_bytes_scheduled();

        debug_assert_eq!(
            bytes_written, total_bytes_scheduled,
            "Bytes written ({bytes_written}) should match total bytes scheduled ({total_bytes_scheduled})"
        );

        run_state.log_progress("File reconstruction completed successfully");

        #[cfg(debug_assertions)]
        if !_is_streaming && let Some(updater) = run_state.progress_updater() {
            updater.assert_complete();
            if requested_range.end < u64::MAX {
                assert_eq!(updater.total_bytes_completed(), requested_range.end - requested_range.start);
            }
        }

        Ok(total_bytes_scheduled)
    }
}

#[cfg(test)]
fn default_progress_updater() -> Option<Arc<ItemProgressUpdater>> {
    Some(ItemProgressUpdater::new_standalone("test"))
}

#[cfg(not(test))]
fn default_progress_updater() -> Option<Arc<ItemProgressUpdater>> {
    None
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::runtime::Handle;
    use xet_client::cas_client::{ClientTestingUtils, DirectAccessClient, LocalClient, RandomFileContents};
    use xet_client::cas_types::FileRange;
    use xet_runtime::config::XetConfig;
    use xet_runtime::core::XetContext;

    use super::*;
    use crate::progress_tracking::ItemProgressUpdater;

    const TEST_CHUNK_SIZE: usize = 101;

    /// Creates a test config with small fetch sizes to force multiple iterations.
    fn test_config() -> ReconstructionConfig {
        let mut config = ReconstructionConfig::default();
        // Use small fetch sizes to force multiple prefetch iterations
        config.min_reconstruction_fetch_size = xet_runtime::utils::ByteSize::from("100");
        config.max_reconstruction_fetch_size = xet_runtime::utils::ByteSize::from("400");
        config.min_prefetch_buffer = xet_runtime::utils::ByteSize::from("800");
        config
    }

    /// Creates a test client and uploads a random file with the given term specification.
    async fn setup_test_file(term_spec: &[(u64, (u64, u64))]) -> (Arc<LocalClient>, RandomFileContents) {
        let client = LocalClient::temporary(XetContext::default().unwrap()).await.unwrap();
        let file_contents = client.upload_random_file(term_spec, TEST_CHUNK_SIZE).await.unwrap();
        (client, file_contents)
    }

    /// Reconstructs a file (or byte range) using a writer and returns the reconstructed data.
    async fn reconstruct_to_vec(
        client: &Arc<LocalClient>,
        file_hash: MerkleHash,
        byte_range: Option<FileRange>,
        config: &ReconstructionConfig,
        semaphore: Option<Arc<AdjustableSemaphore>>,
    ) -> Result<Vec<u8>> {
        let buffer = Arc::new(std::sync::Mutex::new(Cursor::new(Vec::new())));
        let writer = StaticCursorWriter(buffer.clone());

        let mut reconstructor =
            FileReconstructor::new(&XetContext::default().unwrap(), &(client.clone() as Arc<dyn Client>), file_hash)
                .with_config(config);

        if let Some(range) = byte_range {
            reconstructor = reconstructor.with_byte_range(range);
        }
        if let Some(sem) = semaphore {
            reconstructor = reconstructor.with_buffer_semaphore(sem);
        }

        reconstructor.reconstruct_to_writer(writer).await?;

        let data = buffer.lock().unwrap().get_ref().clone();
        Ok(data)
    }

    fn new_reconstructor(
        client: &Arc<LocalClient>,
        file_hash: MerkleHash,
        byte_range: Option<FileRange>,
        config: &ReconstructionConfig,
    ) -> FileReconstructor {
        let reconstructor =
            FileReconstructor::new(&XetContext::default().unwrap(), &(client.clone() as Arc<dyn Client>), file_hash)
                .with_config(config);
        match byte_range {
            Some(range) => reconstructor.with_byte_range(range),
            None => reconstructor,
        }
    }

    /// Reconstructs through a `DownloadStream` and returns the concatenated pieces.
    async fn reconstruct_to_stream_vec(
        client: &Arc<LocalClient>,
        file_hash: MerkleHash,
        byte_range: Option<FileRange>,
        config: &ReconstructionConfig,
    ) -> Result<Vec<u8>> {
        let mut stream = new_reconstructor(client, file_hash, byte_range, config).reconstruct_to_stream();
        let mut data = Vec::new();
        while let Some(piece) = stream.next().await? {
            data.extend_from_slice(&piece);
        }
        Ok(data)
    }

    /// Reconstructs through an `UnorderedDownloadStream` and assembles the pieces by offset, checking that they
    /// cover the output exactly once.
    async fn reconstruct_to_unordered_stream_vec(
        client: &Arc<LocalClient>,
        file_hash: MerkleHash,
        byte_range: Option<FileRange>,
        config: &ReconstructionConfig,
    ) -> Result<Vec<u8>> {
        let mut stream = new_reconstructor(client, file_hash, byte_range, config).reconstruct_to_unordered_stream();
        let mut pieces = Vec::new();
        while let Some(piece) = stream.next().await? {
            pieces.push(piece);
        }
        pieces.sort_by_key(|(offset, _)| *offset);
        let mut data = Vec::new();
        for (offset, piece) in pieces {
            assert_eq!(offset, data.len() as u64, "unordered pieces must tile the output");
            data.extend_from_slice(&piece);
        }
        Ok(data)
    }

    /// Reconstructs to a file and returns the reconstructed data.
    /// Creates a temp file, reconstructs to it, then reads the relevant portion back.
    async fn reconstruct_to_file(
        client: &Arc<LocalClient>,
        file_hash: MerkleHash,
        byte_range: Option<FileRange>,
        config: &ReconstructionConfig,
    ) -> Result<Vec<u8>> {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("output.bin");

        let mut reconstructor =
            FileReconstructor::new(&XetContext::default().unwrap(), &(client.clone() as Arc<dyn Client>), file_hash)
                .with_config(config);

        if let Some(range) = byte_range {
            reconstructor = reconstructor.with_byte_range(range);
        }

        reconstructor.reconstruct_to_file(&file_path, None, false).await?;

        // Read back the data from the file at the expected location.
        let file_data = std::fs::read(&file_path)?;
        let start = byte_range.map(|r| r.start as usize).unwrap_or(0);
        Ok(file_data[start..].to_vec())
    }

    /// Reconstructs to a file at a specific offset and returns the data.
    async fn reconstruct_to_file_at_specific_offset(
        client: &Arc<LocalClient>,
        file_hash: MerkleHash,
        byte_range: Option<FileRange>,
        config: &ReconstructionConfig,
    ) -> Result<Vec<u8>> {
        let offset = 9u64;

        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("output.bin");

        let mut reconstructor =
            FileReconstructor::new(&XetContext::default().unwrap(), &(client.clone() as Arc<dyn Client>), file_hash)
                .with_config(config);

        if let Some(range) = byte_range {
            reconstructor = reconstructor.with_byte_range(range);
        }

        reconstructor.reconstruct_to_file(&file_path, Some(offset), false).await?;

        // Read back all file data.
        let file_data = std::fs::read(&file_path)?;
        Ok(file_data[offset as usize..].to_vec())
    }

    /// Reconstructs to a file at offset 0 and returns the data.
    /// This tests writing to the beginning of a file regardless of the byte range.
    async fn reconstruct_to_file_at_offset_zero(
        client: &Arc<LocalClient>,
        file_hash: MerkleHash,
        byte_range: Option<FileRange>,
        config: &ReconstructionConfig,
    ) -> Result<Vec<u8>> {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("output.bin");

        let mut reconstructor =
            FileReconstructor::new(&XetContext::default().unwrap(), &(client.clone() as Arc<dyn Client>), file_hash)
                .with_config(config);

        if let Some(range) = byte_range {
            reconstructor = reconstructor.with_byte_range(range);
        }

        reconstructor.reconstruct_to_file(&file_path, Some(0), false).await?;

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

            // Test 1: reconstruct_to_writer
            let vec_result = reconstruct_to_vec(&client, h, None, &config, None).await.unwrap();
            assert_eq!(vec_result, *expected, "vec failed (vectored={use_vectored})");

            // Test 2: reconstruct_to_file
            let file_result = reconstruct_to_file(client, h, None, &config).await.unwrap();
            assert_eq!(file_result, *expected, "file failed (vectored={use_vectored})");

            // Test 3: reconstruct_to_file with offset 0
            let file_offset_result = reconstruct_to_file_at_offset_zero(client, h, None, &config).await.unwrap();
            assert_eq!(file_offset_result, *expected, "file_at_offset_zero failed (vectored={use_vectored})");

            // Test 4: reconstruct_to_file with specific offset
            let file_specific_result = reconstruct_to_file_at_specific_offset(client, h, None, &config).await.unwrap();
            assert_eq!(file_specific_result, *expected, "file_at_specific_offset failed (vectored={use_vectored})");
        }

        // Test 5: ordered and unordered streams
        let stream_result = reconstruct_to_stream_vec(client, h, None, &base_config).await.unwrap();
        assert_eq!(stream_result, *expected, "stream failed");
        let unordered_result = reconstruct_to_unordered_stream_vec(client, h, None, &base_config)
            .await
            .unwrap();
        assert_eq!(unordered_result, *expected, "unordered stream failed");
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

            // Test 1: reconstruct_to_writer
            let vec_result = reconstruct_to_vec(&client, file_contents.file_hash, Some(range), &config, None)
                .await
                .expect("reconstruct_to_vec should succeed");
            assert_eq!(vec_result, expected, "vec failed (vectored={use_vectored})");

            // Test 2: reconstruct_to_file
            let file_result = reconstruct_to_file(client, file_contents.file_hash, Some(range), &config)
                .await
                .expect("reconstruct_to_file should succeed");
            assert_eq!(file_result, expected, "file failed (vectored={use_vectored})");

            // Test 3: reconstruct_to_file with offset 0
            let file_offset_result =
                reconstruct_to_file_at_offset_zero(client, file_contents.file_hash, Some(range), &config)
                    .await
                    .expect("reconstruct_to_file_at_offset_zero should succeed");
            assert_eq!(file_offset_result, expected, "file_at_offset failed (vectored={use_vectored})");
        }

        // Test 4: ordered and unordered streams
        let h = file_contents.file_hash;
        let stream_result = reconstruct_to_stream_vec(client, h, Some(range), &base_config).await.unwrap();
        assert_eq!(stream_result, expected, "stream failed");
        let unordered_result = reconstruct_to_unordered_stream_vec(client, h, Some(range), &base_config)
            .await
            .unwrap();
        assert_eq!(unordered_result, expected, "unordered stream failed");
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

        let progress_updater = ItemProgressUpdater::new_standalone("file");
        let bytes_written = FileReconstructor::new(
            &XetContext::default().unwrap(),
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
        )
        .with_config(&config)
        .with_progress_updater(progress_updater.clone())
        .reconstruct_to_writer(writer)
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

        let progress_updater = ItemProgressUpdater::new_standalone("file");
        let bytes_written = FileReconstructor::new(
            &XetContext::default().unwrap(),
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
        )
        .with_config(&config)
        .with_byte_range(range)
        .with_progress_updater(progress_updater.clone())
        .reconstruct_to_writer(writer)
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

        let task = ItemProgressUpdater::new_standalone("test_file.bin");

        let buffer = Arc::new(std::sync::Mutex::new(Cursor::new(Vec::new())));
        let writer = StaticCursorWriter(buffer.clone());

        let bytes_written = FileReconstructor::new(
            &XetContext::default().unwrap(),
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
        )
        .with_config(&config)
        .with_progress_updater(task.clone())
        .reconstruct_to_writer(writer)
        .await
        .unwrap();

        assert_eq!(bytes_written, file_contents.data.len() as u64);

        task.assert_complete();
        assert_eq!(task.total_bytes_completed(), file_contents.data.len() as u64);
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

        let task = ItemProgressUpdater::new_standalone("test_file.bin");

        task.update_item_size(file_size, true);

        let buffer = Arc::new(std::sync::Mutex::new(Cursor::new(Vec::new())));
        let writer = StaticCursorWriter(buffer.clone());

        let bytes_written = FileReconstructor::new(
            &XetContext::default().unwrap(),
            &(client.clone() as Arc<dyn Client>),
            file_contents.file_hash,
        )
        .with_config(&config)
        .with_progress_updater(task.clone())
        .reconstruct_to_writer(writer)
        .await
        .unwrap();

        assert_eq!(bytes_written, file_size);

        assert_eq!(task.total_bytes_completed(), file_size);

        task.assert_complete();
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
    async fn test_overlapping_terms_without_exact_cover() {
        // Terms [0,3) and [2,5) of one xorb share a block whose exact size the terms can't give.
        let (client, file_contents) = setup_test_file(&[(1, (0, 3)), (1, (2, 5)), (1, (1, 4))]).await;
        reconstruct_and_verify_full(&client, &file_contents, ReconstructionConfig::default()).await;
        let file_len = file_contents.data.len() as u64;
        reconstruct_and_verify_range(
            &client,
            &file_contents,
            FileRange::new(file_len / 3 + 7, file_len - 11),
            ReconstructionConfig::default(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_non_contiguous_chunks() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 2)), (1, (4, 6))]).await;
        let config = test_config();
        let result = reconstruct_to_vec(&client, file_contents.file_hash, None, &config, None)
            .await
            .unwrap();
        assert_eq!(result, file_contents.data);
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
        config.min_reconstruction_fetch_size = xet_runtime::utils::ByteSize::from("50");
        config.max_reconstruction_fetch_size = xet_runtime::utils::ByteSize::from("100");
        config.min_prefetch_buffer = xet_runtime::utils::ByteSize::from("50");
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
        let tiny_semaphore = AdjustableSemaphore::new(1, (1, 1));
        let ctx = XetContext::from_external(Handle::current(), XetConfig::new());

        FileReconstructor::new(&ctx, &(client.clone() as Arc<dyn Client>), file_contents.file_hash)
            .with_config(url_refresh_test_config())
            .with_buffer_semaphore(tiny_semaphore)
            .reconstruct_to_writer(writer)
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

        let tiny_semaphore = AdjustableSemaphore::new(1, (1, 1));
        let ctx = XetContext::from_external(Handle::current(), XetConfig::new());

        FileReconstructor::new(&ctx, &(client.clone() as Arc<dyn Client>), file_contents.file_hash)
            .with_config(url_refresh_test_config())
            .with_buffer_semaphore(tiny_semaphore)
            .reconstruct_to_writer(writer)
            .await
            .expect("Reconstruction should succeed");

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

        let tiny_semaphore = AdjustableSemaphore::new(1, (1, 1));
        let ctx = XetContext::from_external(Handle::current(), XetConfig::new());

        FileReconstructor::new(&ctx, &(client.clone() as Arc<dyn Client>), file_contents.file_hash)
            .with_config(url_refresh_test_config())
            .with_buffer_semaphore(tiny_semaphore)
            .reconstruct_to_writer(writer)
            .await
            .expect("Reconstruction should succeed");

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

        let tiny_semaphore = AdjustableSemaphore::new(1, (1, 1));
        let ctx = XetContext::from_external(Handle::current(), XetConfig::new());

        FileReconstructor::new(&ctx, &(client.clone() as Arc<dyn Client>), file_contents.file_hash)
            .with_config(url_refresh_test_config())
            .with_buffer_semaphore(tiny_semaphore)
            .reconstruct_to_writer(writer)
            .await
            .expect("Reconstruction should succeed");

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

        let tiny_semaphore = AdjustableSemaphore::new(1, (0, 1));
        let ctx = XetContext::from_external(Handle::current(), XetConfig::new());

        let range = FileRange::new(file_len / 4, file_len * 3 / 4);

        FileReconstructor::new(&ctx, &(client.clone() as Arc<dyn Client>), file_contents.file_hash)
            .with_byte_range(range)
            .with_config(url_refresh_test_config())
            .with_buffer_semaphore(tiny_semaphore)
            .reconstruct_to_writer(writer)
            .await
            .expect("Reconstruction should succeed");

        let reconstructed = writer_buffer.lock().unwrap().clone();
        let expected = &file_contents.data[range.start as usize..range.end as usize];
        assert_eq!(reconstructed, expected);
    }

    #[test]
    fn test_dynamic_buffer_scaling_noop_increment_preserves_total_permits() {
        let mut runtime_config = xet_runtime::config::XetConfig::new();
        runtime_config.reconstruction.download_buffer_size = xet_runtime::utils::ByteSize::from("1kb");
        runtime_config.reconstruction.download_buffer_limit = xet_runtime::utils::ByteSize::from("4kb");
        let expected_total = runtime_config.reconstruction.download_buffer_limit.as_u64();

        let ctx = XetContext::with_config(runtime_config).unwrap();
        let runtime = ctx.runtime.clone();

        runtime
            .bridge_sync(async move {
                let ctx = ctx.clone();
                let (client, file_contents) = setup_test_file(&[(1, (0, 2)), (2, (0, 2)), (3, (0, 2))]).await;
                let sem = ctx.common.reconstruction_download_buffer.clone();

                // Pre-grow to max so the run's increment request is a no-op.
                let p = sem.increment_total_permits(u64::MAX).unwrap();
                drop(p);
                assert_eq!(sem.total_permits(), expected_total);

                let mut config = test_config();
                config.download_buffer_perfile_size = xet_runtime::utils::ByteSize::from("8kb");

                let reconstructed = reconstruct_to_vec(&client, file_contents.file_hash, None, &config, None)
                    .await
                    .unwrap();
                assert_eq!(reconstructed, file_contents.data);

                assert_eq!(sem.total_permits(), expected_total);
                assert_eq!(ctx.common.active_downloads.load(Ordering::Relaxed), 0);
            })
            .unwrap();
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
        FileReconstructor::new(&XetContext::default().unwrap(), &(client.clone() as Arc<dyn Client>), file_hash)
            .with_byte_range(range)
            .with_config(config)
            .reconstruct_to_file(file_path, None, false)
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

        let client = LocalClient::temporary(XetContext::default().unwrap()).await.unwrap();
        let file_contents = client.upload_random_file(&term_spec, LARGE_CHUNK_SIZE).await.unwrap();
        let file_len = file_contents.data.len() as u64;

        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("output.bin");

        // Pre-create the file with zeros.
        std::fs::write(&file_path, vec![0u8; file_len as usize]).unwrap();

        // Use a config with larger fetch sizes for the concurrent test.
        let mut config = ReconstructionConfig::default();
        config.min_reconstruction_fetch_size = xet_runtime::utils::ByteSize::from("32kb");
        config.max_reconstruction_fetch_size = xet_runtime::utils::ByteSize::from("128kb");

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
                FileReconstructor::new(&XetContext::default().unwrap(), &(client as Arc<dyn Client>), file_hash)
                    .with_byte_range(range)
                    .with_config(config)
                    .reconstruct_to_file(&file_path, None, false)
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

    // ==================== Multi-Disjoint Range Tests (LocalClient) ====================
    //
    // These tests exercise complex disjoint range patterns through the LocalClient path
    // (no HTTP server), ensuring the reconstruction logic handles V2 multi-range
    // XorbBlocks correctly.

    /// Single xorb with three disjoint chunk ranges.
    #[tokio::test]
    async fn test_triple_disjoint_ranges_full() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 2)), (1, (4, 6)), (1, (8, 10))]).await;
        reconstruct_and_verify_full(&client, &file_contents, test_config()).await;
    }

    /// Single xorb with three disjoint chunk ranges, partial byte range.
    #[tokio::test]
    async fn test_triple_disjoint_ranges_partial() {
        let (client, file_contents) = setup_test_file(&[(1, (0, 2)), (1, (4, 6)), (1, (8, 10))]).await;
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(file_len / 4, file_len * 3 / 4);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    /// Multiple xorbs, each with multiple disjoint ranges, interleaved.
    #[tokio::test]
    async fn test_multi_xorb_interleaved_disjoint() {
        let term_spec = &[
            (1, (0, 2)),
            (2, (0, 2)),
            (1, (4, 6)),
            (2, (4, 6)),
            (1, (8, 10)),
            (2, (8, 10)),
        ];
        let (client, file_contents) = setup_test_file(term_spec).await;
        reconstruct_and_verify_full(&client, &file_contents, test_config()).await;
    }

    /// Multiple xorbs with interleaved disjoint ranges, partial byte range.
    #[tokio::test]
    async fn test_multi_xorb_interleaved_disjoint_partial() {
        let term_spec = &[
            (1, (0, 2)),
            (2, (0, 2)),
            (1, (4, 6)),
            (2, (4, 6)),
            (1, (8, 10)),
            (2, (8, 10)),
        ];
        let (client, file_contents) = setup_test_file(term_spec).await;
        let file_len = file_contents.data.len() as u64;
        let range = FileRange::new(file_len / 3, file_len * 2 / 3);
        reconstruct_and_verify_range(&client, &file_contents, range, test_config()).await;
    }

    /// Single xorb with four disjoint ranges (many gaps).
    #[tokio::test]
    async fn test_four_disjoint_ranges() {
        let term_spec = &[(1, (0, 2)), (1, (4, 6)), (1, (8, 10)), (1, (12, 14))];
        let (client, file_contents) = setup_test_file(term_spec).await;
        reconstruct_and_verify_full(&client, &file_contents, test_config()).await;
    }

    /// Mix of contiguous and disjoint ranges from the same xorb.
    #[tokio::test]
    async fn test_mixed_contiguous_and_disjoint() {
        let term_spec = &[
            (1, (0, 3)),  // contiguous block
            (1, (3, 5)),  // continues contiguously
            (1, (8, 10)), // gap, then disjoint
        ];
        let (client, file_contents) = setup_test_file(term_spec).await;
        reconstruct_and_verify_full(&client, &file_contents, test_config()).await;
    }

    /// Disjoint ranges across three xorbs with a complex access pattern.
    #[tokio::test]
    async fn test_complex_three_xorb_disjoint() {
        let term_spec = &[
            (1, (0, 2)),
            (2, (0, 3)),
            (3, (2, 5)),
            (1, (5, 8)),
            (2, (6, 8)),
            (3, (0, 2)),
        ];
        let (client, file_contents) = setup_test_file(term_spec).await;
        reconstruct_and_verify_full(&client, &file_contents, test_config()).await;
    }

    /// LocalClient with max_ranges_per_fetch=2 (tests V2 response splitting without HTTP).
    #[tokio::test]
    async fn test_local_client_max_ranges_2_disjoint() {
        let client = LocalClient::temporary(XetContext::default().unwrap()).await.unwrap();
        client.set_max_ranges_per_fetch(2);

        let term_spec = &[(1, (0, 2)), (1, (4, 6)), (1, (8, 10)), (1, (12, 14))];
        let file_contents = client.upload_random_file(term_spec, TEST_CHUNK_SIZE).await.unwrap();

        let config = test_config();
        let result = reconstruct_to_vec(&client, file_contents.file_hash, None, &config, None)
            .await
            .unwrap();
        assert_eq!(result, file_contents.data.as_ref());
    }

    /// LocalClient with max_ranges_per_fetch=1 (every range gets its own fetch entry).
    #[tokio::test]
    async fn test_local_client_max_ranges_1_multi_xorb() {
        let client = LocalClient::temporary(XetContext::default().unwrap()).await.unwrap();
        client.set_max_ranges_per_fetch(1);

        let term_spec = &[(1, (0, 2)), (2, (0, 2)), (1, (4, 6)), (2, (4, 6))];
        let file_contents = client.upload_random_file(term_spec, TEST_CHUNK_SIZE).await.unwrap();

        let config = test_config();
        let result = reconstruct_to_vec(&client, file_contents.file_hash, None, &config, None)
            .await
            .unwrap();
        assert_eq!(result, file_contents.data.as_ref());
    }

    // ==================== Cancellation Flag Tests ====================

    mod cancellation_tests {
        use tokio_util::sync::CancellationToken;

        use super::*;

        #[tokio::test]
        async fn test_cancellation_token_before_start() {
            let (client, file_contents) = setup_test_file(&[(1, (0, 3))]).await;
            let config = test_config();

            let token = CancellationToken::new();
            token.cancel();
            let buffer = Arc::new(std::sync::Mutex::new(Cursor::new(Vec::new())));
            let writer = StaticCursorWriter(buffer.clone());

            let bytes_written = FileReconstructor::new(
                &XetContext::default().unwrap(),
                &(client.clone() as Arc<dyn Client>),
                file_contents.file_hash,
            )
            .with_config(&config)
            .with_cancellation_token(token)
            .reconstruct_to_writer(writer)
            .await
            .unwrap();

            assert_eq!(bytes_written, 0);
        }

        /// A writer that cancels a token after a certain number of writes,
        /// used to deterministically test mid-reconstruction cancellation.
        struct CancellingWriter {
            buffer: Arc<std::sync::Mutex<Vec<u8>>>,
            cancel_token: CancellationToken,
            write_count: AtomicUsize,
            cancel_after_writes: usize,
        }

        impl Write for CancellingWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                let n = self.buffer.lock().unwrap().write(buf)?;
                let count = self.write_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= self.cancel_after_writes {
                    self.cancel_token.cancel();
                }
                Ok(n)
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        #[tokio::test]
        async fn test_cancellation_token_during_reconstruction() {
            let term_spec: Vec<(u64, (u64, u64))> = (1..=10).map(|i| (i, (0, 5))).collect();
            let (client, file_contents) = setup_test_file(&term_spec).await;
            let config = test_config();

            let token = CancellationToken::new();
            let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));

            let writer = CancellingWriter {
                buffer: buffer.clone(),
                cancel_token: token.clone(),
                write_count: AtomicUsize::new(0),
                cancel_after_writes: 1,
            };

            // Use a tiny semaphore to force sequential term processing.
            let tiny_semaphore = AdjustableSemaphore::new(1, (1, 1));

            let bytes_written = FileReconstructor::new(
                &XetContext::default().unwrap(),
                &(client.clone() as Arc<dyn Client>),
                file_contents.file_hash,
            )
            .with_config(&config)
            .with_cancellation_token(token)
            .with_buffer_semaphore(tiny_semaphore)
            .reconstruct_to_writer(writer)
            .await
            .unwrap();

            // Verify cancellation returned Ok(0) and only partial data was written.
            assert_eq!(bytes_written, 0);
            let written = buffer.lock().unwrap().len();
            assert!(written < file_contents.data.len());
        }

        #[tokio::test]
        async fn test_cancellation_token_not_set_completes_normally() {
            let (client, file_contents) = setup_test_file(&[(1, (0, 3)), (2, (0, 2))]).await;
            let config = test_config();

            let token = CancellationToken::new();
            let buffer = Arc::new(std::sync::Mutex::new(Cursor::new(Vec::new())));
            let writer = StaticCursorWriter(buffer.clone());

            let bytes_written = FileReconstructor::new(
                &XetContext::default().unwrap(),
                &(client.clone() as Arc<dyn Client>),
                file_contents.file_hash,
            )
            .with_config(&config)
            .with_cancellation_token(token)
            .reconstruct_to_writer(writer)
            .await
            .unwrap();

            assert_eq!(bytes_written, file_contents.data.len() as u64);
            assert_eq!(buffer.lock().unwrap().get_ref().clone(), file_contents.data);
        }
    }

    // ==================== Multirange Fetching Tests (LocalClient) ====================

    mod multirange_tests {
        use super::*;

        fn with_multirange_config(enable: bool) -> XetContext {
            let mut config = xet_runtime::config::XetConfig::new();
            config.client.enable_multirange_fetching = enable;
            XetContext::with_config(config).unwrap()
        }

        /// Exercises multiple disjoint-range scenarios through LocalClient with both
        /// enable_multirange_fetching=true and =false.
        #[test]
        fn test_multirange_local_client() {
            for enable in [false, true] {
                let ctx = with_multirange_config(enable);
                ctx.runtime
                    .bridge_sync(async move {
                        let scenarios: Vec<Vec<(u64, (u64, u64))>> = vec![
                            vec![(1, (0, 2)), (1, (4, 6)), (1, (8, 10))],
                            vec![
                                (1, (0, 2)),
                                (2, (0, 2)),
                                (1, (4, 6)),
                                (2, (4, 6)),
                                (1, (8, 10)),
                                (2, (8, 10)),
                            ],
                            vec![
                                (1, (0, 2)),
                                (2, (0, 3)),
                                (3, (2, 5)),
                                (1, (5, 8)),
                                (2, (6, 8)),
                                (3, (0, 2)),
                            ],
                        ];
                        let config = test_config();
                        for term_spec in &scenarios {
                            let (client, fc) = setup_test_file(term_spec).await;
                            reconstruct_and_verify_full(&client, &fc, config.clone()).await;

                            let file_len = fc.data.len() as u64;
                            let range = FileRange::new(file_len / 4, file_len * 3 / 4);
                            reconstruct_and_verify_range(&client, &fc, range, config.clone()).await;
                        }
                    })
                    .unwrap();
            }
        }

        /// LocalClient with max_ranges_per_fetch constraint, both enable settings.
        #[test]
        fn test_multirange_max_ranges() {
            for enable in [false, true] {
                let ctx = with_multirange_config(enable);
                ctx.runtime
                    .bridge_sync(async {
                        let client = LocalClient::temporary(XetContext::default().unwrap()).await.unwrap();
                        client.set_max_ranges_per_fetch(2);

                        let term_spec = &[(1, (0, 2)), (1, (4, 6)), (1, (8, 10)), (1, (12, 14))];
                        let fc = client.upload_random_file(term_spec, TEST_CHUNK_SIZE).await.unwrap();

                        let config = test_config();
                        let result = reconstruct_to_vec(&client, fc.file_hash, None, &config, None).await.unwrap();
                        assert_eq!(result, fc.data.as_ref());
                    })
                    .unwrap();
            }
        }
    }

    // ==================== Server-dependent tests (require simulation feature) ====================
    #[cfg(feature = "simulation")]
    mod server_tests {
        use super::*;

        #[tokio::test]
        async fn test_known_file_size_bounds_reconstruction_requests() {
            let server = xet_client::cas_client::LocalTestServerBuilder::new().start().await;
            let file = server
                .client()
                .upload_random_file(&[(1, (0, 3))], TEST_CHUNK_SIZE)
                .await
                .unwrap();
            server.disable_v2_endpoints(404);
            let file_size = file.data.len() as u64;
            let mut config = test_config();
            // An exact fetch boundary would require another query to discover EOF
            // if the reconstructor ignored the supplied file size.
            config.min_reconstruction_fetch_size = file_size.into();
            config.max_reconstruction_fetch_size = (3 * file_size).into();

            let buffer = Arc::new(std::sync::Mutex::new(Cursor::new(Vec::new())));
            let written =
                FileReconstructor::new(&XetContext::default().unwrap(), &server.remote_client(), file.file_hash)
                    .with_config(&config)
                    .with_file_size(file_size)
                    .reconstruct_to_writer(StaticCursorWriter(buffer.clone()))
                    .await
                    .unwrap();
            assert_eq!(written, file_size);
            assert_eq!(buffer.lock().unwrap().get_ref(), &file.data);
            assert_eq!(server.take_v1_reconstruction_ranges(), vec![Some(format!("bytes=0-{}", file_size - 1))]);
        }

        // ==================== V1 Fallback Tests ====================
        //
        // These tests use LocalTestServer with V2 disabled to verify that
        // reconstruction works correctly when the client falls back from V2 to V1.

        /// Helper to reconstruct through a LocalTestServer (RemoteClient HTTP path).
        async fn reconstruct_via_server(
            server: &xet_client::cas_client::LocalTestServer,
            file_hash: MerkleHash,
            byte_range: Option<FileRange>,
            config: &ReconstructionConfig,
        ) -> Result<Vec<u8>> {
            let buffer = Arc::new(std::sync::Mutex::new(Cursor::new(Vec::new())));
            let writer = StaticCursorWriter(buffer.clone());

            let client: Arc<dyn Client> = server.remote_client().clone();
            let mut reconstructor =
                FileReconstructor::new(&XetContext::default().unwrap(), &client, file_hash).with_config(config);

            if let Some(range) = byte_range {
                reconstructor = reconstructor.with_byte_range(range);
            }

            reconstructor.reconstruct_to_writer(writer).await?;

            let data = buffer.lock().unwrap().get_ref().clone();
            Ok(data)
        }

        #[tokio::test]
        async fn test_v1_fallback_full_reconstruction() {
            let server = xet_client::cas_client::LocalTestServerBuilder::new().start().await;
            let file_contents = server
                .remote_client()
                .upload_random_file(&[(1, (0, 3)), (2, (0, 2))], TEST_CHUNK_SIZE)
                .await
                .unwrap();

            server.disable_v2_endpoints(404);

            let config = test_config();
            let result = reconstruct_via_server(&server, file_contents.file_hash, None, &config)
                .await
                .unwrap();
            assert_eq!(result, file_contents.data.as_ref());
        }

        #[tokio::test]
        async fn test_v1_fallback_partial_range() {
            let server = xet_client::cas_client::LocalTestServerBuilder::new().start().await;
            let file_contents = server
                .remote_client()
                .upload_random_file(&[(1, (0, 5)), (2, (0, 3))], TEST_CHUNK_SIZE)
                .await
                .unwrap();

            server.disable_v2_endpoints(404);

            let file_len = file_contents.data.len() as u64;
            let range = FileRange::new(file_len / 4, file_len * 3 / 4);

            let config = test_config();
            let result = reconstruct_via_server(&server, file_contents.file_hash, Some(range), &config)
                .await
                .unwrap();
            assert_eq!(result, &file_contents.data[range.start as usize..range.end as usize]);
        }

        #[tokio::test]
        async fn test_v1_fallback_non_contiguous_chunks() {
            let server = xet_client::cas_client::LocalTestServerBuilder::new().start().await;
            let file_contents = server
                .remote_client()
                .upload_random_file(&[(1, (0, 2)), (1, (4, 6))], TEST_CHUNK_SIZE)
                .await
                .unwrap();

            server.disable_v2_endpoints(404);

            let config = test_config();
            let result = reconstruct_via_server(&server, file_contents.file_hash, None, &config)
                .await
                .unwrap();
            assert_eq!(result, file_contents.data.as_ref());
        }

        #[tokio::test]
        async fn test_v1_fallback_multiple_xorbs() {
            let server = xet_client::cas_client::LocalTestServerBuilder::new().start().await;
            let file_contents = server
                .remote_client()
                .upload_random_file(&[(1, (0, 2)), (2, (0, 3)), (3, (0, 2)), (1, (2, 4))], TEST_CHUNK_SIZE)
                .await
                .unwrap();

            server.disable_v2_endpoints(404);

            let config = test_config();
            let result = reconstruct_via_server(&server, file_contents.file_hash, None, &config)
                .await
                .unwrap();
            assert_eq!(result, file_contents.data.as_ref());
        }

        /// V1 fallback with three disjoint ranges from the same xorb.
        #[tokio::test]
        async fn test_v1_fallback_triple_disjoint_ranges() {
            let server = xet_client::cas_client::LocalTestServerBuilder::new().start().await;
            let file_contents = server
                .remote_client()
                .upload_random_file(&[(1, (0, 2)), (1, (4, 6)), (1, (8, 10))], TEST_CHUNK_SIZE)
                .await
                .unwrap();

            server.disable_v2_endpoints(404);

            let config = test_config();
            let result = reconstruct_via_server(&server, file_contents.file_hash, None, &config)
                .await
                .unwrap();
            assert_eq!(result, file_contents.data.as_ref());
        }

        // ==================== Max Ranges Tests (via server) ====================

        /// Helper to set up a server with max_ranges_per_fetch and reconstruct.
        async fn reconstruct_via_server_with_max_ranges(
            term_spec: &[(u64, (u64, u64))],
            max_ranges: usize,
            byte_range: Option<FileRange>,
        ) -> (Vec<u8>, RandomFileContents) {
            let server = xet_client::cas_client::LocalTestServerBuilder::new().start().await;
            let file_contents = server
                .remote_client()
                .upload_random_file(term_spec, TEST_CHUNK_SIZE)
                .await
                .unwrap();

            server.set_max_ranges_per_fetch(max_ranges);

            let config = test_config();
            let result = reconstruct_via_server(&server, file_contents.file_hash, byte_range, &config)
                .await
                .unwrap();
            (result, file_contents)
        }

        #[tokio::test]
        async fn test_max_ranges_simple() {
            let (result, file_contents) =
                reconstruct_via_server_with_max_ranges(&[(1, (0, 3)), (2, (0, 2))], 2, None).await;
            assert_eq!(result, file_contents.data.as_ref());
        }

        #[tokio::test]
        async fn test_max_ranges_1_disjoint() {
            let (result, file_contents) =
                reconstruct_via_server_with_max_ranges(&[(1, (0, 2)), (1, (4, 6))], 1, None).await;
            assert_eq!(result, file_contents.data.as_ref());
        }

        #[tokio::test]
        async fn test_max_ranges_2_triple_disjoint() {
            let (result, file_contents) =
                reconstruct_via_server_with_max_ranges(&[(1, (0, 2)), (1, (4, 6)), (1, (8, 10))], 2, None).await;
            assert_eq!(result, file_contents.data.as_ref());
        }

        #[tokio::test]
        async fn test_max_ranges_2_multi_xorb_disjoint() {
            let term_spec = &[
                (1, (0, 2)),
                (2, (0, 2)),
                (1, (4, 6)),
                (2, (4, 6)),
                (1, (8, 10)),
                (2, (8, 10)),
            ];
            let (result, file_contents) = reconstruct_via_server_with_max_ranges(term_spec, 2, None).await;
            assert_eq!(result, file_contents.data.as_ref());
        }

        #[tokio::test]
        async fn test_max_ranges_2_partial_range() {
            let term_spec = &[
                (1, (0, 3)),
                (2, (0, 2)),
                (1, (3, 5)),
                (3, (1, 4)),
                (2, (4, 6)),
                (1, (0, 2)),
            ];
            let server = xet_client::cas_client::LocalTestServerBuilder::new().start().await;
            let file_contents = server
                .remote_client()
                .upload_random_file(term_spec, TEST_CHUNK_SIZE)
                .await
                .unwrap();

            server.set_max_ranges_per_fetch(2);

            let file_len = file_contents.data.len() as u64;
            let range = FileRange::new(file_len / 4, file_len * 3 / 4);

            let config = test_config();
            let result = reconstruct_via_server(&server, file_contents.file_hash, Some(range), &config)
                .await
                .unwrap();
            assert_eq!(result, &file_contents.data[range.start as usize..range.end as usize]);
        }

        // ==================== Multirange via Server ====================

        fn with_multirange_config(enable: bool) -> XetContext {
            let mut config = xet_runtime::config::XetConfig::new();
            config.client.enable_multirange_fetching = enable;
            XetContext::with_config(config).unwrap()
        }

        /// Exercises HTTP server path with full, max-ranges-split, and partial-range
        /// reconstruction, both enable_multirange_fetching values.
        #[test]
        fn test_multirange_via_server() {
            for enable in [false, true] {
                let ctx = with_multirange_config(enable);
                ctx.runtime
                    .bridge_sync(async {
                        let config = test_config();

                        // Full reconstruction with disjoint ranges
                        let server = xet_client::cas_client::LocalTestServerBuilder::new().start().await;
                        let fc = server
                            .remote_client()
                            .upload_random_file(&[(1, (0, 2)), (1, (4, 6)), (1, (8, 10))], TEST_CHUNK_SIZE)
                            .await
                            .unwrap();
                        let result = reconstruct_via_server(&server, fc.file_hash, None, &config).await.unwrap();
                        assert_eq!(result, fc.data.as_ref());

                        // Multi-xorb with max_ranges_per_fetch=2
                        let server = xet_client::cas_client::LocalTestServerBuilder::new().start().await;
                        let fc = server
                            .remote_client()
                            .upload_random_file(
                                &[(1, (0, 2)), (2, (0, 2)), (1, (4, 6)), (2, (4, 6)), (1, (8, 10))],
                                TEST_CHUNK_SIZE,
                            )
                            .await
                            .unwrap();
                        server.set_max_ranges_per_fetch(2);
                        let result = reconstruct_via_server(&server, fc.file_hash, None, &config).await.unwrap();
                        assert_eq!(result, fc.data.as_ref());

                        // Partial byte range
                        let server = xet_client::cas_client::LocalTestServerBuilder::new().start().await;
                        let fc = server
                            .remote_client()
                            .upload_random_file(&[(1, (0, 3)), (2, (0, 2)), (1, (3, 5)), (2, (4, 6))], TEST_CHUNK_SIZE)
                            .await
                            .unwrap();
                        let file_len = fc.data.len() as u64;
                        let range = FileRange::new(file_len / 4, file_len * 3 / 4);
                        let result = reconstruct_via_server(&server, fc.file_hash, Some(range), &config)
                            .await
                            .unwrap();
                        assert_eq!(result, &fc.data[range.start as usize..range.end as usize]);
                    })
                    .unwrap();
            }
        }
    } // mod server_tests

    /// Streams deliver each block's bytes as they are decompressed, not after the whole block has downloaded.
    mod streaming_tests {
        use bytes::Bytes;
        use tokio::sync::watch;
        use xet_client::cas_client::adaptive_concurrency::ConnectionPermit;
        use xet_client::cas_client::{
            BlockByteRange, ProgressCallback, ReadAmount, ShardUploadProgressCallback, URLProvider, XorbBlockData,
        };
        use xet_client::cas_types::{
            BatchQueryReconstructionResponse, FileChunkHashesResponse, QueryReconstructionResponseV2,
        };
        use xet_core_structures::metadata_shard::file_structs::MDBFileInfo;
        use xet_core_structures::xorb_object::SerializedXorbObject;

        use super::*;

        /// How `GatedClient` delivers a block.
        #[derive(Clone, Copy)]
        enum TermDataBehavior {
            /// Appends the first chunk, waits until the test opens the gate, then appends the rest.
            GateAfterFirstChunk,
            /// Appends the first chunk, then fails the download.
            FailAfterFirstChunk,
            /// Delivers the block as the inner client does, but each transfer progress report takes a while.
            DelayProgressReports,
            /// Allows one download at a time: a download permit is granted only once the previous block has
            /// finished downloading. Records each block's first bytes, in download order, in `downloads`.
            OneDownloadAtATime,
        }

        /// Wraps a `LocalClient`, changing how blocks are delivered; see `TermDataBehavior`.
        struct GatedClient {
            inner: Arc<LocalClient>,
            gate: watch::Receiver<bool>,
            behavior: TermDataBehavior,
            /// Download slots for `OneDownloadAtATime`.
            download_slots: Arc<tokio::sync::Semaphore>,
            /// The first bytes of each downloaded block, in download order (`OneDownloadAtATime` only).
            downloads: Arc<std::sync::Mutex<Vec<Bytes>>>,
        }

        impl GatedClient {
            fn new(inner: Arc<LocalClient>, gate: watch::Receiver<bool>, behavior: TermDataBehavior) -> Self {
                Self {
                    inner,
                    gate,
                    behavior,
                    download_slots: Arc::new(tokio::sync::Semaphore::new(1)),
                    downloads: Arc::new(std::sync::Mutex::new(Vec::new())),
                }
            }
        }

        #[async_trait::async_trait]
        impl Client for GatedClient {
            async fn get_file_reconstruction_info(
                &self,
                file_hash: &MerkleHash,
            ) -> xet_client::error::Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
                self.inner.get_file_reconstruction_info(file_hash).await
            }

            async fn get_reconstruction(
                &self,
                file_id: &MerkleHash,
                bytes_range: Option<FileRange>,
            ) -> xet_client::error::Result<Option<QueryReconstructionResponseV2>> {
                self.inner.get_reconstruction(file_id, bytes_range).await
            }

            async fn batch_get_reconstruction(
                &self,
                file_ids: &[MerkleHash],
            ) -> xet_client::error::Result<BatchQueryReconstructionResponse> {
                self.inner.batch_get_reconstruction(file_ids).await
            }

            async fn acquire_download_permit(&self) -> xet_client::error::Result<ConnectionPermit> {
                if let TermDataBehavior::OneDownloadAtATime = self.behavior {
                    // Returned by `get_file_term_data` once the download is done.
                    self.download_slots.acquire().await.unwrap().forget();
                }
                self.inner.acquire_download_permit().await
            }

            async fn get_file_term_data(
                &self,
                url_info: Box<dyn URLProvider>,
                download_permit: ConnectionPermit,
                progress_callback: Option<ProgressCallback>,
                output: Arc<XorbBlockData>,
            ) -> xet_client::error::Result<()> {
                if let TermDataBehavior::DelayProgressReports = self.behavior {
                    // A report made after the last chunk is appended would land after the reconstruction
                    // completes, failing its progress check.
                    let delayed: Option<ProgressCallback> = progress_callback.map(|cb| {
                        Arc::new(move |delta: u64, completed: u64, total: u64| {
                            std::thread::sleep(Duration::from_millis(100));
                            cb(delta, completed, total);
                        }) as ProgressCallback
                    });
                    return self.inner.get_file_term_data(url_info, download_permit, delayed, output).await;
                }

                if let TermDataBehavior::OneDownloadAtATime = self.behavior {
                    let result = self
                        .inner
                        .get_file_term_data(url_info, download_permit, progress_callback, output.clone())
                        .await;
                    let first_bytes = BlockByteRange {
                        first_chunk: 0,
                        offset_into_first_chunk: 0,
                        len: 16,
                    };
                    let prefix = output.try_read(&first_bytes, 0, ReadAmount::All).unwrap().unwrap();
                    self.downloads.lock().unwrap().push(prefix);
                    self.download_slots.add_permits(1);
                    return result;
                }

                let full = Arc::new(XorbBlockData::with_size_bound(output.capacity()));
                self.inner
                    .get_file_term_data(url_info, download_permit, progress_callback, full.clone())
                    .await?;
                full.finish()?;
                let (data, offsets) = full.contents().unwrap();

                let first_end = offsets[1];
                output.append_chunks(&data[..first_end as usize], &[0, first_end])?;

                match self.behavior {
                    TermDataBehavior::GateAfterFirstChunk => {
                        let _ = self.gate.clone().wait_for(|open| *open).await;
                    },
                    TermDataBehavior::FailAfterFirstChunk => {
                        return Err(xet_client::ClientError::Other("simulated failure after first chunk".into()));
                    },
                    TermDataBehavior::DelayProgressReports | TermDataBehavior::OneDownloadAtATime => {
                        unreachable!("handled above")
                    },
                }

                let rest: Vec<u32> = offsets[1..].iter().map(|o| o - first_end).collect();
                output.append_chunks(&data[first_end as usize..], &rest)
            }

            async fn query_for_global_dedup_shard(
                &self,
                prefix: &str,
                chunk_hash: &MerkleHash,
            ) -> xet_client::error::Result<Option<Bytes>> {
                self.inner.query_for_global_dedup_shard(prefix, chunk_hash).await
            }

            async fn acquire_upload_permit(&self) -> xet_client::error::Result<ConnectionPermit> {
                self.inner.acquire_upload_permit().await
            }

            async fn upload_shard(
                &self,
                shard_data: Bytes,
                upload_permit: ConnectionPermit,
                progress_callback: Option<ShardUploadProgressCallback>,
            ) -> xet_client::error::Result<Option<MerkleHash>> {
                self.inner.upload_shard(shard_data, upload_permit, progress_callback).await
            }

            async fn upload_xorb(
                &self,
                prefix: &str,
                serialized_xorb_object: SerializedXorbObject,
                progress_callback: Option<ProgressCallback>,
                upload_permit: ConnectionPermit,
            ) -> xet_client::error::Result<u64> {
                self.inner
                    .upload_xorb(prefix, serialized_xorb_object, progress_callback, upload_permit)
                    .await
            }

            async fn get_file_chunk_hashes(
                &self,
                file_id: &MerkleHash,
                dirty_ranges: Vec<FileRange>,
            ) -> xet_client::error::Result<FileChunkHashesResponse> {
                self.inner.get_file_chunk_hashes(file_id, dirty_ranges).await
            }
        }

        /// One term covering 40 chunks of a single xorb, so the whole file is one block. Reconstructs the bytes
        /// from `trim.0` to `trim.1` before the end, if given. Returns the gated reconstructor, the gate, and the
        /// file contents.
        async fn gated_single_block_file(
            behavior: TermDataBehavior,
            trim: Option<(u64, u64)>,
        ) -> (FileReconstructor, watch::Sender<bool>, RandomFileContents) {
            let (client, file_contents) = setup_test_file(&[(1, (0, 40))]).await;
            let (gate_tx, gate_rx) = watch::channel(false);
            let gated: Arc<dyn Client> = Arc::new(GatedClient::new(client, gate_rx, behavior));
            let mut reconstructor =
                FileReconstructor::new(&XetContext::default().unwrap(), &gated, file_contents.file_hash);
            if let Some((start, end_trim)) = trim {
                let len = file_contents.data.len() as u64;
                reconstructor = reconstructor.with_byte_range(FileRange::new(start, len - end_trim));
            }
            (reconstructor, gate_tx, file_contents)
        }

        const FIRST_BYTES_TIMEOUT: Duration = Duration::from_secs(10);

        #[tokio::test]
        async fn test_stream_yields_first_chunk_before_block_completes() {
            let (reconstructor, gate, file_contents) =
                gated_single_block_file(TermDataBehavior::GateAfterFirstChunk, None).await;
            let mut stream = reconstructor.reconstruct_to_stream();

            // The rest of the block is held back until the stream has returned its first bytes.
            let first = tokio::time::timeout(FIRST_BYTES_TIMEOUT, stream.next())
                .await
                .expect("the first chunk must be streamed before the rest of its block downloads")
                .unwrap()
                .unwrap();
            assert!(!first.is_empty() && first.len() < file_contents.data.len());
            assert_eq!(first, file_contents.data[..first.len()]);

            gate.send(true).unwrap();
            let mut data = first.to_vec();
            while let Some(piece) = stream.next().await.unwrap() {
                data.extend_from_slice(&piece);
            }
            assert_eq!(data, file_contents.data);
        }

        #[tokio::test]
        async fn test_blocking_stream_yields_first_chunk_before_block_completes() {
            let (reconstructor, gate, file_contents) =
                gated_single_block_file(TermDataBehavior::GateAfterFirstChunk, None).await;
            let mut stream = reconstructor.reconstruct_to_stream();

            let reader = tokio::task::spawn_blocking(move || {
                let first = stream.blocking_next().unwrap().unwrap();
                gate.send(true).unwrap();
                let mut data = first.to_vec();
                while let Some(piece) = stream.blocking_next().unwrap() {
                    data.extend_from_slice(&piece);
                }
                (first.len(), data)
            });
            let (first_len, data) = tokio::time::timeout(FIRST_BYTES_TIMEOUT, reader)
                .await
                .expect("the first chunk must be streamed before the rest of its block downloads")
                .unwrap();
            assert!(first_len < file_contents.data.len());
            assert_eq!(data, file_contents.data);
        }

        #[tokio::test]
        async fn test_unordered_stream_yields_first_chunk_before_block_completes() {
            let (reconstructor, gate, file_contents) =
                gated_single_block_file(TermDataBehavior::GateAfterFirstChunk, None).await;
            let mut stream = reconstructor.reconstruct_to_unordered_stream();

            let (offset, first) = tokio::time::timeout(FIRST_BYTES_TIMEOUT, stream.next())
                .await
                .expect("the first chunk must be streamed before the rest of its block downloads")
                .unwrap()
                .unwrap();
            assert_eq!(offset, 0);
            assert!(first.len() < file_contents.data.len());

            gate.send(true).unwrap();
            let mut data = first.to_vec();
            while let Some((offset, piece)) = stream.next().await.unwrap() {
                assert_eq!(offset, data.len() as u64);
                data.extend_from_slice(&piece);
            }
            assert_eq!(data, file_contents.data);
        }

        #[tokio::test]
        async fn test_stream_range_starting_mid_chunk() {
            // Start 50 bytes into the first chunk and end 30 bytes before the file does.
            let (reconstructor, gate, file_contents) =
                gated_single_block_file(TermDataBehavior::GateAfterFirstChunk, Some((50, 30))).await;
            let range = FileRange::new(50, file_contents.data.len() as u64 - 30);
            let mut stream = reconstructor.reconstruct_to_stream();
            let first = tokio::time::timeout(FIRST_BYTES_TIMEOUT, stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(first, file_contents.data[50..50 + first.len()]);

            gate.send(true).unwrap();
            let mut data = first.to_vec();
            while let Some(piece) = stream.next().await.unwrap() {
                data.extend_from_slice(&piece);
            }
            assert_eq!(data, file_contents.data[range.start as usize..range.end as usize]);
        }

        #[tokio::test]
        async fn test_stream_error_after_partial_block() {
            let (reconstructor, _gate, file_contents) =
                gated_single_block_file(TermDataBehavior::FailAfterFirstChunk, None).await;
            let mut stream = reconstructor.reconstruct_to_stream();

            // Bytes already delivered stay delivered; the failure surfaces on a later read.
            let mut received = Vec::new();
            let err = loop {
                match tokio::time::timeout(FIRST_BYTES_TIMEOUT, stream.next()).await.unwrap() {
                    Ok(Some(piece)) => received.extend_from_slice(&piece),
                    Ok(None) => panic!("stream must fail, not end"),
                    Err(err) => break err,
                }
            };
            assert!(matches!(err, FileReconstructionError::ClientError(_)), "{err:?}");
            assert!(received.len() < file_contents.data.len());
            assert_eq!(received, file_contents.data[..received.len()]);
        }

        // Multi-threaded, so the reconstruction can complete while a delayed progress report is pending.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_transfer_progress_complete_when_writing_finishes() {
            // Writing to a file asserts (in debug builds) that all transfer bytes were reported by the time the
            // last byte is written; a client reporting progress after appending a block's last chunk fails it.
            let (reconstructor, _gate, file_contents) =
                gated_single_block_file(TermDataBehavior::DelayProgressReports, None).await;
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().join("out.bin");
            let written = reconstructor.reconstruct_to_file(&path, None, true).await.unwrap();
            assert_eq!(written, file_contents.data.len() as u64);
            assert_eq!(std::fs::read(&path).unwrap(), file_contents.data);
        }

        // Multi-threaded, where the runtime runs spawned tasks in no particular order.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn test_blocks_download_in_file_order() {
            // Eight blocks, one per xorb, in file order.
            let term_spec: Vec<(u64, (u64, u64))> = (1..=8).map(|xorb| (xorb, (0, 4))).collect();
            let (client, file_contents) = setup_test_file(&term_spec).await;
            let (_gate_tx, gate_rx) = watch::channel(false);
            let gated = Arc::new(GatedClient::new(client, gate_rx, TermDataBehavior::OneDownloadAtATime));
            let downloads = gated.downloads.clone();

            let gated: Arc<dyn Client> = gated;
            let mut stream = FileReconstructor::new(&XetContext::default().unwrap(), &gated, file_contents.file_hash)
                .reconstruct_to_stream();
            let mut data = Vec::new();
            while let Some(piece) = tokio::time::timeout(FIRST_BYTES_TIMEOUT, stream.next()).await.unwrap().unwrap() {
                data.extend_from_slice(&piece);
            }
            assert_eq!(data, file_contents.data);

            // The last block is recorded after its data is appended, so the stream can end first.
            tokio::time::timeout(FIRST_BYTES_TIMEOUT, async {
                while downloads.lock().unwrap().len() < 8 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();

            // Each block's first bytes locate it in the file; blocks must have downloaded front to back.
            let offsets: Vec<usize> = downloads
                .lock()
                .unwrap()
                .iter()
                .map(|prefix| {
                    file_contents
                        .data
                        .windows(prefix.len())
                        .position(|window| window == prefix.as_ref())
                        .unwrap()
                })
                .collect();
            assert_eq!(offsets.len(), 8);
            assert!(offsets.windows(2).all(|w| w[0] < w[1]), "blocks downloaded out of file order: {offsets:?}");
        }
    } // mod streaming_tests
}
