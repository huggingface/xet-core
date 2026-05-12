#[cfg(not(target_family = "wasm"))]
use std::future::{self, Future};
#[cfg(not(target_family = "wasm"))]
use std::pin::Pin;
#[cfg(not(target_family = "wasm"))]
use std::sync::Arc;

#[cfg(not(target_family = "wasm"))]
use bytes::Bytes;
#[cfg(not(target_family = "wasm"))]
use chrono::{DateTime, Utc};
#[cfg(not(target_family = "wasm"))]
use tracing::{Instrument, debug_span, info, instrument};
use xet_core_structures::metadata_shard::Sha256;
#[cfg(not(target_family = "wasm"))]
use xet_core_structures::metadata_shard::file_structs::FileMetadataExt;
#[cfg(not(target_family = "wasm"))]
use xet_runtime::core::XetContext;

#[cfg(not(target_family = "wasm"))]
use super::XetFileInfo;
#[cfg(not(target_family = "wasm"))]
use super::deduplication_interface::UploadSessionDataManager;
#[cfg(not(target_family = "wasm"))]
use super::file_upload_session::FileUploadSession;
#[cfg(not(target_family = "wasm"))]
use super::sha256::Sha256Generator;
#[cfg(not(target_family = "wasm"))]
use crate::deduplication::{Chunk, Chunker, DeduplicationMetrics, FileDeduper};
#[cfg(not(target_family = "wasm"))]
use crate::error::Result;
#[cfg(not(target_family = "wasm"))]
use crate::progress_tracking::upload_tracking::CompletionTrackerFileId;

/// Controls how SHA-256 is handled during file cleaning.
#[derive(Clone, Copy)]
pub enum Sha256Policy {
    /// Compute SHA-256 from the file data.
    Compute,
    /// Use a pre-computed SHA-256 value.
    Provided(Sha256),
    /// Skip SHA-256 entirely; no metadata_ext is written to the shard.
    Skip,
}

impl Sha256Policy {
    /// Returns `Skip` when `true`, `Compute` when `false`.
    pub fn from_skip(skip: bool) -> Self {
        if skip { Self::Skip } else { Self::Compute }
    }

    /// Parses a hex-encoded SHA-256 string into a policy.
    ///
    /// Returns `Provided(hash)` if the hex is valid, `Compute` otherwise.
    pub fn from_hex(hex: &str) -> Self {
        Sha256::from_hex(hex).ok().into()
    }
}

impl From<Option<Sha256>> for Sha256Policy {
    fn from(sha256: Option<Sha256>) -> Self {
        match sha256 {
            Some(hash) => Self::Provided(hash),
            None => Self::Compute,
        }
    }
}

/// A class that encapsulates the clean and data task around a single file.
#[cfg(not(target_family = "wasm"))]
pub struct SingleFileCleaner {
    ctx: XetContext,

    // File name, if known.
    file_name: Option<Arc<str>>,

    // Completion id
    file_id: CompletionTrackerFileId,

    // Common state.
    session: Arc<FileUploadSession>,

    // The chunker.
    chunker: Chunker,

    // The deduplication interface.  Use a future that always returns the dedup manager
    // on await so that we can background this part.
    dedup_manager_fut: Pin<Box<dyn Future<Output = Result<FileDeduper<UploadSessionDataManager>>> + Send + 'static>>,

    // SHA-256 generator, present only when computing from file data.
    sha_generator: Option<Sha256Generator>,

    // Pre-computed or finalized SHA-256 value.
    provided_sha256: Option<Sha256>,

    // Start time
    start_time: DateTime<Utc>,
}

#[cfg(not(target_family = "wasm"))]
impl SingleFileCleaner {
    pub(crate) fn new(
        file_name: Option<Arc<str>>,
        file_id: CompletionTrackerFileId,
        sha256: Sha256Policy,
        session: Arc<FileUploadSession>,
    ) -> Self {
        let ctx = session.ctx.clone();
        let deduper = FileDeduper::new(UploadSessionDataManager::new(session.clone()), file_id, ctx.clone());

        let (sha_generator, provided_sha256) = match sha256 {
            Sha256Policy::Compute => (Some(Sha256Generator::new(ctx.clone())), None),
            Sha256Policy::Provided(hash) => (None, Some(hash)),
            Sha256Policy::Skip => (None, None),
        };

        Self {
            ctx,
            file_name,
            file_id,
            dedup_manager_fut: Box::pin(async move { Ok(deduper) }),
            session,
            chunker: crate::deduplication::Chunker::default(),
            sha_generator,
            provided_sha256,
            start_time: Utc::now(),
        }
    }

    /// Gets the dedupe manager to process new chunks, by first
    /// waiting for background operations to complete, then triggering a
    /// new background task.
    async fn deduper_process_chunks(&mut self, chunks: Arc<[Chunk]>) -> Result<()> {
        // Handle the move out by replacing it with a dummy future discarded below.
        let mut deduper = std::mem::replace(&mut self.dedup_manager_fut, Box::pin(future::pending())).await?;

        let num_chunks = chunks.len();

        let dedup_background = tokio::spawn(
            async move {
                deduper.process_chunks(&chunks).await?;
                Ok(deduper)
            }
            .instrument(debug_span!("deduper::process_chunks_task", num_chunks).or_current()),
        );

        self.dedup_manager_fut = Box::pin(async move { dedup_background.await? });

        Ok(())
    }

    pub async fn add_data(&mut self, data: &[u8]) -> Result<()> {
        self.add_data_from_bytes(Bytes::copy_from_slice(data)).await
    }

    pub async fn add_data_from_bytes(&mut self, data: Bytes) -> Result<()> {
        let block_size = *self.ctx.config.data.ingestion_block_size as usize;
        if data.len() > block_size {
            let mut pos = 0;
            while pos < data.len() {
                let next_pos = usize::min(pos + block_size, data.len());
                self.add_data_chunk_impl(data.slice(pos..next_pos)).await?;
                pos = next_pos;
            }
        } else {
            self.add_data_chunk_impl(data).await?;
        }

        Ok(())
    }

    #[instrument(skip_all, level="debug", name = "FileCleaner::add_data", fields(file_name=self.file_name.as_ref().map(|s|s.to_string()), len=data.len()))]
    async fn add_data_chunk_impl(&mut self, data: Bytes) -> Result<()> {
        // If the file size was not specified at the beginning, then incrementally update tho total size with
        // how much data we know about.
        self.session
            .completion_tracker
            .increment_file_size(self.file_id, data.len() as u64);

        // Put the chunking on a compute thread so it doesn't tie up the async schedulers
        let chunk_data_jh = {
            let mut chunker = std::mem::take(&mut self.chunker);
            let data = data.clone();
            let runtime = self.ctx.runtime.clone();

            runtime.spawn_blocking(move || {
                let chunks: Arc<[Chunk]> = Arc::from(chunker.next_block_bytes(&data, false));
                (chunks, chunker)
            })
        };

        // Update the sha256 hasher, which hands this off to be done in the background.
        if let Some(ref mut generator) = self.sha_generator {
            generator.update(data.clone()).await?;
        }

        // Get the chunk data and start processing it.
        let (chunks, chunker) = chunk_data_jh.await?;

        // Restore the chunker state.
        self.chunker = chunker;

        // It's possible this didn't actually add any data in.
        if chunks.is_empty() {
            return Ok(());
        }

        // Run the deduplication interface here.
        self.deduper_process_chunks(chunks).await?;

        Ok(())
    }

    /// Ensures all current background work is completed.
    pub async fn checkpoint(&mut self) -> Result<()> {
        // Flush the background process by sending it a dummy bit of data.
        self.deduper_process_chunks(Arc::new([])).await
    }

    /// Return the representation of the file after clean as a pointer file instance.
    #[instrument(skip_all, name = "FileCleaner::finish", fields(file_name=self.file_name.as_ref().map(|s|s.to_string())))]
    pub async fn finish(mut self) -> Result<(XetFileInfo, DeduplicationMetrics)> {
        // Chunk the rest of the data.
        if let Some(chunk) = self.chunker.finish() {
            let data = Arc::new([chunk]);
            self.deduper_process_chunks(data).await?;
        }

        // Resolve the SHA-256: computed, provided, or skipped.
        let sha256 = if let Some(generator) = self.sha_generator.take() {
            Some(generator.finalize().await?)
        } else {
            self.provided_sha256
        };
        let metadata_ext = sha256.map(FileMetadataExt::new);

        let (file_hash, remaining_file_data, deduplication_metrics) =
            self.dedup_manager_fut.await?.finalize(metadata_ext);

        let file_info = XetFileInfo {
            hash: file_hash.hex(),
            file_size: Some(deduplication_metrics.total_bytes),
            sha256: sha256.map(|s| s.hex()),
        };

        // Let's check some things that should be invariants
        #[cfg(debug_assertions)]
        {
            // There should be exactly one file referenced in the remaining file data.
            debug_assert_eq!(remaining_file_data.pending_file_info.len(), 1);

            // The size should be total bytes
            debug_assert_eq!(remaining_file_data.pending_file_info[0].0.file_size(), deduplication_metrics.total_bytes)
        }

        // Now, return all this information to the
        self.session
            .register_single_file_clean_completion(remaining_file_data, &deduplication_metrics)
            .await?;

        // NB: xorb upload is happening in the background, this number is optimistic since it does
        // not count transfer time of the uploaded xorbs, which is why `end_processing_ts`

        info!(
            target: "client_telemetry",
            action = "clean",
            file_name = self.file_name.unwrap_or_default().to_string(),
            file_size_count = deduplication_metrics.total_bytes,
            new_bytes_count = deduplication_metrics.new_bytes,
            start_ts = self.start_time.to_rfc3339(),
            end_processing_ts = Utc::now().to_rfc3339(),
        );

        Ok((file_info, deduplication_metrics))
    }
}
