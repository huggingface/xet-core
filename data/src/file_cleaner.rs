use std::future::{self, Future};
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use deduplication::{Chunk, Chunker, DeduplicationMetrics, FileDeduper};
use mdb_shard::file_structs::FileMetadataExt;
use merklehash::MerkleHash;
use progress_tracking::upload_tracking::CompletionTrackerFileId;
use tracing::{debug_span, info, instrument, Instrument};

use crate::constants::INGESTION_BLOCK_SIZE;
use crate::deduplication_interface::UploadSessionDataManager;
use crate::errors::Result;
use crate::file_upload_session::FileUploadSession;
use crate::sha256::ShaGenerator;
use crate::XetFileInfo;

/// A class that encapsulates the clean and data task around a single file.
pub struct SingleFileCleaner {
    // The id for completion tracking
    file_id: CompletionTrackerFileId,

    // File name, if known.
    file_name: Option<Arc<str>>,

    // Common state.
    session: Arc<FileUploadSession>,

    // The chunker.
    chunker: Chunker,

    // The deduplication interface.  Use a future that always returns the dedup manager
    // on await so that we can background this part.
    dedup_manager_fut: Pin<Box<dyn Future<Output = Result<FileDeduper<UploadSessionDataManager>>> + Send + 'static>>,

    // Generating the sha256 hash
    sha_generator: ShaGenerator,

    // Start time
    start_time: DateTime<Utc>,
}

impl SingleFileCleaner {
    pub(crate) fn new(
        file_name: Option<Arc<str>>,
        file_id: CompletionTrackerFileId,
        session: Arc<FileUploadSession>,
    ) -> Self {
        let deduper = FileDeduper::new(UploadSessionDataManager::new(session.clone(), file_id), file_id);

        Self {
            file_name,
            file_id,
            dedup_manager_fut: Box::pin(async move { Ok(deduper) }),
            session,
            chunker: deduplication::Chunker::default(),
            sha_generator: ShaGenerator::new(),
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
        if data.len() > *INGESTION_BLOCK_SIZE {
            let mut pos = 0;
            while pos < data.len() {
                let next_pos = usize::min(pos + *INGESTION_BLOCK_SIZE, data.len());
                self.add_data_impl(Bytes::copy_from_slice(&data[pos..next_pos])).await?;
                pos = next_pos;
            }
        } else {
            self.add_data_impl(Bytes::copy_from_slice(data)).await?;
        }

        Ok(())
    }

    #[instrument(skip_all, level="debug", name = "FileCleaner::add_data", fields(file_name=self.file_name.as_ref().map(|s|s.to_string()), len=data.len()))]
    pub(crate) async fn add_data_impl(&mut self, data: Bytes) -> Result<()> {
        // Put the chunking on a compute thread so it doesn't tie up the async schedulers
        let chunk_data_jh = {
            let mut chunker = std::mem::take(&mut self.chunker);
            let data = data.clone();

            tokio::task::spawn_blocking(move || {
                let chunks: Arc<[Chunk]> = Arc::from(chunker.next_block_bytes(&data, false));
                (chunks, chunker)
            })
        };

        // Update the sha256 hasher, which hands this off to be done in the background.
        self.sha_generator.update(data.clone()).await?;

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

        // Finalize the sha256 hashing and create the metadata extension
        let sha256: MerkleHash = self.sha_generator.finalize().await?;
        let metadata_ext = FileMetadataExt::new(sha256);

        // Now finish the deduplication process.
        let repo_salt = self.session.config.shard_config.repo_salt;

        let (file_hash, remaining_file_data, deduplication_metrics) =
            self.dedup_manager_fut.await?.finalize(repo_salt, Some(metadata_ext));

        let file_info = XetFileInfo::new(file_hash.hex(), deduplication_metrics.total_bytes);

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
