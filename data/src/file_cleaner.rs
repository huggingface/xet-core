use std::sync::Arc;

use chrono::{DateTime, Utc};
use deduplication::{Chunk, Chunker, DeduplicationMetrics, FileDeduper};
use mdb_shard::file_structs::FileMetadataExt;
use merklehash::MerkleHash;
use tracing::info;

use crate::constants::INGESTION_BLOCK_SIZE;
use crate::deduplication_interface::UploadSessionDataManager;
use crate::errors::Result;
use crate::file_upload_session::FileUploadSession;
use crate::sha256::ShaGenerator;
use crate::PointerFile;

/// A class that encapsulates the clean and data task around a single file.
pub struct SingleFileCleaner {
    // Auxiliary info
    file_name: String,

    // Common state
    session: Arc<FileUploadSession>,

    // The chunker
    chunker: Chunker,

    // The deduplication interface.
    dedup_manager: FileDeduper<UploadSessionDataManager>,

    // Generating the sha256 hash
    sha_generator: ShaGenerator,

    // Start time
    start_time: DateTime<Utc>,
}

impl SingleFileCleaner {
    pub(crate) fn new(file_name: String, session: Arc<FileUploadSession>) -> Self {
        Self {
            file_name,
            dedup_manager: FileDeduper::new(UploadSessionDataManager::new(session.clone())),
            session,
            chunker: deduplication::Chunker::default(),
            sha_generator: ShaGenerator::new(),
            start_time: Utc::now(),
        }
    }

    pub async fn add_data(&mut self, data: &[u8]) -> Result<()> {
        if data.len() > *INGESTION_BLOCK_SIZE {
            let mut pos = 0;
            while pos < data.len() {
                let next_pos = usize::min(pos + *INGESTION_BLOCK_SIZE, data.len());
                self.add_data_impl(&data[pos..next_pos]).await?;
                pos = next_pos;
            }
        } else {
            self.add_data_impl(data).await?;
        }

        Ok(())
    }

    async fn add_data_impl(&mut self, data: &[u8]) -> Result<()> {
        // Chunk the data.
        let chunks: Arc<[Chunk]> = Arc::from(self.chunker.next_block(data, false));

        // It's possible this didn't actually add any data in.
        if chunks.is_empty() {
            return Ok(());
        }

        // Update the sha256 generator
        self.sha_generator.update(chunks.clone()).await?;

        // Run the deduplication interface here.
        let block_metrics = self.dedup_manager.process_chunks(&chunks).await?;

        // Update the progress bar with the deduped bytes
        if let Some(updater) = self.session.upload_progress_updater.as_ref() {
            updater.update(block_metrics.deduped_bytes as u64);
        }

        Ok(())
    }

    /// Return the representation of the file after clean as a pointer file instance.
    pub async fn finish(mut self) -> Result<(PointerFile, DeduplicationMetrics)> {
        // Chunk the rest of the data.
        if let Some(chunk) = self.chunker.finish() {
            self.sha_generator.update(Arc::new([chunk.clone()])).await?;
            self.dedup_manager.process_chunks(&[chunk]).await?;
        }

        // Finalize the sha256 hashing and create the metadata extension
        let sha256: MerkleHash = self.sha_generator.finalize().await?;
        let metadata_ext = FileMetadataExt::new(sha256);

        // Now finish the deduplication process.
        let repo_salt = self.session.config.shard_config.repo_salt;
        let (file_hash, remaining_file_data, deduplication_metrics, new_xorbs) =
            self.dedup_manager.finalize(repo_salt, Some(metadata_ext));

        let pointer_file =
            PointerFile::init_from_info(&self.file_name, &file_hash.hex(), deduplication_metrics.total_bytes as u64);

        // Let's check some things that should be invarients
        #[cfg(debug_assertions)]
        {
            // There should be exactly one file referenced in the remaining file data.
            debug_assert_eq!(remaining_file_data.pending_file_info.len(), 1);

            // The size should be total bytes
            debug_assert_eq!(remaining_file_data.pending_file_info[0].0.file_size(), pointer_file.filesize() as usize)
        }

        // Now, return all this information to the
        self.session
            .register_single_file_clean_completion(
                self.file_name.clone(),
                remaining_file_data,
                &deduplication_metrics,
                new_xorbs,
            )
            .await?;

        // NB: xorb upload is happening in the background, this number is optimistic since it does
        // not count transfer time of the uploaded xorbs, which is why `end_processing_ts`

        info!(
            target: "client_telemetry",
            action = "clean",
            file_name = &self.file_name,
            file_size_count = deduplication_metrics.total_bytes,
            new_bytes_count = deduplication_metrics.new_bytes,
            start_ts = self.start_time.to_rfc3339(),
            end_processing_ts = Utc::now().to_rfc3339(),
        );

        Ok((pointer_file, deduplication_metrics))
    }
}
