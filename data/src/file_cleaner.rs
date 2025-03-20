use std::sync::Arc;

use deduplication::{Chunk, ChunkPermitType, Chunker, DeduplicationMetrics, FileDeduper};
use mdb_shard::file_structs::FileMetadataExt;
use merkledb::constants::MAXIMUM_CHUNK_SIZE;
use merklehash::MerkleHash;

use crate::constants::DATA_INGESTION_BUFFER_SIZE;
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
}

impl SingleFileCleaner {
    pub(crate) fn new(file_name: String, session: Arc<FileUploadSession>) -> Self {
        Self {
            file_name,
            dedup_manager: FileDeduper::new(UploadSessionDataManager::new(session.clone())),
            session,
            chunker: deduplication::Chunker::default(),
            sha_generator: ShaGenerator::new(),
        }
    }

    pub async fn add_data(&mut self, data: &[u8]) -> Result<()> {
        // Handle the case where this is called with a huge amount of data,

        if data.len() > *DATA_INGESTION_BUFFER_SIZE {
            let mut pos = 0;
            while pos < data.len() {
                let next_pos = usize::min(pos + *DATA_INGESTION_BUFFER_SIZE, data.len());
                self.add_data_impl(&data[pos..next_pos]).await?;
                pos = next_pos;
            }
        } else {
            self.add_data_impl(data).await?;
        }
        Ok(())
    }

    async fn add_data_impl(&mut self, data: &[u8]) -> Result<()> {
        // Acquire the chunk memory permit; need a little extra to account for possible previous
        // chunk data added to this one.  This variable is captured by the acquisition function
        // and thus any remaining permits are released right after the chunking is finished.
        let mut chunk_block_permit = self
            .session
            .chunk_memory_limiter
            .clone()
            .acquire_many_owned((data.len() + MAXIMUM_CHUNK_SIZE) as u32)
            .await?;

        // Chunk the data.
        let chunks: Arc<[Chunk]> = Arc::from(self.chunker.next_block_with_permit(data, false, |n_bytes| {
            chunk_block_permit.split(n_bytes).map(|c| Arc::new(c) as ChunkPermitType)
        }));

        // It's possible this didn't actually add any data in.
        if chunks.is_empty() {
            return Ok(());
        }

        // Update the sha256 generator
        self.sha_generator.update(chunks.clone());

        // Run the deduplication interface here.
        self.dedup_manager.process_chunks(&chunks).await?;

        Ok(())
    }

    /// Return the representation of the file after clean as a pointer file instance.
    pub async fn finish(mut self) -> Result<(PointerFile, DeduplicationMetrics)> {
        // Chunk the rest of the data.
        if let Some(chunk) = self.chunker.finish() {
            self.sha_generator.update(Arc::new([chunk.clone()]));
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
                self.file_name,
                remaining_file_data,
                &deduplication_metrics,
                new_xorbs,
            )
            .await?;

        // NB: xorb upload is happening in the background, this number is optimistic since it does
        // not count transfer time of the uploaded xorbs, which is why `end_processing_ts`

        /* TODO: bring this back.
        info!(
            target: "client_telemetry",
            action = "clean",
            repo_id = ?self.metrics.repo_id,
            file_name = ?self.file_name,
            file_size_count = file_size,
            new_bytes_count = new_bytes,
            start_ts = start.to_rfc3339(),
            end_processing_ts = now.to_rfc3339(),
        );
        */

        Ok((pointer_file, deduplication_metrics))
    }
}
