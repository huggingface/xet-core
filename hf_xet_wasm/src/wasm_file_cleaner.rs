use std::sync::Arc;

use deduplication::{Chunk, Chunker, DeduplicationMetrics, FileDeduper};
use mdb_shard::file_structs::FileMetadataExt;
use merklehash::MerkleHash;

use super::errors::*;
use super::sha256::ShaGenerator;
use super::wasm_deduplication_interface::UploadSessionDataManager;
use super::wasm_file_upload_session::FileUploadSession;

/// A class that encapsulates the clean and data task around a single file for wasm runtime.
pub struct SingleFileCleaner {
    _tracker: String,

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
    pub fn new(session: Arc<FileUploadSession>, _tracker: String) -> Self {
        Self {
            _tracker,
            session: session.clone(),
            chunker: Chunker::default(),
            dedup_manager: FileDeduper::new(UploadSessionDataManager::new(session)),
            sha_generator: ShaGenerator::new(),
        }
    }

    pub async fn add_data(&mut self, data: &[u8]) -> Result<()> {
        // Chunk the data.
        let chunks: Arc<[Chunk]> = Arc::from(self.chunker.next_block(data, false));
        log::info!("chunked into {} chunks", chunks.len());

        // It's possible this didn't actually add any data in.
        if chunks.is_empty() {
            return Ok(());
        }

        // Update the sha256 generator
        self.sha_generator.update(chunks.clone()).await;

        // Run the deduplication interface here.
        let dedup_metrics = self.dedup_manager.process_chunks(&chunks).await?;

        log::info!("{}/{} chunks deduped", dedup_metrics.deduped_chunks, dedup_metrics.total_chunks);

        Ok(())
    }

    /// Return the representation of the file after clean as a pointer file instance.
    pub async fn finish(mut self) -> Result<(MerkleHash, DeduplicationMetrics)> {
        // Chunk the rest of the data.
        if let Some(chunk) = self.chunker.finish() {
            self.sha_generator.update(Arc::new([chunk.clone()])).await;
            self.dedup_manager.process_chunks(&[chunk]).await?;
        }

        // Finalize the sha256 hashing and create the metadata extension
        let sha256: MerkleHash = self.sha_generator.finalize().await?;
        let metadata_ext = FileMetadataExt::new(sha256);

        // Now finish the deduplication process.
        let repo_salt = self.session.config.shard_config.repo_salt;
        let (file_hash, remaining_file_data, deduplication_metrics, new_xorbs) =
            self.dedup_manager.finalize(repo_salt, Some(metadata_ext));

        // Let's check some things that should be invarients
        #[cfg(debug_assertions)]
        {
            // There should be exactly one file referenced in the remaining file data.
            debug_assert_eq!(remaining_file_data.pending_file_info.len(), 1);

            // The size should be total bytes
            debug_assert_eq!(
                remaining_file_data.pending_file_info[0].0.file_size(),
                deduplication_metrics.total_bytes as usize
            )
        }

        // Now, return all this information to the
        self.session
            .register_single_file_clean_completion(remaining_file_data, &deduplication_metrics, new_xorbs)
            .await?;

        Ok((file_hash, deduplication_metrics))
    }
}
