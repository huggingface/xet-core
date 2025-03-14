use std::collections::{HashMap, VecDeque};
use std::mem::take;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::SystemTime;

use cas_object::range_hash_from_chunks;
use chrono::{DateTime, Utc};
use chunking::Chunk;
use deduplication::{DataInterface, DeduplicationMetrics, FileDeduper};
use lazy_static::lazy_static;
use mdb_shard::file_structs::{
    FileDataSequenceEntry, FileDataSequenceHeader, FileMetadataExt, FileVerificationEntry, MDBFileInfo,
};
use mdb_shard::{hash_is_global_dedup_eligible, ShardFileManager};
use merkledb::aggregate_hashes::file_node_hash;
use merkledb::constants::TARGET_CAS_BLOCK_SIZE;
use merklehash::MerkleHash;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{debug, info};
use utils::progress::ProgressUpdater;
use xet_threadpool::ThreadPool;

use crate::constants::{
    DEFAULT_MIN_N_CHUNKS_PER_RANGE, MIN_N_CHUNKS_PER_RANGE_HYSTERESIS_FACTOR, MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES,
    NRANGES_IN_STREAMING_FRAGMENTATION_ESTIMATOR,
};
use crate::data_interface::{self, UploadSessionDataManager};
use crate::errors::DataProcessingError::*;
use crate::errors::Result;
use crate::file_upload_session::FileUploadSessionState;
use crate::metrics::FILTER_BYTES_CLEANED;
use crate::parallel_xorb_uploader::XorbUpload;
use crate::remote_shard_interface::RemoteShardInterface;
use crate::repo_salt::RepoSalt;
use crate::sha256::ShaGenerator;
use crate::PointerFile;

/// A class that encapsulates the clean and data task around a single file.
pub struct SingleFileCleaner {
    // Auxiliary info
    file_name: Option<PathBuf>,

    // Common state
    state: Arc<FileUploadSessionState>,

    // The chunker
    chunker: chunking::Chunker,

    // The deduplication interface.
    dedup_manager: FileDeduper<UploadSessionDataManager>,

    // Internal Data
    metrics: DeduplicationMetrics,

    // Generating the sha256 hash
    sha_generator: ShaGenerator,
}

impl SingleFileCleaner {
    pub(crate) fn new(file_name: Option<&Path>, state: Arc<FileUploadSessionState>) -> Self {
        Self {
            file_name,
            state: state.clone(),
            chunker: deduplication::Chunker::default(),
            dedup_manager: FileDeduper::new(UploadSessionDataManager::new(state)),
            metrics: DeduplicationMetrics::default(),
            sha_generator: ShaGenerator::new(),
        }
    }

    pub async fn add_data(&mut self, data: Vec<u8>) -> Result<()> {
        // Chunk the data.
        let chunks = self.chunker.next_block(&data[..], false);

        // Done with the original data; drop it to free memory pressure.
        drop(data);

        // It's possible this didn't actually add any data in.
        if chunks.empty() {
            return Ok(());
        }

        // Update the sha256 generator
        self.sha_generator.update(chunks.clone())?;

        // Run the deduplication interface here.
        let metrics = self.dedup_manager.process_chunks(&chunks).await?;
        self.metrics.merge_in(&metrics);

        Ok(())
    }

    /// Return the representation of file after clean and the number of new bytes after dedup
    pub async fn finish(mut self) -> Result<(String, DataAggregator, DeduplicationMetrics)> {
        // Chunk the rest of the data.
        if let Some(chunk) = self.chunker.finish() {
            let metrics = self.dedup_manager.process_chunks(&[chunk]).await?;
            self.metrics.merge_in(&metrics);
        }

        // Finalize the sha256 hashing
        let sha256: MerkleHash = self.sha_generator.finalize().await;

        // Prepare the metadata extension
        let metadata_ext = FileMetadataExt::new(sha256);

        // Now finish the deduplication process.
        let repo_salt = self.state.config.dedup_config.repo_salt;
        let (remaining_file_data, deduplication_metrics, new_xorbs) =
            self.dedup_manager.finalize(repo_salt, Some(metadata_ext)).await?;

        self.session.register

        let file_size = self.metrics.file_size.load(Ordering::Relaxed);
        let new_bytes = self.metrics.new_bytes_after_dedup.load(Ordering::Relaxed);

        let return_file = self.finalize_data_processing().await?;

        let current_time = SystemTime::now();
        let start: DateTime<Utc> = self.metrics.start_time.into();
        let now: DateTime<Utc> = current_time.into();

        // NB: xorb upload is happening in the background, this number is optimistic since it does
        // not count transfer time of the uploaded xorbs, which is why `end_processing_ts`
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

        Ok((return_file.to_string(), new_bytes))
    }
}