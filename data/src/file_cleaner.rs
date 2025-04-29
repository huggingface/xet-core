use std::cell::Cell;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use deduplication::{Chunk, Chunker, DeduplicationMetrics, FileDeduper};
use mdb_shard::file_structs::FileMetadataExt;
use merklehash::MerkleHash;
use tokio::task::{JoinError, JoinHandle};
use tracing::info;

use crate::constants::INGESTION_BLOCK_SIZE;
use crate::deduplication_interface::UploadSessionDataManager;
use crate::errors::Result;
use crate::file_upload_session::FileUploadSession;
use crate::sha256::ShaGenerator;
use crate::XetFileInfo;

// A little set of helper types to allow us to background the
// dedupe_manager::process_chunks operation.
// The design is that there is only 1 instance of the FileDeduper in a Cell
// so there can only be a single reference to it.
//
// SingleFileCleaner::dedup_manager initializes as Foreground(Some(deduper))
//
// - get_deduper() will return the Cell and empty out dedup_manager
// (whatever state it current is in) turning it into Foreground(None).
// If dedup_manager is already in Background, it will wait for the JoinHandle to finish
// and return the Cell (once again resetting dedup_manager to Foreground(None))
//
// - deduper_process_chunks() will start processing of new chunks in background
// and switch dedup_manager into Background(JoinHandle)
type DedupeBoxType = Cell<FileDeduper<UploadSessionDataManager>>;
type ProcessChunksResult = Result<DeduplicationMetrics>;
enum DedupManagerBackgrounder {
    Foreground(Option<DedupeBoxType>),
    Background(JoinHandle<std::result::Result<(DedupeBoxType, ProcessChunksResult), JoinError>>),
}

/// A class that encapsulates the clean and data task around a single file.
pub struct SingleFileCleaner {
    // Auxiliary info
    file_name: Option<String>,

    // Common state
    session: Arc<FileUploadSession>,

    // The chunker
    chunker: Chunker,

    // The deduplication interface.
    dedup_manager: DedupManagerBackgrounder,

    // Generating the sha256 hash
    sha_generator: ShaGenerator,

    // Start time
    start_time: DateTime<Utc>,
}

impl SingleFileCleaner {
    pub(crate) fn new(file_name: Option<String>, session: Arc<FileUploadSession>) -> Self {
        let deduper = Cell::new(FileDeduper::new(UploadSessionDataManager::new(session.clone())));
        Self {
            file_name,
            dedup_manager: DedupManagerBackgrounder::Foreground(Some(deduper)),
            session,
            chunker: deduplication::Chunker::default(),
            sha_generator: ShaGenerator::new(),
            start_time: Utc::now(),
        }
    }

    /// Returns the dedupe manager, resetting the state to Foreground(None),
    /// waiting for background operations to complete.
    async fn get_deduper(&mut self) -> Result<DedupeBoxType> {
        match self.dedup_manager {
            DedupManagerBackgrounder::Foreground(ref mut deduper) => {
                let deduper = deduper.take();
                match deduper {
                    Some(deduper) => Ok(deduper),
                    None => {
                        // This should be impossible
                        panic!("Deduper lost");
                    },
                }
            },
            DedupManagerBackgrounder::Background(ref mut jh) => {
                // note that the join handle has the *only* reference to the
                // deduper. So yes, there are some conditions (Tokio join failure)
                // in which we will lose the deduper completely.
                // But those conditions are unlikely to be recoverable anyway..?
                let (deduper, block_metrics) = jh.await??;
                match block_metrics {
                    Ok(block_metrics) => {
                        // This is the normal case, we are done.
                        self.dedup_manager = DedupManagerBackgrounder::Foreground(None);
                        // Update the progress bar with the deduped bytes
                        if let Some(updater) = self.session.upload_progress_updater.as_ref() {
                            updater.update(block_metrics.deduped_bytes as u64);
                        }
                    },
                    Err(e) => {
                        // This is an error case, we need to return the error
                        // but we also need to make sure that the deduper is
                        // in a state where it can be used again.
                        self.dedup_manager = DedupManagerBackgrounder::Foreground(Some(deduper));
                        return Err(e);
                    },
                }

                self.dedup_manager = DedupManagerBackgrounder::Foreground(None);
                Ok(deduper)
            },
        }
    }

    /// Gets the dedupe manager to process new chunks, by first
    /// waiting for background operations to complete, then triggering a
    /// new background task.
    async fn deduper_process_chunks(&mut self, chunks: Arc<[Chunk]>) -> Result<()> {
        let mut deduper = self.get_deduper().await?;
        self.dedup_manager = DedupManagerBackgrounder::Background(tokio::spawn(async move {
            let res = deduper.get_mut().process_chunks(&chunks).await;
            Ok((deduper, res))
        }));
        Ok(())
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
        self.deduper_process_chunks(chunks).await?;

        Ok(())
    }

    /// Return the representation of the file after clean as a pointer file instance.
    pub async fn finish(mut self) -> Result<(XetFileInfo, DeduplicationMetrics)> {
        // Chunk the rest of the data.
        // note that get_deduper returns the only reference to the deduper
        let mut deduper = self.get_deduper().await?;
        if let Some(chunk) = self.chunker.finish() {
            self.sha_generator.update(Arc::new([chunk.clone()])).await?;
            let block_metrics = deduper.get_mut().process_chunks(&[chunk]).await?;
            if let Some(updater) = self.session.upload_progress_updater.as_ref() {
                updater.update(block_metrics.deduped_bytes as u64);
            }
        }

        // Finalize the sha256 hashing and create the metadata extension
        let sha256: MerkleHash = self.sha_generator.finalize().await?;
        let metadata_ext = FileMetadataExt::new(sha256);

        // Now finish the deduplication process.
        let repo_salt = self.session.config.shard_config.repo_salt;
        let (file_hash, remaining_file_data, deduplication_metrics, new_xorbs) =
            deduper.into_inner().finalize(repo_salt, Some(metadata_ext));

        let file_info = XetFileInfo::new(file_hash.hex(), deduplication_metrics.total_bytes);

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

        Ok((file_info, deduplication_metrics))
    }
}
