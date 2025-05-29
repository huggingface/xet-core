use std::sync::Arc;

use deduplication::{Chunk, Chunker, DeduplicationMetrics, FileDeduper};
use mdb_shard::file_structs::FileMetadataExt;
use merklehash::MerkleHash;
use tokio::sync::mpsc;
use tokio_with_wasm::alias as wasmtokio;

use super::errors::*;
use super::wasm_deduplication_interface::UploadSessionDataManager;
use super::wasm_file_upload_session::FileUploadSession;
use crate::sha256::ShaGeneration;
use crate::wasm_timer::Timer;

enum CPUTask {
    CurrentThread((Chunker, ShaGeneration)),
    WorkerThread(
        (
            wasmtokio::task::JoinHandle<Result<MerkleHash>>,
            mpsc::UnboundedSender<Vec<u8>>,
            mpsc::UnboundedReceiver<Vec<Chunk>>,
            u64,
        ),
    ),
}

/// A class that encapsulates the clean and data task around a single file for wasm runtime.
pub struct SingleFileCleaner {
    _tracker: String,

    // Common state
    session: Arc<FileUploadSession>,

    // The CPU intensive task
    cpu_task: CPUTask,

    // The deduplication interface.
    dedup_manager: FileDeduper<UploadSessionDataManager>,
}

impl SingleFileCleaner {
    pub fn new(
        session: Arc<FileUploadSession>,
        _tracker: String,
        sha256: Option<MerkleHash>,
        single_threaded: bool,
    ) -> Self {
        if single_threaded {
            Self::new_with_cpu_task_in_current_thread(session, _tracker, sha256)
        } else {
            Self::new_with_cpu_task_in_worker_thread(session, _tracker, sha256)
        }
    }

    fn new_with_cpu_task_in_current_thread(
        session: Arc<FileUploadSession>,
        _tracker: String,
        sha256: Option<MerkleHash>,
    ) -> Self {
        Self {
            _tracker,
            session: session.clone(),
            cpu_task: CPUTask::CurrentThread((Chunker::default(), ShaGeneration::new(sha256))),
            dedup_manager: FileDeduper::new(UploadSessionDataManager::new(session)),
        }
    }

    fn new_with_cpu_task_in_worker_thread(
        session: Arc<FileUploadSession>,
        _tracker: String,
        sha256: Option<MerkleHash>,
    ) -> Self {
        let (input_tx, mut input_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (chunks_tx, chunks_rx) = mpsc::unbounded_channel::<Vec<Chunk>>();

        let cpu_worker = wasmtokio::task::spawn_blocking(move || {
            futures::executor::block_on(async move {
                let mut chunker = Chunker::default();
                let mut sha_generation = ShaGeneration::new(sha256);
                while let Some(input) = input_rx.recv().await {
                    let chunks = chunker.next_block(&input, false);
                    chunks_tx.send(chunks).map_err(DataProcessingError::internal)?;

                    sha_generation.update_with_bytes(&input);
                }

                if let Some(chunk) = chunker.finish() {
                    sha_generation.update_with_bytes(&chunk.data);
                    chunks_tx.send(vec![chunk]).map_err(DataProcessingError::internal)?;
                }

                sha_generation.finalize()
            })
        });

        let cpu_task = CPUTask::WorkerThread((cpu_worker, input_tx, chunks_rx, 0));

        Self {
            _tracker,
            session: session.clone(),
            cpu_task,
            dedup_manager: FileDeduper::new(UploadSessionDataManager::new(session)),
        }
    }

    pub async fn add_data(&mut self, data: &[u8]) -> Result<()> {
        let chunks = match self.cpu_task {
            CPUTask::CurrentThread((ref mut chunker, ref mut sha_generation)) => {
                Self::add_data_to_cpu_task_in_current_thread(chunker, sha_generation, data).await
            },
            CPUTask::WorkerThread(_) => todo!(),
        }?;

        // Run the deduplication interface here.
        let msg = format!("dedupping {} chunks", chunks.len());
        let _timer = Timer::new(&msg);
        let dedup_metrics = self.dedup_manager.process_chunks(&chunks).await?;
        drop(_timer);

        log::debug!("{}/{} chunks deduped", dedup_metrics.deduped_chunks, dedup_metrics.total_chunks);

        Ok(())
    }

    async fn add_data_to_cpu_task_in_current_thread(
        chunker: &mut Chunker,
        sha_generation: &mut ShaGeneration,
        data: &[u8],
    ) -> Result<Vec<Chunk>> {
        // Chunk the data.
        let _timer = Timer::new(format!("chunking {} bytes", data.len()));
        let chunks = chunker.next_block(data, false);
        drop(_timer);
        log::debug!("chunked into {} chunks", chunks.len());

        // It's possible this didn't actually add any data in.
        if chunks.is_empty() {
            return Ok(chunks);
        }

        // Update the sha256 generator
        let _timer = Timer::new(format!("computing sha256 over {} chunks", chunks.len()));
        sha_generation.update(&chunks);
        drop(_timer);

        Ok(chunks)
    }

    async fn add_data_to_cpu_task_in_worker_thread() -> Result<Vec<Chunk>> {
        todo!()
    }

    /// Return the representation of the file after clean as a pointer file instance.
    pub async fn finish(mut self) -> Result<(MerkleHash, MerkleHash, DeduplicationMetrics)> {
        let (maybe_last_chunk, sha256) = match self.cpu_task {
            CPUTask::CurrentThread((chunker, sha_generation)) => {
                Self::finish_cpu_task_in_current_thread(chunker, sha_generation).await
            },
            CPUTask::WorkerThread(_) => todo!(),
        }?;
        if let Some(chunk) = maybe_last_chunk {
            self.dedup_manager.process_chunks(&[chunk]).await?;
        }

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

        Ok((file_hash, sha256, deduplication_metrics))
    }

    async fn finish_cpu_task_in_current_thread(
        chunker: Chunker,
        mut sha_generation: ShaGeneration,
    ) -> Result<(Option<Chunk>, MerkleHash)> {
        // Chunk the rest of the data.
        if let Some(chunk) = chunker.finish() {
            sha_generation.update(&[chunk.clone()]);

            // Finalize the sha256 hashing
            let sha256: MerkleHash = sha_generation.finalize()?;

            Ok((Some(chunk), sha256))
        } else {
            // Finalize the sha256 hashing
            let sha256: MerkleHash = sha_generation.finalize()?;

            Ok((None, sha256))
        }
    }

    async fn finish_cpu_task_in_worker_thread() -> Result<(Option<Chunk>, MerkleHash)> {
        todo!()
    }
}
