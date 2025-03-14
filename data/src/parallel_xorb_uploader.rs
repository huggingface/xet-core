use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use cas_client::Client;
use mdb_shard::ShardFileManager;
use merklehash::MerkleHash;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use utils::progress::ProgressUpdater;
use xet_threadpool::ThreadPool;

use crate::errors::DataProcessingError::*;
use crate::errors::*;

lazy_static::lazy_static! {
    pub static ref XORB_UPLOAD_RATE_LIMITER: Arc<Semaphore> = Arc::new(Semaphore::new(*MAX_CONCURRENT_XORB_UPLOADS));
}

/// Helper to parallelize xorb upload and registration.
/// Calls to registering xorbs return immediately after computing a xorb hash so callers
/// can continue with other work, and xorb data is queued internally to be uploaded and registered.
///
/// It is critical to call [`flush`] before `ParallelXorbUploader` is dropped. Dropping will
/// cancel all ongoing transfers automatically.
pub(crate) struct ParallelXorbUploader {
    // Configurations
    cas_prefix: String,

    // Utils
    client: Arc<dyn Client + Send + Sync>,

    // Internal worker
    upload_tasks: Mutex<JoinSet<Result<usize>>>,

    // Rate limiter
    rate_limiter: Arc<Semaphore>,

    // Theadpool
    threadpool: Arc<ThreadPool>,

    // Upload Progress
    upload_progress_updater: Option<Arc<dyn ProgressUpdater>>,

    // Metrics
    total_bytes_trans: AtomicU64,
}

impl ParallelXorbUploader {
    pub fn new(
        cas_prefix: String,
        client: Arc<dyn Client + Send + Sync>,
        threadpool: Arc<ThreadPool>,
        upload_progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Arc<Self> {
        Arc::new(ParallelXorbUploader {
            cas_prefix: cas_prefix.to_owned(),
            client,
            upload_tasks: Mutex::new(JoinSet::new()),
            rate_limiter: XORB_UPLOAD_RATE_LIMITER.clone(),
            threadpool,
            upload_progress_updater,
            total_bytes_trans: 0.into(),
        })
    }

    async fn status_is_ok(&self) -> Result<()> {
        let mut upload_tasks = self.upload_tasks.lock().await;
        while let Some(result) = upload_tasks.try_join_next() {
            self.total_bytes_trans.fetch_add(result?? as u64, Ordering::Relaxed);
        }

        Ok(())
    }
}

impl ParallelXorbUploader {
    pub async fn register_new_xorb_for_upload(&self, xorb: RawXorbData) -> Result<()> {
        self.status_is_ok().await?;

        // Acquire a permit for uploading; the acquired permit is dropped after the task completes.
        // The chosen Semaphore is fair, meaning xorbs added first will be scheduled to upload first.
        let permit = self
            .rate_limiter
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| UploadTaskError(e.to_string()))?;

        let client = self.cas.clone();
        let cas_prefix = self.cas_prefix.clone();
        let upload_progress_updater = self.upload_progress_updater.clone();

        self.upload_tasks.lock().await.spawn_on(
            async move {
                let (cas_info, data, file_info) = cas_data.finalize();
                let cas_hash = cas_info.metadata.cas_hash;

                shard_manager.add_cas_block(cas_info).await?;

                if let Some(updater) = upload_progress_updater {
                    updater.update(n_bytes_transmitted as u64);
                }
                Ok(n_bytes_transmitted)
            },
            &self.threadpool.handle(),
        );
    }

    /// Flush makes sure all xorbs added to queue before this call are sent successfully
    /// to remote. This function can be called multiple times and should be called at
    /// least once before `ParallelXorbUploader` is dropped.
    pub async fn finalize(&self) -> Result<u64> {
        let mut upload_tasks = self.upload_tasks.lock().await;

        while let Some(result) = upload_tasks.join_next().await {
            self.total_bytes_trans.fetch_add(result?? as u64, Ordering::Relaxed);
        }

        Ok(self.total_bytes_trans.load(Ordering::Relaxed))
    }
}
