use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use cas_client::Client;
use deduplication::RawXorbData;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use utils::progress::ProgressUpdater;
use xet_threadpool::ThreadPool;

use crate::constants::MAX_CONCURRENT_XORB_UPLOADS;
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
    parallel_upload_limiter: Arc<Semaphore>,

    // Theadpool
    threadpool: Arc<ThreadPool>,

    // Upload Progress
    upload_progress_updater: Option<Arc<dyn ProgressUpdater>>,

    // Metrics
    total_bytes_trans: AtomicUsize,
}

impl ParallelXorbUploader {
    pub fn new(
        cas_prefix: String,
        client: Arc<dyn Client + Send + Sync>,
        threadpool: Arc<ThreadPool>,
        upload_progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Self {
        ParallelXorbUploader {
            cas_prefix: cas_prefix.to_owned(),
            client,
            upload_tasks: Mutex::new(JoinSet::new()),
            parallel_upload_limiter: XORB_UPLOAD_RATE_LIMITER.clone(),
            threadpool,
            upload_progress_updater,
            total_bytes_trans: 0.into(),
        }
    }

    async fn status_is_ok(&self) -> Result<()> {
        let mut upload_tasks = self.upload_tasks.lock().await;
        while let Some(result) = upload_tasks.try_join_next() {
            self.total_bytes_trans.fetch_add(result??, Ordering::Relaxed);
        }

        Ok(())
    }
}

impl ParallelXorbUploader {
    pub async fn register_new_xorb_for_upload(&self, xorb: RawXorbData) -> Result<()> {
        self.status_is_ok().await?;

        // No need to process an empty xorb.
        if xorb.num_bytes() == 0 {
            return Ok(());
        }

        let client = self.client.clone();
        let cas_prefix = self.cas_prefix.clone();
        let upload_progress_updater = self.upload_progress_updater.clone();

        let xorb_hash = xorb.hash();
        let xorb_data = xorb.to_vec();
        let chunks_and_boundaries = xorb.cas_info.chunks_and_boundaries();

        drop(xorb);

        // Acquire a permit for uploading before we spawn the task; the acquired permit is dropped after the task
        // completes. The chosen Semaphore is fair, meaning xorbs added first will be scheduled to upload first.
        //
        // It's also important to acquire the permit before the task is launched; otherwise, we may spawn an unlimited
        // number of tasks that end up using up a ton of memory; this forces the pipeline to block here while the upload
        // is happening.
        let upload_permit = self
            .parallel_upload_limiter
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| UploadTaskError(e.to_string()))?;

        self.upload_tasks.lock().await.spawn_on(
            async move {
                let n_bytes_transmitted = client.put(&cas_prefix, &xorb_hash, xorb_data, chunks_and_boundaries).await?;

                drop(upload_permit);

                if let Some(updater) = upload_progress_updater {
                    updater.update(n_bytes_transmitted as u64);
                }
                Ok(n_bytes_transmitted)
            },
            &self.threadpool.handle(),
        );

        Ok(())
    }

    /// Flush makes sure all xorbs added to queue before this call are sent successfully
    /// to remote. This function can be called multiple times and should be called at
    /// least once before `ParallelXorbUploader` is dropped.
    pub async fn finalize(&self) -> Result<usize> {
        let mut upload_tasks = self.upload_tasks.lock().await;

        while let Some(result) = upload_tasks.join_next().await {
            self.total_bytes_trans.fetch_add(result??, Ordering::Relaxed);
        }

        Ok(self.total_bytes_trans.load(Ordering::Relaxed))
    }
}
