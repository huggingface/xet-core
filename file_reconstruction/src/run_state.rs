use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use merklehash::MerkleHash;
use progress_tracking::download_tracking::DownloadTaskUpdater;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error::{FileReconstructionError, Result};

/// Internal error type for the reconstruction run loop. Separates cancellation
/// (which maps to `Ok(0)`) from real errors (which propagate as `Err`).
pub(crate) enum RunError {
    Cancelled,
    Error(FileReconstructionError),
}

impl From<FileReconstructionError> for RunError {
    fn from(err: FileReconstructionError) -> Self {
        RunError::Error(err)
    }
}

/// Shared run state for coordinating cancellation and error propagation across
/// the reconstruction loop, background writer thread, and data-fetching tasks.
///
/// All components share the same `Arc<RunState>`. When any background task
/// encounters an error, it calls [`set_error`](Self::set_error) which stores
/// the error AND cancels the token, immediately waking any `select!` branch
/// listening on [`cancelled`](Self::cancelled).
pub(crate) struct RunState {
    cancellation_token: CancellationToken,
    has_error: AtomicBool,
    stored_error: Mutex<Option<FileReconstructionError>>,

    file_hash: MerkleHash,
    progress_updater: Option<Arc<DownloadTaskUpdater>>,

    total_terms_processed: AtomicU64,
    total_bytes_scheduled: AtomicU64,
    block_count: AtomicU64,
}

impl RunState {
    pub(crate) fn new(
        cancellation_token: CancellationToken,
        file_hash: MerkleHash,
        progress_updater: Option<Arc<DownloadTaskUpdater>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            cancellation_token,
            has_error: AtomicBool::new(false),
            stored_error: Mutex::new(None),
            file_hash,
            progress_updater,
            total_terms_processed: AtomicU64::new(0),
            total_bytes_scheduled: AtomicU64::new(0),
            block_count: AtomicU64::new(0),
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_test() -> Arc<Self> {
        Self::new(CancellationToken::new(), MerkleHash::default(), None)
    }

    /// Stores the first error and immediately cancels the token, waking any
    /// `select!` branches listening on [`cancelled`](Self::cancelled).
    /// Subsequent calls are ignored (first error wins).
    pub(crate) fn set_error(&self, error: FileReconstructionError) {
        let mut error_guard = self.stored_error.lock().unwrap();
        if error_guard.is_none() {
            *error_guard = Some(error);
            self.has_error.store(true, AtomicOrdering::Release);
        }
        drop(error_guard);
        self.cancellation_token.cancel();
    }

    /// Returns the stored error if one has been set.
    pub(crate) fn check_error(&self) -> Result<()> {
        if self.has_error.load(AtomicOrdering::Acquire) {
            let error_guard = self.stored_error.lock().unwrap();
            if let Some(err) = error_guard.as_ref() {
                return Err(err.clone());
            }
            return Err(FileReconstructionError::InternalError(
                "Unknown error occurred in background task".to_string(),
            ));
        }
        Ok(())
    }

    /// Checks for errors and cancellation. Error state is checked first so that
    /// error-triggered cancellation returns the underlying error rather than
    /// a generic cancellation.
    pub(crate) fn check_run_state(&self) -> std::result::Result<(), RunError> {
        if let Err(e) = self.check_error() {
            return Err(RunError::Error(e));
        }
        if self.is_cancelled() {
            warn!(file_hash = %self.file_hash, "Reconstruction cancelled");
            return Err(RunError::Cancelled);
        }
        Ok(())
    }

    /// Cancels without an error (genuine external cancellation).
    pub(crate) fn cancel(&self) {
        self.cancellation_token.cancel();
    }

    /// Returns true if cancelled (by error or external cancel).
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    /// Future that resolves when cancelled; for use in `select!`.
    pub(crate) async fn cancelled(&self) {
        self.cancellation_token.cancelled().await;
    }

    pub(crate) fn file_hash(&self) -> &MerkleHash {
        &self.file_hash
    }

    pub(crate) fn progress_updater(&self) -> Option<&Arc<DownloadTaskUpdater>> {
        self.progress_updater.as_ref()
    }

    /// Reports bytes written to the progress updater.
    pub(crate) fn report_bytes_written(&self, len: u64) {
        if let Some(ref updater) = self.progress_updater {
            updater.report_bytes_written(len);
        }
    }

    /// Records that a new block of file terms has begun processing.
    pub(crate) fn record_new_block(&self) -> u64 {
        self.block_count.fetch_add(1, AtomicOrdering::Relaxed) + 1
    }

    /// Records that a term of the given size has been scheduled for writing.
    pub(crate) fn record_new_term(&self, term_size: u64) {
        self.total_terms_processed.fetch_add(1, AtomicOrdering::Relaxed);
        self.total_bytes_scheduled.fetch_add(term_size, AtomicOrdering::Relaxed);
    }

    pub(crate) fn total_terms_processed(&self) -> u64 {
        self.total_terms_processed.load(AtomicOrdering::Relaxed)
    }

    pub(crate) fn total_bytes_scheduled(&self) -> u64 {
        self.total_bytes_scheduled.load(AtomicOrdering::Relaxed)
    }

    pub(crate) fn block_count(&self) -> u64 {
        self.block_count.load(AtomicOrdering::Relaxed)
    }

    /// Logs current progress stats with the file hash.
    pub(crate) fn log_progress(&self, message: &str) {
        info!(
            file_hash = %self.file_hash,
            block_count = self.block_count(),
            total_terms_processed = self.total_terms_processed(),
            total_bytes_scheduled = self.total_bytes_scheduled(),
            "{message}"
        );
    }
}
