use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::info;

use super::super::error::Result;
use super::super::file_reconstructor::FileReconstructor;
use super::super::run_state::RunState;
use super::unordered_writer::{CompletedTerm, UnorderedWriterProgress};

/// A streaming download handle that yields data chunks in completion order,
/// each tagged with its byte offset in the output file.
///
/// Created by [`FileReconstructor::reconstruct_to_unordered_stream`]. The
/// reconstruction task is spawned immediately but pauses until
/// [`start`](Self::start) is called (or the first [`next`](Self::next) /
/// [`blocking_next`](Self::blocking_next)). Because the `tokio::spawn`
/// happens at construction time, subsequent calls to `start()`, `next()`,
/// and `blocking_next()` do **not** require a tokio runtime context.
///
/// Unlike [`DownloadStream`](super::download_stream::DownloadStream), data
/// chunks may arrive out of order. Each chunk is returned as `(offset, Bytes)`
/// so the consumer knows where it belongs. Progress can be monitored via
/// the tracking methods which read shared atomic counters.
///
/// Holds only `Arc<WriterProgress>`, not the writer itself, so the channel
/// sender is dropped naturally when the reconstruction task finishes.
pub struct UnorderedDownloadStream {
    /// Shared atomic progress counters (also held by the writer and its tasks).
    progress: Arc<UnorderedWriterProgress>,

    /// Channel receiver for completed terms from spawned tasks.
    receiver: UnboundedReceiver<Result<CompletedTerm>>,

    /// Whether the stream has finished (no more data).
    finished: bool,

    /// Shared run state with the `FileReconstructor`.
    run_state: Arc<RunState>,

    /// Signal to unblock the spawned reconstruction task. `Some` means
    /// `start()` has not yet been called; the spawned task is waiting.
    start_signal: Option<Arc<Notify>>,
}

impl UnorderedDownloadStream {
    /// Creates a new `UnorderedDownloadStream`, immediately spawning the
    /// reconstruction task on the current tokio runtime. The task blocks
    /// on an internal [`Notify`] until [`start`](Self::start) is called.
    ///
    /// # Panics
    ///
    /// Panics if called outside a tokio runtime context.
    pub(crate) fn new(reconstructor: FileReconstructor, run_state: Arc<RunState>) -> Self {
        use super::unordered_writer::UnorderedWriter;

        let (writer, receiver, progress) = UnorderedWriter::new_streaming(run_state.clone());
        let start_signal = Arc::new(Notify::new());

        let signal = start_signal.clone();
        let rs = run_state.clone();
        tokio::spawn(async move {
            signal.notified().await;
            info!(file_hash = %rs.file_hash(), "Starting unordered download stream");
            let _ = reconstructor.run(writer, rs, true).await;
        });

        Self {
            progress,
            receiver,
            finished: false,
            run_state,
            start_signal: Some(start_signal),
        }
    }

    pub(crate) fn abort_callback(&self) -> Box<dyn Fn() + Send + Sync> {
        let run_state = self.run_state.clone();
        let start_signal = self.start_signal.clone();
        Box::new(move || {
            run_state.cancel();
            if let Some(signal) = start_signal.as_ref() {
                signal.notify_one();
            }
        })
    }

    /// Unblocks the reconstruction task so it begins producing data.
    ///
    /// If already started, this is a no-op. Called automatically on the first
    /// [`next`](Self::next) / [`blocking_next`](Self::blocking_next).
    ///
    /// This method is non-async and does not require a tokio runtime context.
    pub fn start(&mut self) {
        if let Some(signal) = self.start_signal.take() {
            signal.notify_one();
        }
    }

    fn ensure_started(&mut self) {
        if self.start_signal.is_some() {
            self.start();
        }
    }

    fn cancel_reconstruction(&self) {
        self.run_state.cancel();
        if let Some(signal) = self.start_signal.as_ref() {
            signal.notify_one();
        }
    }

    /// Returns the next chunk of downloaded data with its byte offset,
    /// blocking the current thread until data is available.
    ///
    /// Returns `Ok(None)` when the download is complete.
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime context. Use from a
    /// regular thread or from [`tokio::task::spawn_blocking`] instead.
    /// For the async-safe variant, use [`next`](Self::next).
    pub fn blocking_next(&mut self) -> Result<Option<(u64, Bytes)>> {
        if self.finished {
            return Ok(None);
        }
        self.ensure_started();

        match self.receiver.blocking_recv() {
            Some(result) => self.process_term(result),
            None => {
                self.finished = true;
                self.run_state.check_error()?;
                Ok(None)
            },
        }
    }

    /// Returns the next chunk of downloaded data with its byte offset
    /// asynchronously.
    ///
    /// Returns `Ok(None)` when the download is complete.
    pub async fn next(&mut self) -> Result<Option<(u64, Bytes)>> {
        if self.finished {
            return Ok(None);
        }
        self.ensure_started();

        if let Ok(result) = self.receiver.try_recv() {
            return self.process_term(result);
        }

        let next_item = tokio::select! {
            biased;
            recv = self.receiver.recv() => recv,
            _ = self.run_state.cancelled() => None,
        };

        match next_item {
            Some(result) => self.process_term(result),
            None => {
                self.finished = true;
                self.run_state.check_error()?;
                Ok(None)
            },
        }
    }

    fn process_term(&mut self, result: Result<CompletedTerm>) -> Result<Option<(u64, Bytes)>> {
        let term = result?;
        self.run_state.report_bytes_written(term.data.len() as u64);
        let offset = term.byte_range.start;
        let data = term.data;
        drop(term.permit);
        Ok(Some((offset, data)))
    }

    /// Cancels the in-progress (or not-yet-started) download.
    ///
    /// Signals the shared run state so the reconstruction loop aborts at its
    /// next check point. After calling this, subsequent calls to
    /// [`blocking_next`](Self::blocking_next) / [`next`](Self::next) will
    /// return `Ok(None)`.
    pub fn cancel(&mut self) {
        self.cancel_reconstruction();
        let _ = self.start_signal.take();
        self.receiver.close();
        self.finished = true;
    }

    // ── Tracking methods ─────────────────────────────────────────────────

    /// Total bytes expected for the reconstruction, read from the progress
    /// updater. Returns 0 if not yet known or no progress updater is set.
    pub fn total_bytes_expected(&self) -> u64 {
        self.run_state
            .progress_updater()
            .map(|u| u.item().total_bytes.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    /// Bytes currently being fetched by in-progress tasks.
    pub fn bytes_in_progress(&self) -> u64 {
        self.progress.bytes_in_progress()
    }

    /// Bytes that have been delivered through the progress updater.
    /// Returns 0 if no progress updater is set.
    pub fn bytes_completed(&self) -> u64 {
        self.run_state
            .progress_updater()
            .map(|u| u.total_bytes_completed())
            .unwrap_or(0)
    }

    /// Number of tasks currently resolving data futures.
    pub fn terms_in_progress(&self) -> u64 {
        self.progress.terms_in_progress()
    }

    /// Returns `true` once the stream has reached terminal state.
    ///
    /// This flips to `true` after [`next`](Self::next) / [`blocking_next`](Self::blocking_next)
    /// has observed the end-of-stream (`None`), or after [`cancel`](Self::cancel).
    /// Buffered but unconsumed channel items do not count as complete.
    pub fn is_complete(&self) -> bool {
        self.finished
    }
}

impl Drop for UnorderedDownloadStream {
    fn drop(&mut self) {
        self.cancel_reconstruction();
        self.receiver.close();
    }
}
