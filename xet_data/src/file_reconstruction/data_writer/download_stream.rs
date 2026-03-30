use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::info;

use super::super::error::{FileReconstructionError, Result};
use super::super::file_reconstructor::FileReconstructor;
use super::super::run_state::RunState;
use super::sequential_writer::{SequentialRetrievalItem, SequentialWriter};

/// A streaming download handle that yields data chunks as they are reconstructed.
///
/// Created by [`FileReconstructor::reconstruct_to_stream`].  The reconstruction
/// task is spawned immediately but pauses until [`start`](Self::start) is
/// called (or the first [`next`](Self::next) / [`blocking_next`](Self::blocking_next)).
/// Because the `tokio::spawn` happens at construction time, subsequent calls to
/// `start()`, `next()`, and `blocking_next()` do **not** require a tokio runtime
/// context.
///
/// Data is delivered by pulling items directly from the sequential writer's
/// internal queue, bypassing the synchronous writer thread entirely. Each call
/// to [`blocking_next`](Self::blocking_next) / [`next`](Self::next) returns the
/// next sequential chunk, or `None` when the download is complete. Any
/// reconstruction error is surfaced on the call that would have returned the
/// next chunk (or on the final `None` boundary) via the shared run state.
pub struct DownloadStream {
    /// Channel receiver for sequential retrieval items from the writer queue.
    receiver: UnboundedReceiver<SequentialRetrievalItem>,
    /// Whether the stream has finished (no more data).
    finished: bool,
    /// Shared run state with the `FileReconstructor`. When cancelled,
    /// the reconstruction loop aborts promptly at its next check point or
    /// `select!` branch. Also used for progress reporting and error propagation.
    run_state: Arc<RunState>,
    /// Signal to unblock the spawned reconstruction task. `Some` means
    /// `start()` has not yet been called; the spawned task is waiting.
    start_signal: Option<Arc<Notify>>,
}

impl DownloadStream {
    /// Creates a new `DownloadStream`, immediately spawning the reconstruction
    /// task on the current tokio runtime.  The task blocks on an internal
    /// [`Notify`] until [`start`](Self::start) is called.
    ///
    /// # Panics
    ///
    /// Panics if called outside a tokio runtime context.
    pub(crate) fn new(reconstructor: FileReconstructor, run_state: Arc<RunState>) -> Self {
        let (data_writer, receiver) = SequentialWriter::new_streaming(run_state.clone());
        let start_signal = Arc::new(Notify::new());

        let signal = start_signal.clone();
        let rs = run_state.clone();
        tokio::spawn(async move {
            signal.notified().await;
            info!(file_hash = %rs.file_hash(), "Starting download stream");
            let _ = reconstructor.run(data_writer, rs, true).await;
        });

        Self {
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

    /// Returns the next chunk of downloaded data, blocking the current thread
    /// until data is available.
    ///
    /// Returns `Ok(None)` when the download is complete.
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime context (e.g. inside a
    /// `tokio::spawn` or `async fn`). Use from a regular thread or from
    /// [`tokio::task::spawn_blocking`] instead. For the async-safe variant,
    /// use [`next`](Self::next).
    pub fn blocking_next(&mut self) -> Result<Option<Bytes>> {
        if self.finished {
            return Ok(None);
        }
        self.ensure_started();

        match self.receiver.blocking_recv() {
            Some(SequentialRetrievalItem::Data { receiver, permit }) => {
                let data = match receiver.blocking_recv() {
                    Ok(data) => data,
                    Err(_) => {
                        self.run_state.check_error()?;
                        return Err(FileReconstructionError::InternalWriterError(
                            "Data sender was dropped before sending data.".to_string(),
                        ));
                    },
                };
                self.run_state.report_bytes_written(data.len() as u64);
                drop(permit);
                Ok(Some(data))
            },
            Some(SequentialRetrievalItem::Finish) | None => {
                self.finished = true;
                self.run_state.check_error()?;
                Ok(None)
            },
        }
    }

    /// Returns the next chunk of downloaded data asynchronously.
    ///
    /// Returns `Ok(None)` when the download is complete or cancelled.
    pub async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.finished {
            return Ok(None);
        }
        self.ensure_started();

        let item = if let Ok(item) = self.receiver.try_recv() {
            Some(item)
        } else {
            tokio::select! {
                biased;
                recv = self.receiver.recv() => recv,
                _ = self.run_state.cancelled() => None,
            }
        };

        match item {
            Some(SequentialRetrievalItem::Data { receiver, permit }) => {
                let data = match receiver.await {
                    Ok(data) => data,
                    Err(_) => {
                        self.run_state.check_error()?;
                        return Err(FileReconstructionError::InternalWriterError(
                            "Data sender was dropped before sending data.".to_string(),
                        ));
                    },
                };
                self.run_state.report_bytes_written(data.len() as u64);
                drop(permit);
                Ok(Some(data))
            },
            Some(SequentialRetrievalItem::Finish) | None => {
                self.finished = true;
                self.run_state.check_error()?;
                Ok(None)
            },
        }
    }

    /// Cancels the in-progress (or not-yet-started) download.
    ///
    /// Signals the shared run state so the reconstruction loop aborts at its
    /// next check point or `select!` branch, and closes the channel receiver.
    /// After calling this, subsequent calls to [`blocking_next`](Self::blocking_next)
    /// / [`next`](Self::next) will return `Ok(None)`.
    pub fn cancel(&mut self) {
        self.cancel_reconstruction();
        let _ = self.start_signal.take();
        self.receiver.close();
        self.finished = true;
    }
}

impl Drop for DownloadStream {
    fn drop(&mut self) {
        self.cancel_reconstruction();
        self.receiver.close();
    }
}
