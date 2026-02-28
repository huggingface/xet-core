use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::info;

use super::sequential_writer::{SequentialRetrievalItem, SequentialWriter};
use crate::error::{FileReconstructionError, Result};
use crate::file_reconstructor::FileReconstructor;
use crate::run_state::RunState;

/// A streaming download handle that yields data chunks as they are reconstructed.
///
/// Created by [`FileReconstructor::reconstruct_to_stream`]. The reconstruction
/// task is **not** started until [`start`](Self::start) is called explicitly, or
/// automatically on the first call to [`next`](Self::next) /
/// [`blocking_next`](Self::blocking_next).
///
/// Data is delivered by pulling items directly from the sequential writer's
/// internal queue, bypassing the synchronous writer thread entirely. Each call
/// to [`blocking_next`](Self::blocking_next) / [`next`](Self::next) returns the
/// next sequential chunk, or `None` when the download is complete. Any
/// reconstruction error is surfaced on the call that would have returned the
/// next chunk (or on the final `None` boundary) via the shared run state.
pub struct DownloadStream {
    /// The `FileReconstructor` to start when `start()` is called.
    /// `None` once the reconstruction has been started (or cancelled before start).
    reconstructor: Option<FileReconstructor>,
    /// Channel receiver for sequential retrieval items from the writer queue (set after start).
    receiver: Option<UnboundedReceiver<SequentialRetrievalItem>>,
    /// Whether the stream has finished (no more data).
    finished: bool,
    /// Shared run state with the `FileReconstructor`. When cancelled,
    /// the reconstruction loop aborts promptly at its next check point or
    /// `select!` branch. Also used for progress reporting and error propagation.
    run_state: Arc<RunState>,
}

impl DownloadStream {
    pub(crate) fn new(reconstructor: FileReconstructor, run_state: Arc<RunState>) -> Self {
        Self {
            reconstructor: Some(reconstructor),
            receiver: None,
            finished: false,
            run_state,
        }
    }

    /// Starts the reconstruction task in the background. If already started,
    /// this is a no-op. Called automatically on the first [`next`](Self::next) /
    /// [`blocking_next`](Self::blocking_next).
    pub fn start(&mut self) {
        if let Some(reconstructor) = self.reconstructor.take() {
            info!(file_hash = %self.run_state.file_hash(), "Starting download stream");

            let (data_writer, receiver) = SequentialWriter::new_streaming(self.run_state.clone());
            let run_state = self.run_state.clone();

            tokio::spawn(async move {
                let _ = reconstructor.run(data_writer, run_state, true).await;
            });
            self.receiver = Some(receiver);
        }
    }

    fn ensure_started(&mut self) {
        if self.reconstructor.is_some() {
            self.start();
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
        let receiver = self.receiver.as_mut().expect("receiver must exist after start");

        match receiver.blocking_recv() {
            Some(SequentialRetrievalItem::Data { receiver, permit }) => {
                let data = receiver.blocking_recv().map_err(|_| {
                    FileReconstructionError::InternalWriterError(
                        "Data sender was dropped before sending data.".to_string(),
                    )
                })?;
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
    /// Returns `Ok(None)` when the download is complete.
    pub async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.finished {
            return Ok(None);
        }
        self.ensure_started();
        let receiver = self.receiver.as_mut().expect("receiver must exist after start");

        match receiver.recv().await {
            Some(SequentialRetrievalItem::Data { receiver, permit }) => {
                let data = receiver.await.map_err(|_| {
                    FileReconstructionError::InternalWriterError(
                        "Data sender was dropped before sending data.".to_string(),
                    )
                })?;
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
        self.run_state.cancel();
        self.reconstructor.take();
        if let Some(ref mut receiver) = self.receiver {
            receiver.close();
        }
        self.finished = true;
    }
}

impl Drop for DownloadStream {
    fn drop(&mut self) {
        self.run_state.cancel();
        if let Some(ref mut receiver) = self.receiver {
            receiver.close();
        }
    }
}
