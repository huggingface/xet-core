use bytes::Bytes;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{FileReconstructionError, Result};
use crate::file_reconstructor::FileReconstructor;

/// A streaming download handle that yields data chunks as they are reconstructed.
///
/// Created by [`FileReconstructor::stream_download`]. The download does not begin
/// until [`start`](Self::start) is called (or implicitly on the first call to
/// [`blocking_next`](Self::blocking_next) / [`next`](Self::next)).
///
/// Data is delivered through an unbounded channel from the reconstruction pipeline.
/// Each call to `blocking_next` / `next` returns the next sequential chunk, or
/// `None` when the download is complete. Any reconstruction error is surfaced on the
/// call that would have returned the next chunk (or on the final `None` boundary).
pub struct DownloadStream {
    /// The reconstructor ready to run. Present before `start()`, consumed on start.
    reconstructor: Option<FileReconstructor>,
    /// The spawned download task handle. Present after `start()`, consumed when
    /// the stream finishes to retrieve the final result.
    download_task: Option<JoinHandle<Result<u64>>>,
    /// Channel receiver for data chunks from the reconstruction.
    receiver: UnboundedReceiver<(Bytes, Option<OwnedSemaphorePermit>)>,
    /// Whether the stream has finished (no more data).
    finished: bool,
    /// Cancellation token shared with the `FileReconstructor`. When cancelled,
    /// the reconstruction loop aborts promptly at its next check point or
    /// `select!` branch.
    cancellation_token: CancellationToken,
}

impl DownloadStream {
    pub(crate) fn new(
        reconstructor: FileReconstructor,
        receiver: UnboundedReceiver<(Bytes, Option<OwnedSemaphorePermit>)>,
        cancellation_token: CancellationToken,
        start_prefetch: bool,
    ) -> Self {
        let mut stream = Self {
            reconstructor: Some(reconstructor),
            download_task: None,
            receiver,
            finished: false,
            cancellation_token,
        };

        if start_prefetch {
            stream.ensure_started();
        }

        stream
    }

    fn ensure_started(&mut self) {
        if let Some(reconstructor) = self.reconstructor.take() {
            self.download_task = Some(tokio::spawn(async move { reconstructor.run().await }));
        }
    }

    /// Begin the download and prefetching in the background.
    ///
    /// Spawns the reconstruction task. If not called explicitly, the first call
    /// to [`blocking_next`](Self::blocking_next) or [`next`](Self::next) will
    /// start the download automatically.
    pub fn start(&mut self) {
        self.ensure_started();
    }

    /// Returns the next chunk of downloaded data, blocking the current thread
    /// until data is available.
    ///
    /// Calls `start()` automatically if needed. Returns `Ok(None)` when the
    /// download is complete.
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime context (e.g. inside a
    /// `tokio::spawn` or `async fn`). Use from a regular thread or from
    /// [`tokio::task::spawn_blocking`] instead. For the async-safe variant,
    /// use [`next`](Self::next).
    pub fn blocking_next(&mut self) -> Result<Option<Bytes>> {
        self.ensure_started();
        if self.finished {
            return Ok(None);
        }
        match self.receiver.blocking_recv() {
            Some((bytes, _permit)) => Ok(Some(bytes)),
            None => {
                self.finished = true;
                if let Some(handle) = self.download_task.take() {
                    match tokio::runtime::Handle::current().block_on(handle) {
                        Ok(Ok(_)) => {},
                        Ok(Err(e)) => return Err(e),
                        Err(join_err) => {
                            return Err(FileReconstructionError::InternalError(format!(
                                "Download task panicked: {join_err}"
                            )));
                        },
                    }
                }
                Ok(None)
            },
        }
    }

    /// Returns the next chunk of downloaded data asynchronously.
    ///
    /// Calls `start()` automatically if needed.
    /// Returns `Ok(None)` when the download is complete.
    pub async fn next(&mut self) -> Result<Option<Bytes>> {
        self.ensure_started();
        if self.finished {
            return Ok(None);
        }
        match self.receiver.recv().await {
            Some((bytes, _permit)) => Ok(Some(bytes)),
            None => {
                self.finished = true;
                if let Some(handle) = self.download_task.take() {
                    match handle.await {
                        Ok(Ok(_)) => {},
                        Ok(Err(e)) => return Err(e),
                        Err(join_err) => {
                            return Err(FileReconstructionError::InternalError(format!(
                                "Download task panicked: {join_err}"
                            )));
                        },
                    }
                }
                Ok(None)
            },
        }
    }

    /// Cancels the in-progress download.
    ///
    /// Triggers the cancellation token so the reconstruction loop aborts at its
    /// next check point or `select!` branch, and closes the channel receiver.
    /// After calling this, subsequent calls to [`blocking_next`](Self::blocking_next)
    /// / [`next`](Self::next) will return `Ok(None)`.
    pub fn cancel(&mut self) {
        self.cancellation_token.cancel();
        self.receiver.close();
        self.finished = true;
    }
}

impl Drop for DownloadStream {
    fn drop(&mut self) {
        // Signal the reconstruction loop to stop at its next cancellation check
        // point or select! branch.
        self.cancellation_token.cancel();

        // Close the receiver so the background writer sees a BrokenPipe on its
        // next send, even before the field is dropped.
        self.receiver.close();

        // Remaining fields (receiver, reconstructor, download_task) are dropped
        // automatically after this returns, releasing any buffered channel items
        // and their associated semaphore permits.
    }
}
