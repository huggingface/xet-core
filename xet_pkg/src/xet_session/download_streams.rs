use std::sync::Arc;

use bytes::Bytes;
use xet_data::DataError;
use xet_data::processing::{DownloadStream, FileDownloadSession, UnorderedDownloadStream};
use xet_data::progress_tracking::{ItemProgressReport, UniqueID};

use super::errors::SessionError;
use super::task_runtime::TaskRuntime;

/// A streaming download handle with built-in progress tracking.
///
/// Wraps a [`DownloadStream`] and keeps a reference to the
/// [`FileDownloadSession`] that created it, so callers can poll progress
/// while consuming data chunks.  Created by
/// [`XetSession::download_stream`](super::XetSession::download_stream) or
/// [`XetSession::download_stream_blocking`](super::XetSession::download_stream_blocking).
///
/// The reconstruction task is spawned at creation time but paused until
/// [`start`](Self::start) is called explicitly, or automatically on the
/// first call to [`next`](Self::next) / [`blocking_next`](Self::blocking_next).
/// Because the spawn happens during creation, `start()` is non-async and
/// works from any executor or plain thread.
pub struct XetDownloadStream {
    inner: DownloadStream,
    download_session: Arc<FileDownloadSession>,
    id: UniqueID,
    task_runtime: Arc<TaskRuntime>,
}

impl XetDownloadStream {
    pub(super) fn new(
        inner: DownloadStream,
        download_session: Arc<FileDownloadSession>,
        id: UniqueID,
        task_runtime: Arc<TaskRuntime>,
    ) -> Self {
        Self {
            inner,
            download_session,
            id,
            task_runtime,
        }
    }

    /// Unblocks the reconstruction task so it begins producing data.
    ///
    /// If already started, this is a no-op. Called automatically on the first
    /// [`next`](Self::next) / [`blocking_next`](Self::blocking_next).
    ///
    /// This method is non-async and does not require a tokio runtime context.
    pub fn start(&mut self) {
        self.inner.start();
    }

    /// Returns the next chunk of downloaded data asynchronously.
    ///
    /// Returns `Ok(None)` when the download is complete.
    pub async fn next(&mut self) -> Result<Option<Bytes>, SessionError> {
        self.inner.next().await.map_err(|e| SessionError::from(DataError::from(e)))
    }

    /// Returns the next chunk of downloaded data, blocking the current thread
    /// until data is available.
    ///
    /// Returns `Ok(None)` when the download is complete.
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime context. Use
    /// [`next`](Self::next) for async contexts.
    pub fn blocking_next(&mut self) -> Result<Option<Bytes>, SessionError> {
        self.inner.blocking_next().map_err(|e| SessionError::from(DataError::from(e)))
    }

    /// Cancels the in-progress (or not-yet-started) download.
    ///
    /// Subsequent calls to [`next`](Self::next) / [`blocking_next`](Self::blocking_next)
    /// will return `Ok(None)`.
    pub fn cancel(&mut self) {
        let _ = self.task_runtime.cancel_subtree();
        self.inner.cancel();
    }

    /// Returns a snapshot of this stream's download progress.
    ///
    /// The returned [`ItemProgressReport`] contains the item name,
    /// total bytes, and bytes completed so far. This method is lock-free
    /// (reads atomic counters) and safe to call from any thread.
    pub fn get_progress(&self) -> ItemProgressReport {
        self.download_session
            .item_report(self.id)
            .expect("progress item was registered at stream creation and is never removed")
    }
}

impl Drop for XetDownloadStream {
    fn drop(&mut self) {
        self.download_session.unregister_stream_abort_callback(self.id);
    }
}

/// A streaming download handle that yields data chunks in completion order,
/// each tagged with their byte offset in the output file.
///
/// Wraps an [`UnorderedDownloadStream`] and keeps a reference to the
/// [`FileDownloadSession`] that created it, so callers can poll progress
/// while consuming data chunks. Created by
/// [`XetSession::download_unordered_stream`](super::XetSession::download_unordered_stream) or
/// [`XetSession::download_unordered_stream_blocking`](super::XetSession::download_unordered_stream_blocking).
///
/// The reconstruction task is spawned at creation time but paused until
/// [`start`](Self::start) is called explicitly, or automatically on the
/// first call to [`next`](Self::next) / [`blocking_next`](Self::blocking_next).
/// Because the spawn happens during creation, `start()` is non-async and
/// works from any executor or plain thread.
pub struct XetUnorderedDownloadStream {
    inner: UnorderedDownloadStream,
    download_session: Arc<FileDownloadSession>,
    id: UniqueID,
    task_runtime: Arc<TaskRuntime>,
}

impl XetUnorderedDownloadStream {
    pub(super) fn new(
        inner: UnorderedDownloadStream,
        download_session: Arc<FileDownloadSession>,
        id: UniqueID,
        task_runtime: Arc<TaskRuntime>,
    ) -> Self {
        Self {
            inner,
            download_session,
            id,
            task_runtime,
        }
    }

    /// Unblocks the reconstruction task so it begins producing data.
    ///
    /// If already started, this is a no-op. Called automatically on the first
    /// [`next`](Self::next) / [`blocking_next`](Self::blocking_next).
    ///
    /// This method is non-async and does not require a tokio runtime context.
    pub fn start(&mut self) {
        self.inner.start();
    }

    /// Returns the next chunk of downloaded data with its byte offset
    /// asynchronously.
    ///
    /// Returns `Ok(None)` when the download is complete.
    pub async fn next(&mut self) -> Result<Option<(u64, Bytes)>, SessionError> {
        self.inner.next().await.map_err(|e| SessionError::from(DataError::from(e)))
    }

    /// Returns the next chunk of downloaded data with its byte offset,
    /// blocking the current thread until data is available.
    ///
    /// Returns `Ok(None)` when the download is complete.
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime context. Use
    /// [`next`](Self::next) for async contexts.
    pub fn blocking_next(&mut self) -> Result<Option<(u64, Bytes)>, SessionError> {
        self.inner.blocking_next().map_err(|e| SessionError::from(DataError::from(e)))
    }

    /// Cancels the in-progress (or not-yet-started) download.
    ///
    /// Subsequent calls to [`next`](Self::next) / [`blocking_next`](Self::blocking_next)
    /// will return `Ok(None)`.
    pub fn cancel(&mut self) {
        let _ = self.task_runtime.cancel_subtree();
        self.inner.cancel();
    }

    /// Returns a snapshot of this stream's download progress.
    ///
    /// The returned [`ItemProgressReport`] contains the item name,
    /// total bytes, and bytes completed so far. This method is lock-free
    /// (reads atomic counters) and safe to call from any thread.
    pub fn get_progress(&self) -> ItemProgressReport {
        self.download_session
            .item_report(self.id)
            .expect("progress item was registered at stream creation and is never removed")
    }
}

impl Drop for XetUnorderedDownloadStream {
    fn drop(&mut self) {
        self.download_session.unregister_stream_abort_callback(self.id);
    }
}
