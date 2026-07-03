use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;
use tracing::{debug, info};
use xet_data::DataError;
use xet_data::processing::{DownloadStream, FileDownloadSession, UnorderedDownloadStream};
use xet_data::progress_tracking::ItemProgressReport;
use xet_runtime::utils::UniqueId;

use super::errors::SessionError;
use super::task_runtime::TaskRuntime;

/// A streaming download handle with built-in progress tracking.
///
/// Wraps a [`DownloadStream`] and keeps a reference to the
/// [`FileDownloadSession`] that created it, so callers can poll progress
/// while consuming data chunks.  Created by
/// [`XetDownloadStreamGroup::download_stream`](super::download_stream_group::XetDownloadStreamGroup::download_stream)
/// or
/// [`XetDownloadStreamGroup::download_stream_blocking`](super::download_stream_group::XetDownloadStreamGroup::download_stream_blocking).
///
/// The reconstruction task is spawned at creation time but paused until
/// [`start`](Self::start) is called explicitly, or automatically on the
/// first call to [`next`](Self::next) / [`blocking_next`](Self::blocking_next).
/// Because the spawn happens during creation, `start()` is non-async and
/// works from any executor or plain thread.
///
/// All methods take `&self`: the stream is internally synchronized, so a
/// handle shared between threads (e.g. behind an `Arc`) can be cancelled or
/// polled for progress while another thread is blocked in
/// [`blocking_next`](Self::blocking_next) / [`next`](Self::next).
pub struct XetDownloadStream {
    /// The data path.  Locked only by `start` / `next` / `blocking_next`;
    /// `cancel` and the progress accessors never touch it.
    inner: Mutex<DownloadStream>,
    /// Lock-free cancellation handle (signals the shared run state and the
    /// start signal), captured before `inner` goes behind the mutex so
    /// `cancel` never waits on a blocked `next` / `blocking_next`.
    abort: Box<dyn Fn() + Send + Sync>,
    download_session: Arc<FileDownloadSession>,
    id: UniqueId,
    task_runtime: Arc<TaskRuntime>,
}

impl XetDownloadStream {
    pub(super) fn new(
        inner: DownloadStream,
        download_session: Arc<FileDownloadSession>,
        id: UniqueId,
        task_runtime: Arc<TaskRuntime>,
    ) -> Self {
        let abort = inner.abort_callback();
        Self {
            inner: Mutex::new(inner),
            abort,
            download_session,
            id,
            task_runtime,
        }
    }

    /// Unblocks the reconstruction task so it begins producing data.
    ///
    /// If already started, this is a no-op.  Called automatically on the first
    /// [`next`](Self::next) / [`blocking_next`](Self::blocking_next).  If
    /// another thread currently holds the stream (inside `next` /
    /// `blocking_next`), the stream is already started and this is a no-op.
    ///
    /// This method is non-async and does not require a tokio runtime context.
    pub fn start(&self) {
        info!(stream_id = %self.id, "Download stream start");
        if let Ok(mut inner) = self.inner.try_lock() {
            inner.start();
        }
    }

    /// Returns the next chunk of downloaded data asynchronously.
    ///
    /// Returns `Ok(None)` when the download is complete.
    pub async fn next(&self) -> Result<Option<Bytes>, SessionError> {
        debug!(stream_id = %self.id, "Download stream next");
        let mut inner = self.inner.lock().await;
        inner.next().await.map_err(|e| SessionError::from(DataError::from(e)))
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
    #[cfg(not(target_family = "wasm"))]
    pub fn blocking_next(&self) -> Result<Option<Bytes>, SessionError> {
        debug!(stream_id = %self.id, "Download stream next");
        let mut inner = self.inner.blocking_lock();
        inner.blocking_next().map_err(|e| SessionError::from(DataError::from(e)))
    }

    /// Cancels the in-progress (or not-yet-started) download.
    ///
    /// Callable from any thread without exclusive access: a thread currently
    /// blocked in [`blocking_next`](Self::blocking_next) / [`next`](Self::next)
    /// wakes and returns `Ok(None)`, and subsequent calls return `Ok(None)`.
    pub fn cancel(&self) {
        info!(stream_id = %self.id, "Download stream cancel");
        let _ = self.task_runtime.cancel_subtree();
        (self.abort)();
    }

    /// Returns the unique task ID for this stream.
    pub fn task_id(&self) -> UniqueId {
        self.id
    }

    /// Returns a snapshot of this stream's download progress, or `None` if
    /// the progress item is not yet available.
    ///
    /// The returned [`ItemProgressReport`] contains the item name,
    /// total bytes, and bytes completed so far. This method is lock-free
    /// (reads atomic counters) and safe to call from any thread.
    pub fn progress(&self) -> Option<ItemProgressReport> {
        self.download_session.item_report(self.id)
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
/// [`XetDownloadStreamGroup::download_unordered_stream`](super::download_stream_group::XetDownloadStreamGroup::download_unordered_stream) or
/// [`XetDownloadStreamGroup::download_unordered_stream_blocking`](super::download_stream_group::XetDownloadStreamGroup::download_unordered_stream_blocking).
///
/// The reconstruction task is spawned at creation time but paused until
/// [`start`](Self::start) is called explicitly, or automatically on the
/// first call to [`next`](Self::next) / [`blocking_next`](Self::blocking_next).
/// Because the spawn happens during creation, `start()` is non-async and
/// works from any executor or plain thread.
///
/// All methods take `&self`: the stream is internally synchronized, so a
/// handle shared between threads (e.g. behind an `Arc`) can be cancelled or
/// polled for progress while another thread is blocked in
/// [`blocking_next`](Self::blocking_next) / [`next`](Self::next).
pub struct XetUnorderedDownloadStream {
    /// The data path.  Locked only by `start` / `next` / `blocking_next`;
    /// `cancel` and the progress accessors never touch it.
    inner: Mutex<UnorderedDownloadStream>,
    /// Lock-free cancellation handle (signals the shared run state and the
    /// start signal), captured before `inner` goes behind the mutex so
    /// `cancel` never waits on a blocked `next` / `blocking_next`.
    abort: Box<dyn Fn() + Send + Sync>,
    download_session: Arc<FileDownloadSession>,
    id: UniqueId,
    task_runtime: Arc<TaskRuntime>,
}

impl XetUnorderedDownloadStream {
    pub(super) fn new(
        inner: UnorderedDownloadStream,
        download_session: Arc<FileDownloadSession>,
        id: UniqueId,
        task_runtime: Arc<TaskRuntime>,
    ) -> Self {
        let abort = inner.abort_callback();
        Self {
            inner: Mutex::new(inner),
            abort,
            download_session,
            id,
            task_runtime,
        }
    }

    /// Unblocks the reconstruction task so it begins producing data.
    ///
    /// If already started, this is a no-op.  Called automatically on the first
    /// [`next`](Self::next) / [`blocking_next`](Self::blocking_next).  If
    /// another thread currently holds the stream (inside `next` /
    /// `blocking_next`), the stream is already started and this is a no-op.
    ///
    /// This method is non-async and does not require a tokio runtime context.
    pub fn start(&self) {
        info!(stream_id = %self.id, "Download stream start");
        if let Ok(mut inner) = self.inner.try_lock() {
            inner.start();
        }
    }

    /// Returns the next chunk of downloaded data with its byte offset
    /// asynchronously.
    ///
    /// Returns `Ok(None)` when the download is complete.
    pub async fn next(&self) -> Result<Option<(u64, Bytes)>, SessionError> {
        debug!(stream_id = %self.id, "Download stream next");
        let mut inner = self.inner.lock().await;
        inner.next().await.map_err(|e| SessionError::from(DataError::from(e)))
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
    #[cfg(not(target_family = "wasm"))]
    pub fn blocking_next(&self) -> Result<Option<(u64, Bytes)>, SessionError> {
        debug!(stream_id = %self.id, "Download stream next");
        let mut inner = self.inner.blocking_lock();
        inner.blocking_next().map_err(|e| SessionError::from(DataError::from(e)))
    }

    /// Cancels the in-progress (or not-yet-started) download.
    ///
    /// Callable from any thread without exclusive access: a thread currently
    /// blocked in [`blocking_next`](Self::blocking_next) / [`next`](Self::next)
    /// wakes and returns `Ok(None)`, and subsequent calls return `Ok(None)`.
    pub fn cancel(&self) {
        info!(stream_id = %self.id, "Download stream cancel");
        let _ = self.task_runtime.cancel_subtree();
        (self.abort)();
    }

    /// Returns the unique task ID for this stream.
    pub fn task_id(&self) -> UniqueId {
        self.id
    }

    /// Returns a snapshot of this stream's download progress, or `None` if
    /// the progress item is not yet available.
    ///
    /// The returned [`ItemProgressReport`] contains the item name,
    /// total bytes, and bytes completed so far. This method is lock-free
    /// (reads atomic counters) and safe to call from any thread.
    pub fn progress(&self) -> Option<ItemProgressReport> {
        self.download_session.item_report(self.id)
    }
}

impl Drop for XetUnorderedDownloadStream {
    fn drop(&mut self) {
        self.download_session.unregister_stream_abort_callback(self.id);
    }
}
