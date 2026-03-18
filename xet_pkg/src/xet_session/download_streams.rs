use bytes::Bytes;
use xet_data::DataError;
use xet_data::processing::{DownloadStream, UnorderedDownloadStream};

use super::errors::SessionError;

/// A streaming download handle.
///
/// Wraps a [`DownloadStream`] and provides async/blocking accessors.
/// Created by [`XetSession::download_stream`](super::XetSession::download_stream) or
/// [`XetSession::download_stream_blocking`](super::XetSession::download_stream_blocking).
///
/// The reconstruction task is spawned at creation time but paused until
/// [`start`](Self::start) is called explicitly, or automatically on the
/// first call to [`next`](Self::next) / [`blocking_next`](Self::blocking_next).
pub struct XetDownloadStream {
    inner: DownloadStream,
}

impl XetDownloadStream {
    pub(super) fn new(inner: DownloadStream) -> Self {
        Self { inner }
    }

    /// Unblocks the reconstruction task so it begins producing data.
    ///
    /// If already started, this is a no-op. Called automatically on the first
    /// [`next`](Self::next) / [`blocking_next`](Self::blocking_next).
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
        self.inner.cancel();
    }
}

/// A streaming download handle that yields data chunks in completion order,
/// each tagged with their byte offset in the output file.
///
/// Wraps an [`UnorderedDownloadStream`] and provides async/blocking accessors.
/// Created by [`XetSession::download_unordered_stream`](super::XetSession::download_unordered_stream) or
/// [`XetSession::download_unordered_stream_blocking`](super::XetSession::download_unordered_stream_blocking).
///
/// The reconstruction task is spawned at creation time but paused until
/// [`start`](Self::start) is called explicitly, or automatically on the
/// first call to [`next`](Self::next) / [`blocking_next`](Self::blocking_next).
pub struct XetUnorderedDownloadStream {
    inner: UnorderedDownloadStream,
}

impl XetUnorderedDownloadStream {
    pub(super) fn new(inner: UnorderedDownloadStream) -> Self {
        Self { inner }
    }

    /// Unblocks the reconstruction task so it begins producing data.
    ///
    /// If already started, this is a no-op. Called automatically on the first
    /// [`next`](Self::next) / [`blocking_next`](Self::blocking_next).
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
        self.inner.cancel();
    }
}
