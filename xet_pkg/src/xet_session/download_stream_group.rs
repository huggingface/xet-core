//! [`DownloadStreamGroup`], [`XetDownloadStream`], and [`XetUnorderedDownloadStream`]
//! — authenticated streaming downloads.
//!
//! [`DownloadStreamGroup`] manages a shared CAS connection pool and auth token for one
//! or more concurrent streams.  Obtain one via
//! [`XetSession::new_download_stream_group`](super::session::XetSession::new_download_stream_group).
//!
//! Each call to [`DownloadStreamGroup::download_stream`] or
//! [`DownloadStreamGroup::download_unordered_stream`] returns a stream handle that
//! yields data independently.  Ordered streams ([`XetDownloadStream`]) deliver bytes
//! in file order; unordered streams ([`XetUnorderedDownloadStream`]) yield
//! `(offset, Bytes)` chunks in completion order for lower latency.

use std::ops::Range;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http::HeaderMap;
use xet_data::DataError;
use xet_data::processing::{DownloadStream, FileDownloadSession, UnorderedDownloadStream, XetFileInfo};
use xet_data::progress_tracking::{ItemProgressReport, UniqueID};

use super::common::create_translator_config;
use super::errors::SessionError;
use super::session::XetSession;
use crate::error::XetError;

/// Builder for [`DownloadStreamGroup`].
///
/// Obtain via [`XetSession::new_download_stream_group`], configure per-group auth
/// with [`with_token_info`](Self::with_token_info) and
/// [`with_token_refresh_url`](Self::with_token_refresh_url), then call
/// [`build`](Self::build) (async) or [`build_blocking`](Self::build_blocking) (sync).
pub struct DownloadStreamGroupBuilder {
    session: XetSession,
    token_info: Option<(String, u64)>,
    token_refresh: Option<(String, Arc<HeaderMap>)>,
}

impl DownloadStreamGroupBuilder {
    pub(super) fn new(session: XetSession) -> Self {
        Self {
            session,
            token_info: None,
            token_refresh: None,
        }
    }

    /// Seed an initial CAS access token and its expiry as a Unix timestamp (seconds).
    ///
    /// When combined with [`with_token_refresh_url`](Self::with_token_refresh_url) this
    /// avoids an extra refresh round-trip on the first request.
    pub fn with_token_info(self, token: impl Into<String>, expiry: u64) -> Self {
        Self {
            token_info: Some((token.into(), expiry)),
            ..self
        }
    }

    /// Set a URL this group will call (HTTP GET) to obtain a fresh CAS access token
    /// whenever the current one is about to expire.
    ///
    /// `headers` are sent with every token-refresh request for this group.
    pub fn with_token_refresh_url(self, url: impl Into<String>, headers: HeaderMap) -> Self {
        Self {
            token_refresh: Some((url.into(), Arc::new(headers))),
            ..self
        }
    }

    /// Create the [`DownloadStreamGroup`] from an async context.
    pub async fn build(self) -> Result<DownloadStreamGroup, SessionError> {
        let DownloadStreamGroupBuilder {
            session,
            token_info,
            token_refresh,
        } = self;
        let group = session
            .runtime
            .bridge_async("new_download_stream_group", {
                let session = session.clone();
                async move { DownloadStreamGroup::new(session, token_info, token_refresh).await }
            })
            .await??;
        session
            .active_download_stream_groups
            .lock()?
            .insert(group.id(), Arc::downgrade(&group.inner));
        Ok(group)
    }

    /// Create the [`DownloadStreamGroup`] from a sync context.
    ///
    /// # Errors
    ///
    /// Returns [`SessionError::WrongRuntimeMode`] if the session wraps an external
    /// tokio runtime (created via [`XetSessionBuilder::with_tokio_handle`] or
    /// auto-detected inside `#[tokio::main]`).
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime on an Owned-mode session.
    pub fn build_blocking(self) -> Result<DownloadStreamGroup, SessionError> {
        let DownloadStreamGroupBuilder {
            session,
            token_info,
            token_refresh,
        } = self;
        let group = session.runtime.bridge_sync({
            let session = session.clone();
            async move { DownloadStreamGroup::new(session, token_info, token_refresh).await }
        })??;
        session
            .active_download_stream_groups
            .lock()?
            .insert(group.id(), Arc::downgrade(&group.inner));
        Ok(group)
    }
}

/// API for creating authenticated streaming downloads.
///
/// Obtain via [`XetSession::new_download_stream_group`] — configure per-group
/// auth on the returned [`DownloadStreamGroupBuilder`], then call
/// [`build`](DownloadStreamGroupBuilder::build) (async) or
/// [`build_blocking`](DownloadStreamGroupBuilder::build_blocking) (sync).
///
/// Create streams with [`download_stream`](Self::download_stream) /
/// [`download_stream_blocking`](Self::download_stream_blocking) for ordered
/// byte output, or [`download_unordered_stream`](Self::download_unordered_stream) /
/// [`download_unordered_stream_blocking`](Self::download_unordered_stream_blocking)
/// for out-of-order chunk delivery.
///
/// Each stream is independent — multiple streams can be active concurrently
/// from the same group.  All streams created from the same group share the
/// same CAS connection pool and auth token.
///
/// # Cloning
///
/// Cloning is cheap — it simply increments an atomic reference count.
/// All clones share the same underlying [`FileDownloadSession`].
#[derive(Clone)]
pub struct DownloadStreamGroup {
    pub(super) inner: Arc<DownloadStreamGroupInner>,
}

impl std::ops::Deref for DownloadStreamGroup {
    type Target = DownloadStreamGroupInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// All shared state owned by a single DownloadStreamGroup instance.
/// Accessed through `Arc<DownloadStreamGroupInner>`; do not use this type directly.
#[doc(hidden)]
pub struct DownloadStreamGroupInner {
    group_id: UniqueID,
    session: XetSession,
    /// Wrapped in `Option` so [`abort`](DownloadStreamGroup::abort) can drop the
    /// [`FileDownloadSession`] and cancel all active streams.
    download_session: Mutex<Option<Arc<FileDownloadSession>>>,
}

impl DownloadStreamGroup {
    /// Create a new download stream group. Called by [`DownloadStreamGroupBuilder::build`].
    pub(super) async fn new(
        session: XetSession,
        token_info: Option<(String, u64)>,
        token_refresh: Option<(String, Arc<HeaderMap>)>,
    ) -> Result<Self, XetError> {
        let group_id = UniqueID::new();
        let config = create_translator_config(&session, token_info, token_refresh.as_ref())?;
        let download_session = FileDownloadSession::new(Arc::new(config)).await?;

        Ok(Self {
            inner: Arc::new(DownloadStreamGroupInner {
                group_id,
                session,
                download_session: Mutex::new(Some(download_session)),
            }),
        })
    }

    /// Get the group ID.
    pub(super) fn id(&self) -> UniqueID {
        self.group_id
    }

    /// Abort this download stream group, cancelling all active streams.
    pub(super) fn abort(&self) -> Result<(), XetError> {
        if let Some(session) = self.download_session.lock()?.take() {
            session.abort_active_streams();
        }
        Ok(())
    }

    fn get_download_session(&self) -> Result<Arc<FileDownloadSession>, XetError> {
        self.download_session.lock()?.clone().ok_or(XetError::Aborted)
    }

    /// Create a [`XetDownloadStream`] for the given file, optionally
    /// restricted to a byte range.
    ///
    /// The returned stream yields data chunks as they are reconstructed,
    /// with built-in progress tracking via
    /// [`get_progress`](XetDownloadStream::get_progress).
    /// The reconstruction task is spawned on the session's runtime but
    /// paused until [`start`](XetDownloadStream::start) is called (or the
    /// first [`next`](XetDownloadStream::next) /
    /// [`blocking_next`](XetDownloadStream::blocking_next)). Because the
    /// spawn happens during creation, `start()` and `next()` work from any
    /// executor (tokio, smol, async-std, futures).
    ///
    /// If `range` is `Some`, only the specified byte range of the file is
    /// reconstructed.
    ///
    /// Returns `Err(SessionError::Aborted)` if the session or this group has been aborted.
    pub async fn download_stream(
        &self,
        file_info: XetFileInfo,
        range: Option<Range<u64>>,
    ) -> Result<XetDownloadStream, SessionError> {
        self.session.check_alive()?;

        let group = self.clone();
        self.session
            .runtime
            .bridge_async("download_stream", async move {
                let dl_session = group.get_download_session()?;
                let (id, stream) = dl_session.download_stream(&file_info, range).await?;
                Ok(XetDownloadStream::new(stream, dl_session, id))
            })
            .await?
    }

    /// Blocking version of [`download_stream`](Self::download_stream).
    ///
    /// The reconstruction task is spawned on the session's runtime but
    /// paused until [`start`](XetDownloadStream::start) is called (or the
    /// first [`blocking_next`](XetDownloadStream::blocking_next)). No
    /// tokio runtime context is required on the calling thread after this
    /// method returns.
    ///
    /// # Errors
    ///
    /// Returns [`SessionError::WrongRuntimeMode`] if the session wraps an external
    /// tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime on an Owned-mode session.
    pub fn download_stream_blocking(
        &self,
        file_info: XetFileInfo,
        range: Option<Range<u64>>,
    ) -> Result<XetDownloadStream, SessionError> {
        self.session.check_alive()?;

        let group = self.clone();
        self.session.runtime.bridge_sync(async move {
            let dl_session = group.get_download_session()?;
            let (id, stream) = dl_session.download_stream(&file_info, range).await?;
            Ok(XetDownloadStream::new(stream, dl_session, id))
        })?
    }

    /// Create an [`XetUnorderedDownloadStream`] for the given file,
    /// optionally restricted to a byte range.
    ///
    /// The returned stream yields `(offset, Bytes)` chunks in whatever
    /// order they complete, with built-in progress tracking via
    /// [`get_progress`](XetUnorderedDownloadStream::get_progress).
    ///
    /// If `range` is `Some`, only the specified byte range of the file is
    /// reconstructed.
    ///
    /// Can be awaited from any async executor (tokio, smol, async-std, futures).
    ///
    /// Returns `Err(SessionError::Aborted)` if the session or this group has been aborted.
    pub async fn download_unordered_stream(
        &self,
        file_info: XetFileInfo,
        range: Option<Range<u64>>,
    ) -> Result<XetUnorderedDownloadStream, SessionError> {
        self.session.check_alive()?;

        let group = self.clone();
        self.session
            .runtime
            .bridge_async("download_unordered_stream", async move {
                let dl_session = group.get_download_session()?;
                let (id, stream) = dl_session.download_unordered_stream(&file_info, range).await?;
                Ok(XetUnorderedDownloadStream::new(stream, dl_session, id))
            })
            .await?
    }

    /// Blocking version of [`download_unordered_stream`](Self::download_unordered_stream).
    ///
    /// The reconstruction task is spawned on the session's runtime but
    /// paused until [`start`](XetUnorderedDownloadStream::start) is called
    /// (or the first [`blocking_next`](XetUnorderedDownloadStream::blocking_next)).
    /// No tokio runtime context is required on the calling thread after
    /// this method returns.
    ///
    /// # Errors
    ///
    /// Returns [`SessionError::WrongRuntimeMode`] if the session wraps an external
    /// tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime on an Owned-mode session.
    pub fn download_unordered_stream_blocking(
        &self,
        file_info: XetFileInfo,
        range: Option<Range<u64>>,
    ) -> Result<XetUnorderedDownloadStream, SessionError> {
        self.session.check_alive()?;

        let group = self.clone();
        self.session.runtime.bridge_sync(async move {
            let dl_session = group.get_download_session()?;
            let (id, stream) = dl_session.download_unordered_stream(&file_info, range).await?;
            Ok(XetUnorderedDownloadStream::new(stream, dl_session, id))
        })?
    }
}

/// A streaming download handle with built-in progress tracking.
///
/// Wraps a [`DownloadStream`] and keeps a reference to the
/// [`FileDownloadSession`] that created it, so callers can poll progress
/// while consuming data chunks.  Created by
/// [`DownloadStreamGroup::download_stream`] or
/// [`DownloadStreamGroup::download_stream_blocking`].
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
}

impl XetDownloadStream {
    pub(super) fn new(inner: DownloadStream, download_session: Arc<FileDownloadSession>, id: UniqueID) -> Self {
        Self {
            inner,
            download_session,
            id,
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
/// [`DownloadStreamGroup::download_unordered_stream`] or
/// [`DownloadStreamGroup::download_unordered_stream_blocking`].
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
}

impl XetUnorderedDownloadStream {
    pub(super) fn new(
        inner: UnorderedDownloadStream,
        download_session: Arc<FileDownloadSession>,
        id: UniqueID,
    ) -> Self {
        Self {
            inner,
            download_session,
            id,
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

#[cfg(test)]
mod tests {
    use tempfile::{TempDir, tempdir};
    use xet_data::processing::{Sha256Policy, XetFileInfo};

    use super::super::session::{XetSession, XetSessionBuilder};
    use super::*;

    async fn local_session(temp: &TempDir) -> Result<XetSession, Box<dyn std::error::Error>> {
        let cas_path = temp.path().join("cas");
        Ok(XetSessionBuilder::new()
            .with_endpoint(format!("local://{}", cas_path.display()))
            .build()?)
    }

    fn local_session_sync(temp: &TempDir) -> Result<XetSession, Box<dyn std::error::Error>> {
        let cas_path = temp.path().join("cas");
        Ok(XetSessionBuilder::new()
            .with_endpoint(format!("local://{}", cas_path.display()))
            .build()?)
    }

    async fn upload_bytes(
        session: &XetSession,
        data: &[u8],
        name: &str,
    ) -> Result<XetFileInfo, Box<dyn std::error::Error>> {
        let commit = session.new_upload_commit()?.build().await?;
        let handle = commit
            .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some(name.into()))
            .await?;
        let results = commit.commit().await?;
        let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
        Ok(XetFileInfo {
            hash: meta.hash.clone(),
            file_size: Some(meta.file_size),
            sha256: meta.sha256.clone(),
        })
    }

    fn upload_bytes_blocking(
        session: &XetSession,
        data: &[u8],
        name: &str,
    ) -> Result<XetFileInfo, Box<dyn std::error::Error>> {
        let commit = session.new_upload_commit()?.build_blocking()?;
        let handle = commit.upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some(name.into()))?;
        let results = commit.commit_blocking()?;
        let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
        Ok(XetFileInfo {
            hash: meta.hash.clone(),
            file_size: Some(meta.file_size),
            sha256: meta.sha256.clone(),
        })
    }

    async fn stream_group_async(session: &XetSession) -> DownloadStreamGroup {
        session.new_download_stream_group().unwrap().build().await.unwrap()
    }

    fn stream_group_sync(session: &XetSession) -> DownloadStreamGroup {
        session.new_download_stream_group().unwrap().build_blocking().unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    // Async streaming download round-trip: upload, stream, verify content.
    async fn test_download_stream_round_trip() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let original = b"Hello, streaming download!";
        let file_info = upload_bytes(&session, original, "stream.bin").await.unwrap();

        let group = stream_group_async(&session).await;
        let mut stream = group.download_stream(file_info, None).await.unwrap();
        let mut collected = Vec::new();
        while let Some(chunk) = stream.next().await.unwrap() {
            collected.extend_from_slice(&chunk);
        }
        assert_eq!(collected, original);
    }

    #[test]
    // Blocking streaming download round-trip: upload, stream, verify content.
    fn test_download_stream_blocking_round_trip() {
        let temp = tempdir().unwrap();
        let session = local_session_sync(&temp).unwrap();
        let original = b"Hello, blocking streaming download!";
        let file_info = upload_bytes_blocking(&session, original, "stream.bin").unwrap();

        let group = stream_group_sync(&session);
        let mut stream = group.download_stream_blocking(file_info, None).unwrap();

        let mut collected = Vec::new();
        while let Some(chunk) = stream.blocking_next().unwrap() {
            collected.extend_from_slice(&chunk);
        }
        assert_eq!(collected, original);
    }

    #[tokio::test(flavor = "multi_thread")]
    // get_progress() reports correct totals after consuming the stream.
    async fn test_download_stream_progress_reports_completion() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let original = b"progress tracking test data for streaming";
        let file_info = upload_bytes(&session, original, "progress.bin").await.unwrap();

        let group = stream_group_async(&session).await;
        let mut stream = group.download_stream(file_info, None).await.unwrap();
        let initial = stream.get_progress();
        assert_eq!(initial.total_bytes, original.len() as u64);
        assert_eq!(initial.bytes_completed, 0);

        let mut collected = Vec::new();
        while let Some(chunk) = stream.next().await.unwrap() {
            collected.extend_from_slice(&chunk);
        }
        assert_eq!(collected, original);

        let final_progress = stream.get_progress();
        assert_eq!(final_progress.total_bytes, original.len() as u64);
        assert_eq!(final_progress.bytes_completed, original.len() as u64);
    }

    #[test]
    // get_progress() works correctly in blocking mode.
    fn test_download_stream_blocking_progress_reports_completion() {
        let temp = tempdir().unwrap();
        let session = local_session_sync(&temp).unwrap();
        let original = b"blocking progress tracking test data";
        let file_info = upload_bytes_blocking(&session, original, "progress.bin").unwrap();

        let group = stream_group_sync(&session);
        let mut stream = group.download_stream_blocking(file_info, None).unwrap();

        let mut collected = Vec::new();
        while let Some(chunk) = stream.blocking_next().unwrap() {
            collected.extend_from_slice(&chunk);
        }
        assert_eq!(collected, original);

        let final_progress = stream.get_progress();
        assert_eq!(final_progress.total_bytes, original.len() as u64);
        assert_eq!(final_progress.bytes_completed, original.len() as u64);
    }

    #[tokio::test(flavor = "multi_thread")]
    // Multiple sequential streaming downloads use the same group.
    async fn test_download_stream_multiple_sequential() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let data_a = b"first stream payload";
        let data_b = b"second stream payload";
        let info_a = upload_bytes(&session, data_a, "a.bin").await.unwrap();
        let info_b = upload_bytes(&session, data_b, "b.bin").await.unwrap();

        let group = stream_group_async(&session).await;
        let mut stream_a = group.download_stream(info_a, None).await.unwrap();
        let mut collected_a = Vec::new();
        while let Some(chunk) = stream_a.next().await.unwrap() {
            collected_a.extend_from_slice(&chunk);
        }
        assert_eq!(collected_a, data_a);

        let mut stream_b = group.download_stream(info_b, None).await.unwrap();
        let mut collected_b = Vec::new();
        while let Some(chunk) = stream_b.next().await.unwrap() {
            collected_b.extend_from_slice(&chunk);
        }
        assert_eq!(collected_b, data_b);
    }
}
