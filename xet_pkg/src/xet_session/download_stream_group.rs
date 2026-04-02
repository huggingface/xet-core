//! [`XetDownloadStreamGroup`] and [`AuthGroupBuilder`] — authenticated
//! streaming download group management.
//!
//! [`XetDownloadStreamGroup`] manages a shared CAS connection pool and auth token for one
//! or more concurrent streams.  Obtain one via
//! [`XetSession::new_download_stream_group`](super::session::XetSession::new_download_stream_group).
//!
//! Individual stream handles ([`XetDownloadStream`] and [`XetUnorderedDownloadStream`])
//! are defined in [`super::download_stream_handle`] and returned by
//! [`XetDownloadStreamGroup::download_stream`] /
//! [`XetDownloadStreamGroup::download_unordered_stream`].

use std::ops::Range;
use std::sync::Arc;

use tracing::info;
use xet_data::processing::{FileDownloadSession, XetFileInfo};
use xet_data::progress_tracking::UniqueID;

use super::auth_group_builder::{AuthGroupBuilder, AuthOptions};
use super::common::create_translator_config;
use super::session::XetSession;
use super::task_runtime::TaskRuntime;
use super::{XetDownloadStream, XetUnorderedDownloadStream};
use crate::error::XetError;

impl AuthGroupBuilder<XetDownloadStreamGroup> {
    /// Create the [`XetDownloadStreamGroup`] from an async context.
    pub async fn build(self) -> Result<XetDownloadStreamGroup, XetError> {
        let AuthGroupBuilder {
            session, auth_options, ..
        } = self;
        let session_for_reg = session.clone();
        let parent_runtime = session.inner.task_runtime.clone();
        let child_parent = parent_runtime.clone();
        let group = parent_runtime
            .bridge_async("new_download_stream_group", async move {
                let group_runtime = child_parent.child()?;
                XetDownloadStreamGroup::new(session, group_runtime, auth_options).await
            })
            .await?;
        info!("New download stream group, session_id={}, group_id={}", group.session().id(), group.id());
        session_for_reg.register_download_stream_group(&group)?;
        Ok(group)
    }

    /// Create the [`XetDownloadStreamGroup`] from a sync context.
    ///
    /// # Errors
    ///
    /// Returns [`XetError::WrongRuntimeMode`] if the session wraps an external
    /// tokio runtime (created via [`XetSessionBuilder::with_tokio_handle`] or
    /// auto-detected inside `#[tokio::main]`).
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime on an Owned-mode session.
    pub fn build_blocking(self) -> Result<XetDownloadStreamGroup, XetError> {
        let AuthGroupBuilder {
            session, auth_options, ..
        } = self;
        let session_for_reg = session.clone();
        let parent_runtime = session.inner.task_runtime.clone();
        let child_parent = parent_runtime.clone();
        let group = parent_runtime.bridge_sync("new_download_stream_group_blocking", async move {
            let group_runtime = child_parent.child()?;
            XetDownloadStreamGroup::new(session, group_runtime, auth_options).await
        })?;
        info!("New download stream group, session_id={}, group_id={}", group.session().id(), group.id());
        session_for_reg.register_download_stream_group(&group)?;
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
pub struct XetDownloadStreamGroup {
    pub(super) inner: Arc<XetDownloadStreamGroupInner>,
    task_runtime: Arc<TaskRuntime>,
}

/// All shared state owned by a single [`XetDownloadStreamGroup`] instance.
/// Accessed through `Arc<XetDownloadStreamGroupInner>`; do not use this type directly.
#[doc(hidden)]
pub(super) struct XetDownloadStreamGroupInner {
    session: XetSession,
    group_id: UniqueID,
    download_session: Arc<FileDownloadSession>,
}

impl XetDownloadStreamGroupInner {
    pub(super) fn abort(&self) {
        self.download_session.abort_active_streams();
    }
}

impl XetDownloadStreamGroup {
    /// Create a new download stream group. Called by [`AuthGroupBuilder::build`].
    async fn new(
        session: XetSession,
        task_runtime: Arc<TaskRuntime>,
        auth_options: AuthOptions,
    ) -> Result<Self, XetError> {
        let group_id = UniqueID::new();
        let config = create_translator_config(&session, auth_options).await?;
        let download_session = FileDownloadSession::new(Arc::new(config), None).await?;

        let inner = Arc::new(XetDownloadStreamGroupInner {
            session,
            group_id,
            download_session,
        });

        Ok(Self { inner, task_runtime })
    }

    /// Returns the unique ID for this stream group.
    pub(super) fn id(&self) -> UniqueID {
        self.inner.group_id
    }

    fn session(&self) -> &XetSession {
        &self.inner.session
    }

    /// Create a [`XetDownloadStream`] for the given file, optionally
    /// restricted to a byte range.
    ///
    /// The returned stream yields data chunks as they are reconstructed,
    /// with built-in progress tracking via
    /// [`progress`](XetDownloadStream::progress).
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
    /// Returns `Err(XetError::UserCancelled)` if the session or this group has been aborted.
    pub async fn download_stream(
        &self,
        file_info: XetFileInfo,
        range: Option<Range<u64>>,
    ) -> Result<XetDownloadStream, XetError> {
        let group = self.clone();
        let download_session = self.inner.download_session.clone();
        self.task_runtime
            .bridge_async("download_stream", async move {
                let (id, stream) = download_session.download_stream(&file_info, range).await?;
                let stream_runtime = group.task_runtime.child()?;
                Ok(XetDownloadStream::new(stream, download_session, id, stream_runtime))
            })
            .await
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
    /// Returns [`XetError::WrongRuntimeMode`] if the session wraps an external
    /// tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime on an Owned-mode session.
    pub fn download_stream_blocking(
        &self,
        file_info: XetFileInfo,
        range: Option<Range<u64>>,
    ) -> Result<XetDownloadStream, XetError> {
        let group = self.clone();
        let download_session = self.inner.download_session.clone();
        self.task_runtime.bridge_sync("download_stream_blocking", async move {
            let (id, stream) = download_session.download_stream(&file_info, range).await?;
            let stream_runtime = group.task_runtime.child()?;
            Ok(XetDownloadStream::new(stream, download_session, id, stream_runtime))
        })
    }

    /// Create an [`XetUnorderedDownloadStream`] for the given file,
    /// optionally restricted to a byte range.
    ///
    /// The returned stream yields `(offset, Bytes)` chunks in whatever
    /// order they complete, with built-in progress tracking via
    /// [`progress`](XetUnorderedDownloadStream::progress).
    ///
    /// If `range` is `Some`, only the specified byte range of the file is
    /// reconstructed.
    ///
    /// Can be awaited from any async executor (tokio, smol, async-std, futures).
    ///
    /// Returns `Err(XetError::UserCancelled)` if the session or this group has been aborted.
    pub async fn download_unordered_stream(
        &self,
        file_info: XetFileInfo,
        range: Option<Range<u64>>,
    ) -> Result<XetUnorderedDownloadStream, XetError> {
        let group = self.clone();
        let download_session = self.inner.download_session.clone();
        self.task_runtime
            .bridge_async("download_unordered_stream", async move {
                let (id, stream) = download_session.download_unordered_stream(&file_info, range).await?;
                let stream_runtime = group.task_runtime.child()?;
                Ok(XetUnorderedDownloadStream::new(stream, download_session, id, stream_runtime))
            })
            .await
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
    /// Returns [`XetError::WrongRuntimeMode`] if the session wraps an external
    /// tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime on an Owned-mode session.
    pub fn download_unordered_stream_blocking(
        &self,
        file_info: XetFileInfo,
        range: Option<Range<u64>>,
    ) -> Result<XetUnorderedDownloadStream, XetError> {
        let group = self.clone();
        let download_session = self.inner.download_session.clone();
        self.task_runtime.bridge_sync("download_unordered_stream_blocking", async move {
            let (id, stream) = download_session.download_unordered_stream(&file_info, range).await?;
            let stream_runtime = group.task_runtime.child()?;
            Ok(XetUnorderedDownloadStream::new(stream, download_session, id, stream_runtime))
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use xet_data::processing::{Sha256Policy, XetFileInfo};

    use super::super::session::{XetSession, XetSessionBuilder};
    use super::*;

    async fn upload_bytes(
        session: &XetSession,
        endpoint: &str,
        data: &[u8],
        name: &str,
    ) -> Result<XetFileInfo, Box<dyn std::error::Error>> {
        let commit = session.new_upload_commit()?.with_endpoint(endpoint).build().await?;
        let _handle = commit
            .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some(name.into()))
            .await?;
        let results = commit.commit().await?;
        let meta = results.uploads.into_values().next().expect("one uploaded file");
        Ok(meta.xet_info)
    }

    fn upload_bytes_blocking(
        session: &XetSession,
        endpoint: &str,
        data: &[u8],
        name: &str,
    ) -> Result<XetFileInfo, Box<dyn std::error::Error>> {
        let commit = session.new_upload_commit()?.with_endpoint(endpoint).build_blocking()?;
        let _handle = commit.upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some(name.into()))?;
        let results = commit.commit_blocking()?;
        let meta = results.uploads.into_values().next().expect("one uploaded file");
        Ok(meta.xet_info)
    }

    async fn stream_group_async(session: &XetSession, endpoint: &str) -> XetDownloadStreamGroup {
        session
            .new_download_stream_group()
            .unwrap()
            .with_endpoint(endpoint)
            .build()
            .await
            .unwrap()
    }

    fn stream_group_sync(session: &XetSession, endpoint: &str) -> XetDownloadStreamGroup {
        session
            .new_download_stream_group()
            .unwrap()
            .with_endpoint(endpoint)
            .build_blocking()
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    // Async streaming download round-trip: upload, stream, verify content.
    async fn test_download_stream_round_trip() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let original = b"Hello, streaming download!";
        let file_info = upload_bytes(&session, &endpoint, original, "stream.bin").await.unwrap();

        let group = stream_group_async(&session, &endpoint).await;
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
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let original = b"Hello, blocking streaming download!";
        let file_info = upload_bytes_blocking(&session, &endpoint, original, "stream.bin").unwrap();

        let group = stream_group_sync(&session, &endpoint);
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
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let original = b"progress tracking test data for streaming";
        let file_info = upload_bytes(&session, &endpoint, original, "progress.bin").await.unwrap();

        let group = stream_group_async(&session, &endpoint).await;
        let mut stream = group.download_stream(file_info, None).await.unwrap();
        let initial = stream.progress();
        assert_eq!(initial.total_bytes, original.len() as u64);
        assert_eq!(initial.bytes_completed, 0);

        let mut collected = Vec::new();
        while let Some(chunk) = stream.next().await.unwrap() {
            collected.extend_from_slice(&chunk);
        }
        assert_eq!(collected, original);

        let final_progress = stream.progress();
        assert_eq!(final_progress.total_bytes, original.len() as u64);
        assert_eq!(final_progress.bytes_completed, original.len() as u64);
    }

    #[test]
    // get_progress() works correctly in blocking mode.
    fn test_download_stream_blocking_progress_reports_completion() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let original = b"blocking progress tracking test data";
        let file_info = upload_bytes_blocking(&session, &endpoint, original, "progress.bin").unwrap();

        let group = stream_group_sync(&session, &endpoint);
        let mut stream = group.download_stream_blocking(file_info, None).unwrap();

        let mut collected = Vec::new();
        while let Some(chunk) = stream.blocking_next().unwrap() {
            collected.extend_from_slice(&chunk);
        }
        assert_eq!(collected, original);

        let final_progress = stream.progress();
        assert_eq!(final_progress.total_bytes, original.len() as u64);
        assert_eq!(final_progress.bytes_completed, original.len() as u64);
    }

    #[tokio::test(flavor = "multi_thread")]
    // Multiple sequential streaming downloads use the same group.
    async fn test_download_stream_multiple_sequential() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let data_a = b"first stream payload";
        let data_b = b"second stream payload";
        let info_a = upload_bytes(&session, &endpoint, data_a, "a.bin").await.unwrap();
        let info_b = upload_bytes(&session, &endpoint, data_b, "b.bin").await.unwrap();

        let group = stream_group_async(&session, &endpoint).await;
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
