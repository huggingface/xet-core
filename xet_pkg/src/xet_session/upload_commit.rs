//! UploadCommit — groups related uploads

use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use xet_data::deduplication::DeduplicationMetrics;
use xet_data::processing::{FileUploadCoordinator, Sha256Policy, XetFileInfo};
use xet_data::progress_tracking::{GroupProgressReport, UniqueID};

use super::common::create_translator_config;
use super::session::XetSession;
use super::task_runtime::TaskRuntime;
use super::upload_file_handle::{TrackedFileUpload, UploadFileHandle, UploadFileHandleInner};
use super::upload_stream_handle::{UploadStreamHandle, UploadStreamHandleInner};
use crate::error::XetError;

// ── Data types ──────────────────────────────────────────────────────────────

/// Report returned by [`UploadCommit::commit`].
///
/// Contains aggregate deduplication metrics, final progress, and per-file
/// [`FileMetadata`] for every file that was successfully ingested.
#[derive(Clone, Debug)]
pub struct CommitReport {
    /// Aggregate deduplication metrics across all files in this commit.
    pub dedup_metrics: DeduplicationMetrics,
    /// Final progress snapshot at the time the commit completed.
    pub progress: GroupProgressReport,
    /// Per-file metadata, one entry per successfully ingested file.
    pub files: Vec<FileMetadata>,
}

/// Per-file metadata returned by [`UploadFileHandle::finish`] and
/// [`UploadStreamHandle::finish`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FileMetadata {
    /// Xet file information: hash, size, and optional SHA-256.
    pub xet_info: XetFileInfo,
    /// Per-file deduplication and chunking metrics.
    pub dedup_metrics: DeduplicationMetrics,
    /// Original file name or designated tracking name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracking_name: Option<String>,
}

// ── UploadCommitInner ───────────────────────────────────────────────────────

pub(super) struct UploadCommitInner {
    commit_id: UniqueID,
    pub(super) session: XetSession,
    pub(super) task_runtime: Arc<TaskRuntime>,
    upload_coordinator: Arc<FileUploadCoordinator>,
    pub(super) file_handles: Mutex<Vec<TrackedFileUpload>>,
    stream_handles: Mutex<Vec<UploadStreamHandle>>,
}

impl UploadCommitInner {
    async fn new(session: XetSession, task_runtime: Arc<TaskRuntime>) -> Result<Self, XetError> {
        let commit_id = UniqueID::new();
        let config = create_translator_config(&session)?;
        let upload_coordinator = FileUploadCoordinator::new(Arc::new(config)).await?;

        Ok(Self {
            commit_id,
            session,
            task_runtime,
            upload_coordinator,
            file_handles: Mutex::new(Vec::new()),
            stream_handles: Mutex::new(Vec::new()),
        })
    }

    async fn upload_from_path(
        self: &Arc<Self>,
        file_path: PathBuf,
        sha256: Sha256Policy,
    ) -> Result<UploadFileHandle, XetError> {
        let absolute_path = std::path::absolute(file_path)?;
        let tracking_name = absolute_path.to_str().map(|s| s.to_owned());
        let (task_id, join_handle) = self.upload_coordinator.spawn_upload_from_path(absolute_path, sha256).await?;

        self.register_spawned_task(task_id, join_handle, tracking_name)
    }

    async fn upload_stream(
        self: &Arc<Self>,
        tracking_name: Option<String>,
        sha256: Sha256Policy,
    ) -> Result<UploadStreamHandle, XetError> {
        let name: Option<Arc<str>> = tracking_name.as_deref().map(Arc::from);
        let (task_id, cleaner) = self.upload_coordinator.start_clean(name, None, sha256)?;
        let task_runtime = self.task_runtime.child()?;

        let handle = UploadStreamHandle {
            inner: Arc::new(UploadStreamHandleInner {
                task_id,
                result: Arc::new(OnceLock::new()),
                cleaner: Arc::new(tokio::sync::Mutex::new(Some((cleaner, tracking_name)))),
                upload_coordinator: self.upload_coordinator.clone(),
                task_runtime: task_runtime.clone(),
            }),
            task_runtime,
        };

        self.stream_handles
            .lock()
            .expect("stream_handles lock poisoned")
            .push(handle.clone());

        Ok(handle)
    }

    async fn upload_bytes(
        self: &Arc<Self>,
        bytes: Vec<u8>,
        sha256: Sha256Policy,
        tracking_name: Option<String>,
    ) -> Result<UploadFileHandle, XetError> {
        let name: Option<Arc<str>> = tracking_name.as_deref().map(Arc::from);
        let (task_id, join_handle) = self.upload_coordinator.spawn_upload_bytes(bytes, sha256, name).await?;

        self.register_spawned_task(task_id, join_handle, tracking_name)
    }

    fn register_spawned_task(
        &self,
        task_id: UniqueID,
        join_handle: tokio::task::JoinHandle<Result<(XetFileInfo, DeduplicationMetrics), xet_data::DataError>>,
        tracking_name: Option<String>,
    ) -> Result<UploadFileHandle, XetError> {
        let task_runtime = self.task_runtime.child()?;
        let abort_handle = join_handle.abort_handle();
        let result = Arc::new(OnceLock::new());
        let join_handle = Arc::new(tokio::sync::Mutex::new(Some(join_handle)));

        self.file_handles
            .lock()
            .expect("file_handles lock poisoned")
            .push(TrackedFileUpload {
                result: result.clone(),
                join_handle: join_handle.clone(),
                tracking_name: tracking_name.clone(),
                abort_handle,
                task_runtime: task_runtime.clone(),
            });

        Ok(UploadFileHandle {
            inner: Arc::new(UploadFileHandleInner {
                task_id,
                result,
                join_handle,
                tracking_name,
                upload_coordinator: self.upload_coordinator.clone(),
            }),
            task_runtime,
        })
    }

    fn get_progress(self: &Arc<Self>) -> GroupProgressReport {
        self.upload_coordinator.report()
    }

    async fn commit(self: &Arc<Self>) -> Result<CommitReport, XetError> {
        let stream_uploads = std::mem::take(&mut *self.stream_handles.lock()?);
        let file_uploads = std::mem::take(&mut *self.file_handles.lock()?);

        let mut files = Vec::with_capacity(file_uploads.len() + stream_uploads.len());

        for stream in &stream_uploads {
            match stream.try_finish() {
                Some(Ok(meta)) => files.push(meta),
                Some(Err(e)) => return Err(e),
                None => return Err(XetError::other("stream upload not finished before commit")),
            }
        }

        let mut first_error = None;
        for upload in &file_uploads {
            match upload.finish().await {
                Ok(meta) => files.push(meta),
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                },
            }
        }

        let finalize_result = self.upload_coordinator.clone().finalize_with_report().await;
        self.session.finish_upload_commit(self.commit_id)?;
        let (dedup_metrics, progress) = match finalize_result {
            Ok(v) => v,
            Err(e) => {
                let err: XetError = e.into();
                self.task_runtime.mark_runtime_finished_err(err.clone())?;
                return Err(err);
            },
        };
        if let Some(e) = first_error {
            self.task_runtime.mark_runtime_finished_err(e.clone())?;
            return Err(e);
        }

        self.task_runtime.mark_runtime_finished_ok()?;

        Ok(CommitReport {
            dedup_metrics,
            progress,
            files,
        })
    }

    fn abort(&self) -> Result<(), XetError> {
        self.task_runtime.cancel_subtree()?;
        let file_uploads = std::mem::take(&mut *self.file_handles.lock()?);
        for upload in file_uploads {
            upload.abort_handle.abort();
        }
        let stream_uploads = std::mem::take(&mut *self.stream_handles.lock()?);
        for stream in stream_uploads {
            stream.abort();
        }
        Ok(())
    }
}

// ── UploadCommit (public wrapper) ───────────────────────────────────────────

/// API for grouping related file uploads into a single atomic commit.
///
/// Obtain via [`XetSession::new_upload_commit`] (async) or
/// [`XetSession::new_upload_commit_blocking`] (sync).
///
/// Enqueue files with [`upload_from_path`](Self::upload_from_path) /
/// [`upload_from_path_blocking`](Self::upload_from_path_blocking), stream bytes
/// with [`upload_stream`](Self::upload_stream) /
/// [`upload_stream_blocking`](Self::upload_stream_blocking), or upload raw bytes
/// with [`upload_bytes`](Self::upload_bytes) /
/// [`upload_bytes_blocking`](Self::upload_bytes_blocking) — transfers start
/// immediately in the background.
///
/// Poll progress with [`get_progress`](Self::get_progress), then call
/// [`commit`](Self::commit) (async) or [`commit_blocking`](Self::commit_blocking)
/// to wait for all uploads to finish and push metadata to the CAS server.
///
/// Per-file results are available via [`UploadFileHandle::finish`] or
/// [`UploadStreamHandle::finish`] at any time after ingestion completes —
/// you do not need to wait for [`commit`](Self::commit).
///
/// This type is cheaply clonable; all clones share the same underlying state.
///
/// # Errors
///
/// Methods return [`XetError::Aborted`] if the parent session has been
/// aborted, and [`XetError::AlreadyCommitted`] if [`commit`](Self::commit)
/// has already been called.
#[derive(Clone)]
pub struct UploadCommit {
    pub(super) inner: Arc<UploadCommitInner>,
    pub(super) task_runtime: Arc<TaskRuntime>,
}

impl UploadCommit {
    pub(super) async fn new(session: XetSession, task_runtime: Arc<TaskRuntime>) -> Result<Self, XetError> {
        let inner = UploadCommitInner::new(session, task_runtime.clone()).await?;
        Ok(Self {
            inner: Arc::new(inner),
            task_runtime,
        })
    }

    pub(super) fn id(&self) -> UniqueID {
        self.inner.commit_id
    }

    // ===== Async public API =====

    /// Queue a file for upload from a path on disk.
    ///
    /// The file is read in a background task.  Returns an [`UploadFileHandle`]
    /// whose [`finish`](UploadFileHandle::finish) method yields per-file
    /// [`FileMetadata`] once ingestion completes.
    ///
    /// # Parameters
    ///
    /// - `file_path`: path to the file. Resolved to an absolute path so the upload is unaffected by later
    ///   working-directory changes.
    /// - `sha256`: SHA-256 handling policy for this file.
    pub async fn upload_from_path(
        &self,
        file_path: PathBuf,
        sha256: Sha256Policy,
    ) -> Result<UploadFileHandle, XetError> {
        let inner = Arc::clone(&self.inner);
        self.task_runtime
            .bridge_async_runtime_checked(
                "upload_from_path",
                XetError::AlreadyCommitted,
                "Upload commit cancelled",
                async move { inner.upload_from_path(file_path, sha256).await },
            )
            .await
    }

    /// Begin an incremental streaming upload.
    ///
    /// Returns an [`UploadStreamHandle`] for writing data in chunks.  Call
    /// [`write`](UploadStreamHandle::write) to feed bytes, then
    /// [`finish`](UploadStreamHandle::finish) to finalise ingestion.
    /// **`finish` must be called before [`commit`](Self::commit).**
    ///
    /// ```rust,no_run
    /// # use std::fs::File;
    /// # use std::io::Read;
    /// # use bytes::Bytes;
    /// # use xet::XetError;
    /// # async fn example(commit: xet::xet_session::UploadCommit, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    /// # use xet::xet_session::Sha256Policy;
    /// let handle = commit.upload_stream(Some(filename.into()), Sha256Policy::Compute).await?;
    /// let mut reader = File::open(&filename)?;
    /// let mut buffer = vec![0u8; 65536];
    /// loop {
    ///     let bytes = reader.read(&mut buffer)?;
    ///     if bytes == 0 { break; }
    ///     handle.write(Bytes::copy_from_slice(&buffer[..bytes])).await?;
    /// }
    /// let meta = handle.finish().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Parameters
    ///
    /// - `tracking_name`: optional display name for progress and [`FileMetadata::tracking_name`].
    /// - `sha256`: SHA-256 handling policy for this file.
    pub async fn upload_stream(
        &self,
        tracking_name: Option<String>,
        sha256: Sha256Policy,
    ) -> Result<UploadStreamHandle, XetError> {
        let inner = Arc::clone(&self.inner);
        self.task_runtime
            .bridge_async_runtime_checked(
                "upload_stream",
                XetError::AlreadyCommitted,
                "Upload commit cancelled",
                async move { inner.upload_stream(tracking_name, sha256).await },
            )
            .await
    }

    /// Queue raw bytes for upload, starting the transfer immediately.
    ///
    /// Returns an [`UploadFileHandle`] whose [`finish`](UploadFileHandle::finish)
    /// method yields per-file [`FileMetadata`] once ingestion completes.
    ///
    /// # Parameters
    ///
    /// - `bytes`: raw byte content to upload.
    /// - `sha256`: SHA-256 handling policy for this file.
    /// - `tracking_name`: optional display name for progress and telemetry.
    pub async fn upload_bytes(
        &self,
        bytes: Vec<u8>,
        sha256: Sha256Policy,
        tracking_name: Option<String>,
    ) -> Result<UploadFileHandle, XetError> {
        let inner = Arc::clone(&self.inner);
        self.task_runtime
            .bridge_async_runtime_checked(
                "upload_bytes",
                XetError::AlreadyCommitted,
                "Upload commit cancelled",
                async move { inner.upload_bytes(bytes, sha256, tracking_name).await },
            )
            .await
    }

    /// Return a snapshot of aggregate progress for every queued upload.
    pub fn get_progress(&self) -> GroupProgressReport {
        self.inner.get_progress()
    }

    /// Wait for all uploads to complete and push metadata to the CAS server.
    ///
    /// Returns a [`CommitReport`] with aggregate dedup metrics, progress,
    /// and per-file [`FileMetadata`].  Subsequent calls return
    /// [`XetError::AlreadyCommitted`].
    pub async fn commit(&self) -> Result<CommitReport, XetError> {
        let inner = Arc::clone(&self.inner);
        self.task_runtime
            .bridge_async_runtime_checked("commit", XetError::AlreadyCommitted, "Upload commit cancelled", async move {
                inner.commit().await
            })
            .await
    }

    /// Returns `true` if [`commit`](Self::commit) has completed.
    #[cfg(test)]
    async fn is_committed(&self) -> bool {
        matches!(self.inner.task_runtime.state(), Ok(super::task_runtime::TaskState::Finished(Ok(()))))
    }

    // ===== Blocking (sync) variants =====

    /// Blocking version of [`upload_from_path`](Self::upload_from_path).
    ///
    /// Reads a file from disk in a background task and returns an
    /// [`UploadFileHandle`] for tracking the result.
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn upload_from_path_blocking(
        &self,
        file_path: PathBuf,
        sha256: Sha256Policy,
    ) -> Result<UploadFileHandle, XetError> {
        let inner = Arc::clone(&self.inner);
        self.task_runtime.bridge_sync_runtime_checked(
            "upload_from_path_blocking",
            XetError::AlreadyCommitted,
            "Upload commit cancelled",
            async move { inner.upload_from_path(file_path, sha256).await },
        )
    }

    /// Blocking version of [`upload_stream`](Self::upload_stream).
    ///
    /// Returns an [`UploadStreamHandle`] for incrementally writing data.
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn upload_stream_blocking(
        &self,
        tracking_name: Option<String>,
        sha256: Sha256Policy,
    ) -> Result<UploadStreamHandle, XetError> {
        let inner = Arc::clone(&self.inner);
        self.task_runtime.bridge_sync_runtime_checked(
            "upload_stream_blocking",
            XetError::AlreadyCommitted,
            "Upload commit cancelled",
            async move { inner.upload_stream(tracking_name, sha256).await },
        )
    }

    /// Blocking version of [`upload_bytes`](Self::upload_bytes).
    ///
    /// Queues raw bytes and returns an [`UploadFileHandle`].
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn upload_bytes_blocking(
        &self,
        bytes: Vec<u8>,
        sha256: Sha256Policy,
        tracking_name: Option<String>,
    ) -> Result<UploadFileHandle, XetError> {
        let inner = Arc::clone(&self.inner);
        self.task_runtime.bridge_sync_runtime_checked(
            "upload_bytes_blocking",
            XetError::AlreadyCommitted,
            "Upload commit cancelled",
            async move { inner.upload_bytes(bytes, sha256, tracking_name).await },
        )
    }

    /// Blocking version of [`get_progress`](Self::get_progress).
    pub fn get_progress_blocking(&self) -> GroupProgressReport {
        self.inner.get_progress()
    }

    /// Blocking version of [`commit`](Self::commit).
    ///
    /// Waits for all uploads to complete and pushes metadata to the CAS
    /// server.  Returns a [`CommitReport`].
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn commit_blocking(&self) -> Result<CommitReport, XetError> {
        let inner = Arc::clone(&self.inner);
        self.task_runtime.bridge_sync_runtime_checked(
            "commit_blocking",
            XetError::AlreadyCommitted,
            "Upload commit cancelled",
            async move { inner.commit().await },
        )
    }

    /// Cancel all tracked upload tasks.
    pub(super) fn abort(&self) -> Result<(), XetError> {
        self.inner.abort()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::time::Duration;

    use tempfile::{TempDir, tempdir};
    use xet_runtime::core::RuntimeMode;

    use super::*;
    use crate::xet_session::session::{XetSession, XetSessionBuilder};

    async fn local_session(temp: &TempDir) -> Result<XetSession, Box<dyn std::error::Error>> {
        let cas_path = temp.path().join("cas");
        Ok(XetSessionBuilder::new()
            .with_endpoint(format!("local://{}", cas_path.display()))
            .build_async()
            .await?)
    }

    // ── Mutex guard / concurrency test ───────────────────────────────────────

    #[test]
    fn test_commit_blocked_while_upload_registration_holds_state_lock() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let cas_path = temp.path().join("cas");
        let session = XetSessionBuilder::new()
            .with_endpoint(format!("local://{}", cas_path.display()))
            .build()?;
        let runtime = session.runtime.clone();
        let commit = session.new_upload_commit_blocking()?;
        let commit_for_thread = commit.clone();
        let runtime_for_thread = runtime.clone();

        let guard = commit.inner.file_handles.lock().unwrap();

        let (done_tx, done_rx) = mpsc::channel::<()>();
        let join_handle = std::thread::spawn(move || {
            let _ = runtime_for_thread.external_run_async_task(async move { commit_for_thread.commit().await });
            let _ = done_tx.send(());
        });

        std::thread::sleep(Duration::from_millis(50));
        assert!(done_rx.try_recv().is_err());

        drop(guard);

        assert!(done_rx.recv_timeout(Duration::from_secs(5)).is_ok());
        let _ = join_handle.join();
        Ok(())
    }

    // ── Identity ─────────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_commit_has_unique_id() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let c1 = session.new_upload_commit().await.unwrap();
        let c2 = session.new_upload_commit().await.unwrap();
        assert_ne!(c1.id(), c2.id());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_commit_clone_shares_id() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let commit2 = commit.clone();
        assert_eq!(commit.id(), commit2.id());
    }

    // ── Initial state ────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_progress_empty_initially() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let report = commit.get_progress();
        assert_eq!(report.total_bytes, 0);
        assert_eq!(report.total_bytes_completed, 0);
    }

    // ── Commit lifecycle ─────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_commit_empty_succeeds() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        session.new_upload_commit().await.unwrap().commit().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_commit_marks_as_committed() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let commit_clone = commit.clone();
        commit.commit().await.unwrap();
        assert!(commit_clone.is_committed().await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_second_commit_fails() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let c1 = session.new_upload_commit().await.unwrap();
        let c2 = c1.clone();
        c1.commit().await.unwrap();
        let err = c2.commit().await.unwrap_err();
        assert!(matches!(err, XetError::AlreadyCommitted | XetError::Internal(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_commit_unregisters_from_session() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
        commit.commit().await.unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 0);
    }

    // ── Session-abort guards ─────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_file_on_aborted_session_returns_error() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        session.abort().unwrap();
        let err = commit
            .upload_from_path(PathBuf::from("nonexistent.bin"), Sha256Policy::Compute)
            .await
            .unwrap_err();
        assert!(matches!(err, XetError::Cancelled(_) | XetError::Aborted));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_bytes_on_aborted_session_returns_error() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        session.abort().unwrap();
        let err = commit
            .upload_bytes(b"data".to_vec(), Sha256Policy::Compute, Some("bytes 1".into()))
            .await
            .unwrap_err();
        assert!(matches!(err, XetError::Cancelled(_) | XetError::Aborted));
    }

    // ── Post-commit guards (AlreadyCommitted) ────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_from_path_after_commit_fails() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let c1 = session.new_upload_commit().await.unwrap();
        let c2 = c1.clone();
        c1.commit().await.unwrap();
        let err = c2
            .upload_from_path(PathBuf::from("any.bin"), Sha256Policy::Compute)
            .await
            .unwrap_err();
        assert!(matches!(err, XetError::AlreadyCommitted));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_bytes_after_commit_fails() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let c1 = session.new_upload_commit().await.unwrap();
        let c2 = c1.clone();
        c1.commit().await.unwrap();
        let err = c2
            .upload_bytes(b"hello".to_vec(), Sha256Policy::Compute, None)
            .await
            .unwrap_err();
        assert!(matches!(err, XetError::AlreadyCommitted));
    }

    // ── API coverage & abort ─────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_file_returns_stream_handle() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit
            .upload_stream(Some("stream.bin".into()), Sha256Policy::Compute)
            .await
            .unwrap();
        assert!(handle.try_finish().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_abort_cancels_tracked_uploads() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit
            .upload_bytes(b"data".to_vec(), Sha256Policy::Compute, None)
            .await
            .unwrap();
        commit.abort().unwrap();
        assert!(commit.inner.file_handles.lock().unwrap().is_empty());
        let result = handle.finish().await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_abort_while_state_lock_held_still_drains_tasks() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let _handle = commit
            .upload_bytes(b"data".to_vec(), Sha256Policy::Compute, None)
            .await
            .unwrap();

        commit.abort().unwrap();
        assert!(matches!(
            commit.inner.task_runtime.state().unwrap(),
            crate::xet_session::task_runtime::TaskState::Finished(Err(XetError::Aborted))
        ));

        assert!(commit.inner.file_handles.lock().unwrap().is_empty());
    }

    // ── Independence ─────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_two_commits_are_independent() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let c1 = session.new_upload_commit().await.unwrap();
        let c2 = session.new_upload_commit().await.unwrap();
        c1.commit().await.unwrap();
        assert!(!c2.is_committed().await);
    }

    // ── Round-trip tests ─────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_bytes_round_trip() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let data = b"Hello, upload commit round-trip!";
        let commit = session.new_upload_commit().await.unwrap();
        let task_handle = commit
            .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("hello.bin".into()))
            .await
            .unwrap();

        commit.commit().await.unwrap();

        let meta = task_handle.finish().await.unwrap();
        assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        assert!(!meta.xet_info.hash.is_empty());
        assert!(meta.xet_info.sha256.is_some());
        assert_eq!(meta.xet_info.sha256.as_deref().unwrap().len(), 64);
        assert_eq!(meta.tracking_name.as_deref(), Some("hello.bin"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_bytes_task_id_matches_progress() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();

        let handle = commit
            .upload_bytes(b"id-match".to_vec(), Sha256Policy::Compute, Some("id.bin".into()))
            .await
            .unwrap();

        for _ in 0..50 {
            if handle.get_progress().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(handle.get_progress().is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_from_path_round_trip() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let src = temp.path().join("data.bin");
        let data = b"file path upload content";
        std::fs::write(&src, data).unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit.upload_from_path(src, Sha256Policy::Compute).await.unwrap();
        commit.commit().await.unwrap();
        let meta = handle.finish().await.unwrap();
        assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        assert!(!meta.xet_info.hash.is_empty());
        assert!(meta.xet_info.sha256.is_some());
        assert_eq!(meta.xet_info.sha256.as_deref().unwrap().len(), 64);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_bytes_sha256_policy_metadata() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let provided_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();

        let compute_handle = commit
            .upload_bytes(b"compute".to_vec(), Sha256Policy::Compute, Some("compute.bin".into()))
            .await
            .unwrap();
        let provided_handle = commit
            .upload_bytes(b"provided".to_vec(), Sha256Policy::from_hex(&provided_sha256), Some("provided.bin".into()))
            .await
            .unwrap();
        let skip_handle = commit
            .upload_bytes(b"skip".to_vec(), Sha256Policy::Skip, Some("skip.bin".into()))
            .await
            .unwrap();

        commit.commit().await.unwrap();

        let compute_meta = compute_handle.finish().await.unwrap();
        assert!(compute_meta.xet_info.sha256.is_some());
        assert_eq!(compute_meta.xet_info.sha256.as_deref().unwrap().len(), 64);

        let provided_meta = provided_handle.finish().await.unwrap();
        assert_eq!(provided_meta.xet_info.sha256.as_deref(), Some(provided_sha256.as_str()));

        let skip_meta = skip_handle.finish().await.unwrap();
        assert!(skip_meta.xet_info.sha256.is_none());
    }

    // ── finish() before and after commit ─────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_finish_returns_result_before_commit() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let data = b"finish before commit";
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit
            .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("early.bin".into()))
            .await
            .unwrap();
        let meta = handle.finish().await.unwrap();
        assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        commit.commit().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_finish_is_idempotent() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit
            .upload_bytes(b"idem".to_vec(), Sha256Policy::Compute, None)
            .await
            .unwrap();
        let meta1 = handle.finish().await.unwrap();
        let meta2 = handle.finish().await.unwrap();
        assert_eq!(meta1.xet_info.hash, meta2.xet_info.hash);
        commit.commit().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_finish_includes_dedup_metrics() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let data = b"dedup metrics check";
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit.upload_bytes(data.to_vec(), Sha256Policy::Compute, None).await.unwrap();
        let meta = handle.finish().await.unwrap();
        assert_eq!(meta.dedup_metrics.total_bytes, data.len() as u64);
        commit.commit().await.unwrap();
    }

    // ── Streaming upload ─────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_streaming_round_trip() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let data = b"streamed upload bytes";
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit
            .upload_stream(Some("stream.bin".into()), Sha256Policy::Compute)
            .await
            .unwrap();
        handle.write(&data[..]).await.unwrap();
        let meta = handle.finish().await.unwrap();
        assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        assert!(!meta.xet_info.hash.is_empty());
        assert_eq!(meta.tracking_name.as_deref(), Some("stream.bin"));
        commit.commit().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_commit_errors_when_stream_not_finished() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit
            .upload_stream(Some("unfinished.bin".into()), Sha256Policy::Compute)
            .await
            .unwrap();
        handle.write(&b"partial data"[..]).await.unwrap();
        let err = commit.commit().await.unwrap_err();
        assert!(matches!(err, XetError::Internal(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_finish_is_idempotent() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit
            .upload_stream(Some("idem.bin".into()), Sha256Policy::Compute)
            .await
            .unwrap();
        handle.write(&b"idem"[..]).await.unwrap();
        let meta1 = handle.finish().await.unwrap();
        let meta2 = handle.finish().await.unwrap();
        assert_eq!(meta1.xet_info.hash, meta2.xet_info.hash);
        commit.commit().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_write_after_finish_errors() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit
            .upload_stream(Some("done.bin".into()), Sha256Policy::Compute)
            .await
            .unwrap();
        handle.write(&b"done"[..]).await.unwrap();
        handle.finish().await.unwrap();
        let err = handle.write(&b"more"[..]).await.unwrap_err();
        assert!(matches!(err, XetError::Internal(_)));
        commit.commit().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_abort_prevents_further_writes() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit
            .upload_stream(Some("abort.bin".into()), Sha256Policy::Compute)
            .await
            .unwrap();
        handle.abort();
        let err = handle.write(&b"data"[..]).await.unwrap_err();
        assert!(matches!(err, XetError::Cancelled(_) | XetError::Aborted));
        let result = handle.finish().await;
        assert!(result.is_err());
    }

    // ── Multiple files ───────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_multiple_files_in_one_commit() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let h1 = commit
            .upload_bytes(b"file one".to_vec(), Sha256Policy::Compute, Some("a.bin".into()))
            .await
            .unwrap();
        let h2 = commit
            .upload_bytes(b"file two".to_vec(), Sha256Policy::Compute, Some("b.bin".into()))
            .await
            .unwrap();
        let h3 = commit
            .upload_bytes(b"file three".to_vec(), Sha256Policy::Compute, Some("c.bin".into()))
            .await
            .unwrap();
        commit.commit().await.unwrap();
        assert!(h1.finish().await.is_ok());
        assert!(h2.finish().await.is_ok());
        assert!(h3.finish().await.is_ok());
    }

    // ── Progress ─────────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_progress_reflects_bytes_after_commit() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let data = b"progress tracking upload data";
        let commit = session.new_upload_commit().await.unwrap();
        let progress_observer = commit.clone();
        commit
            .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("prog.bin".into()))
            .await
            .unwrap();
        commit.commit().await.unwrap();
        let report = progress_observer.get_progress();
        assert_eq!(report.total_bytes, data.len() as u64);
        assert_eq!(report.total_bytes_completed, data.len() as u64);
        assert_eq!(report.total_transfer_bytes, report.total_transfer_bytes_completed);
        assert!(report.total_transfer_bytes_completed <= data.len() as u64);
    }

    // ── Non-tokio executor (Owned-mode bridge) ────────────────────────────────

    #[test]
    fn test_async_bridge_works_from_futures_executor() {
        let temp = tempdir().unwrap();

        futures::executor::block_on(async {
            let session = local_session(&temp).await.unwrap();
            assert_eq!(session.runtime_mode, RuntimeMode::Owned);

            let data = b"hello from non-tokio executor";
            let commit = session.new_upload_commit().await.unwrap();
            let handle = commit
                .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
                .await
                .unwrap();
            commit.commit().await.unwrap();

            let meta = handle.finish().await.unwrap();
            assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        });
    }

    #[test]
    fn test_async_bridge_works_from_smol_executor() {
        let temp = tempdir().unwrap();

        smol::block_on(async {
            let session = local_session(&temp).await.unwrap();
            assert_eq!(session.runtime_mode, RuntimeMode::Owned);

            let data = b"hello from smol executor";
            let commit = session.new_upload_commit().await.unwrap();
            let handle = commit
                .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
                .await
                .unwrap();
            commit.commit().await.unwrap();

            let meta = handle.finish().await.unwrap();
            assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        });
    }

    #[test]
    fn test_async_bridge_works_from_async_std_executor() {
        let temp = tempdir().unwrap();

        async_std::task::block_on(async {
            let session = local_session(&temp).await.unwrap();
            assert_eq!(session.runtime_mode, RuntimeMode::Owned);

            let data = b"hello from async-std executor";
            let commit = session.new_upload_commit().await.unwrap();
            let handle = commit
                .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
                .await
                .unwrap();
            commit.commit().await.unwrap();

            let meta = handle.finish().await.unwrap();
            assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        });
    }

    // ── Blocking API tests ────────────────────────────────────────────────────

    fn local_session_sync(temp: &TempDir) -> Result<XetSession, Box<dyn std::error::Error>> {
        let cas_path = temp.path().join("cas");
        Ok(XetSessionBuilder::new()
            .with_endpoint(format!("local://{}", cas_path.display()))
            .build()?)
    }

    #[test]
    fn test_blocking_upload_bytes_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session_sync(&temp)?;
        let data = b"Hello, upload commit round-trip!";
        let commit = session.new_upload_commit_blocking()?;
        let task_handle =
            commit.upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some("hello.bin".into()))?;
        commit.commit_blocking()?;
        let meta = task_handle.finish_blocking()?;
        assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        assert!(!meta.xet_info.hash.is_empty());
        Ok(())
    }

    #[test]
    fn test_blocking_upload_from_path_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session_sync(&temp)?;
        let src = temp.path().join("data.bin");
        let data = b"file path upload content";
        std::fs::write(&src, data)?;
        let commit = session.new_upload_commit_blocking()?;
        let handle = commit.upload_from_path_blocking(src, Sha256Policy::Compute)?;
        commit.commit_blocking()?;
        let meta = handle.finish_blocking()?;
        assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        assert!(!meta.xet_info.hash.is_empty());
        Ok(())
    }

    #[test]
    fn test_blocking_finish_before_commit() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session_sync(&temp)?;
        let data = b"result access patterns";
        let commit = session.new_upload_commit_blocking()?;
        let handle = commit.upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, None)?;

        let meta = handle.finish_blocking()?;
        assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));

        commit.commit_blocking()?;
        Ok(())
    }

    #[test]
    fn test_blocking_upload_streaming_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session_sync(&temp)?;
        let data = b"streamed upload bytes";
        let commit = session.new_upload_commit_blocking()?;
        let handle = commit.upload_stream_blocking(Some("stream.bin".into()), Sha256Policy::Compute)?;
        handle.write_blocking(&data[..])?;
        let meta = handle.finish_blocking()?;
        assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        assert!(!meta.xet_info.hash.is_empty());
        assert_eq!(meta.tracking_name.as_deref(), Some("stream.bin"));
        commit.commit_blocking()?;
        Ok(())
    }

    #[test]
    fn test_blocking_upload_multiple_files_in_one_commit() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session_sync(&temp)?;
        let commit = session.new_upload_commit_blocking()?;
        let h1 = commit.upload_bytes_blocking(b"file one".to_vec(), Sha256Policy::Compute, Some("a.bin".into()))?;
        let h2 = commit.upload_bytes_blocking(b"file two".to_vec(), Sha256Policy::Compute, Some("b.bin".into()))?;
        let h3 = commit.upload_bytes_blocking(b"file three".to_vec(), Sha256Policy::Compute, Some("c.bin".into()))?;
        commit.commit_blocking()?;
        assert!(h1.finish_blocking().is_ok());
        assert!(h2.finish_blocking().is_ok());
        assert!(h3.finish_blocking().is_ok());
        Ok(())
    }

    #[test]
    fn test_blocking_upload_progress_reflects_bytes_after_commit() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session_sync(&temp)?;
        let data = b"progress tracking upload data";
        let commit = session.new_upload_commit_blocking()?;
        let progress_observer = commit.clone();
        commit.upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some("prog.bin".into()))?;
        commit.commit_blocking()?;
        let snapshot = progress_observer.get_progress_blocking();
        assert_eq!(snapshot.total_bytes, data.len() as u64);
        assert_eq!(snapshot.total_bytes_completed, data.len() as u64);
        assert_eq!(snapshot.total_transfer_bytes, snapshot.total_transfer_bytes_completed);
        assert!(snapshot.total_transfer_bytes_completed <= data.len() as u64);
        Ok(())
    }

    #[test]
    fn test_blocking_upload_file_returns_stream_handle() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session_sync(&temp)?;
        let commit = session.new_upload_commit_blocking()?;
        let handle = commit.upload_stream_blocking(Some("stream.bin".into()), Sha256Policy::Compute)?;
        assert!(handle.try_finish().is_none());
        Ok(())
    }

    fn assert_blocking_upload_round_trip<R>(run: R)
    where
        R: FnOnce(std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>),
    {
        let temp = tempdir().unwrap();
        let session = local_session_sync(&temp).unwrap();

        run(Box::pin(async move {
            let data = b"upload from smol executor";
            let commit = session.new_upload_commit_blocking().unwrap();
            let handle = commit
                .upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
                .unwrap();
            commit.commit_blocking().unwrap();
            let meta = handle.finish_blocking().unwrap();
            assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
            assert!(!meta.xet_info.hash.is_empty());
        }));
    }

    #[test]
    fn test_blocking_upload_round_trip_in_smol() {
        assert_blocking_upload_round_trip(smol::block_on);
    }

    #[test]
    fn test_blocking_upload_round_trip_in_futures_executor() {
        assert_blocking_upload_round_trip(futures::executor::block_on);
    }

    #[test]
    fn test_blocking_upload_round_trip_in_async_std() {
        assert_blocking_upload_round_trip(async_std::task::block_on);
    }
}
