//! XetUploadCommit — groups related uploads

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use tracing::{error, info};
use xet_data::deduplication::DeduplicationMetrics;
use xet_data::processing::{FileUploadSession, Sha256Policy, XetFileInfo};
use xet_data::progress_tracking::{GroupProgressReport, UniqueID};

use super::auth_group_builder::{AuthGroupBuilder, AuthOptions};
use super::common::create_translator_config;
use super::session::XetSession;
use super::task_runtime::{BackgroundTaskState, TaskRuntime, XetTaskState};
use super::upload_file_handle::{XetFileUpload, XetFileUploadInner};
use super::upload_stream_handle::{XetStreamUpload, XetStreamUploadInner};
use crate::error::XetError;

pub type XetUploadCommitBuilder = AuthGroupBuilder<XetUploadCommit>;

impl AuthGroupBuilder<XetUploadCommit> {
    /// Create the [`XetUploadCommit`] from an async context.
    pub async fn build(self) -> Result<XetUploadCommit, XetError> {
        let AuthGroupBuilder {
            session, auth_options, ..
        } = self;
        let parent_runtime = session.inner.task_runtime.clone();
        let child_parent = parent_runtime.clone();
        let commit = parent_runtime
            .bridge_async("new_upload_commit", async move {
                let commit_runtime = child_parent.child()?;
                XetUploadCommit::new(session, commit_runtime, auth_options).await
            })
            .await?;
        info!("New upload commit, session_id={}, commit_id={}", commit.session().id(), commit.id());
        Ok(commit)
    }

    /// Create the [`XetUploadCommit`] from a sync context.
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
    pub fn build_blocking(self) -> Result<XetUploadCommit, XetError> {
        let AuthGroupBuilder {
            session, auth_options, ..
        } = self;
        let parent_runtime = session.inner.task_runtime.clone();
        let child_parent = parent_runtime.clone();
        let commit = parent_runtime.bridge_sync("new_upload_commit_blocking", async move {
            let commit_runtime = child_parent.child()?;
            XetUploadCommit::new(session, commit_runtime, auth_options).await
        })?;
        info!("New upload commit, session_id={}, commit_id={}", commit.session().id(), commit.id());
        Ok(commit)
    }
}

// ── Data types ──────────────────────────────────────────────────────────────

/// Report returned by [`XetUploadCommit::commit`].
///
/// Contains aggregate deduplication metrics, final progress, and per-file
/// [`XetFileMetadata`] for every file that was successfully ingested,
/// keyed by [`UniqueID`].
#[derive(Clone, Debug)]
pub struct XetCommitReport {
    /// Aggregate deduplication metrics across all files in this commit.
    pub dedup_metrics: DeduplicationMetrics,
    /// Final progress snapshot at the time the commit completed.
    pub progress: GroupProgressReport,
    /// Per-file metadata keyed by task ID, one entry per successfully ingested file.
    pub uploads: HashMap<UniqueID, XetFileMetadata>,
}

/// Per-file metadata returned by [`XetFileUpload::finalize_ingestion`] and
/// [`XetStreamUpload::finish`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct XetFileMetadata {
    /// Unique identifier for the task that produced this metadata.
    #[serde(skip)]
    pub task_id: UniqueID,
    /// Xet file information: hash, size, and optional SHA-256.
    pub xet_info: XetFileInfo,
    /// Per-file deduplication and chunking metrics.
    pub dedup_metrics: DeduplicationMetrics,
    /// Original file name or designated tracking name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracking_name: Option<String>,
}

// ── XetUploadCommitInner ────────────────────────────────────────────────────

pub(super) struct XetUploadCommitInner {
    commit_id: UniqueID,
    pub(super) session: XetSession,
    pub(super) task_runtime: Arc<TaskRuntime>,
    upload_session: Arc<FileUploadSession>,
    pub(super) file_handles: Mutex<Vec<XetFileUpload>>,
    stream_handles: Mutex<Vec<XetStreamUpload>>,
}

impl XetUploadCommitInner {
    async fn upload_from_path(
        self: &Arc<Self>,
        file_path: PathBuf,
        sha256: Sha256Policy,
    ) -> Result<XetFileUpload, XetError> {
        let absolute_path = std::path::absolute(file_path)?;
        let tracking_name = absolute_path.to_str().map(|s| s.to_owned());
        let (task_id, join_handle) = self
            .upload_session
            .spawn_upload_from_path(absolute_path.clone(), sha256)
            .await?;

        self.register_spawned_task(task_id, join_handle, tracking_name, Some(absolute_path))
    }

    async fn upload_stream(
        self: &Arc<Self>,
        tracking_name: Option<String>,
        sha256: Sha256Policy,
    ) -> Result<XetStreamUpload, XetError> {
        let name: Option<Arc<str>> = tracking_name.as_deref().map(Arc::from);
        let (task_id, cleaner) = self.upload_session.start_clean(name, None, sha256)?;
        let task_runtime = self.task_runtime.child()?;

        let handle = XetStreamUpload {
            inner: Arc::new(XetStreamUploadInner {
                task_id,
                result: Arc::new(OnceLock::new()),
                cleaner: Arc::new(tokio::sync::Mutex::new(Some((cleaner, tracking_name)))),
                upload_session: self.upload_session.clone(),
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
    ) -> Result<XetFileUpload, XetError> {
        let name: Option<Arc<str>> = tracking_name.as_deref().map(Arc::from);
        let (task_id, join_handle) = self.upload_session.spawn_upload_bytes(bytes, sha256, name).await?;

        self.register_spawned_task(task_id, join_handle, tracking_name, None)
    }

    fn register_spawned_task(
        &self,
        task_id: UniqueID,
        join_handle: tokio::task::JoinHandle<Result<(XetFileInfo, DeduplicationMetrics), xet_data::DataError>>,
        tracking_name: Option<String>,
        file_path: Option<PathBuf>,
    ) -> Result<XetFileUpload, XetError> {
        let task_runtime = self.task_runtime.child()?;
        let token = task_runtime.cancellation_token();

        let tn = tracking_name.clone();
        let mut upload_join_handle = join_handle;
        let mapped_handle = tokio::spawn(async move {
            tokio::select! {
                // Propagate TaskRuntime cancellation through the mapped background task.
                // We abort the owned upload join handle here so cancellation does not
                // leave the ingestion task detached in the runtime.
                _ = token.cancelled() => {
                    upload_join_handle.abort();
                    Err(XetError::UserCancelled("upload task cancelled by user".to_string()))
                }
                join_result = &mut upload_join_handle => {
                    match join_result {
                        Ok(Ok((xet_info, dedup_metrics))) => Ok(XetFileMetadata {
                            task_id,
                            xet_info,
                            dedup_metrics,
                            tracking_name: tn,
                        }),
                        Ok(Err(e)) => Err(XetError::from(e)),
                        Err(e) => Err(XetError::from(e)),
                    }
                }
            }
        });

        let inner = Arc::new(XetFileUploadInner {
            task_id,
            file_path,
            upload_session: self.upload_session.clone(),
            state: tokio::sync::Mutex::new(BackgroundTaskState::Running {
                join_handle: Some(mapped_handle),
            }),
        });

        self.file_handles
            .lock()
            .expect("file_handles lock poisoned")
            .push(XetFileUpload {
                inner: inner.clone(),
                task_runtime: task_runtime.clone(),
            });

        Ok(XetFileUpload { inner, task_runtime })
    }

    fn progress(&self) -> GroupProgressReport {
        self.upload_session.report()
    }

    async fn commit(self: &Arc<Self>) -> Result<XetCommitReport, XetError> {
        let stream_uploads = self.stream_handles.lock()?.clone();
        let mut files = HashMap::with_capacity(stream_uploads.len());

        for stream in &stream_uploads {
            match stream.try_finish() {
                Some(meta) => {
                    files.insert(stream.task_id(), meta);
                },
                None => return Err(XetError::other("stream upload not finished before commit")),
            }
        }

        let file_uploads = std::mem::take(&mut *self.file_handles.lock()?);

        let mut first_error = None;
        for upload in &file_uploads {
            match upload.finalize_ingestion().await {
                Ok(meta) => {
                    files.insert(upload.task_id(), meta);
                },
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    } else {
                        error!(task_id = %upload.task_id(), err = %e, "File upload finalization failed");
                    }
                },
            }
        }

        let finalize_result = self.upload_session.clone().finalize_with_report().await;
        let (dedup_metrics, progress) = match finalize_result {
            Ok(v) => v,
            Err(e) => return Err(e.into()),
        };
        if let Some(e) = first_error {
            return Err(e);
        }

        Ok(XetCommitReport {
            dedup_metrics,
            progress,
            uploads: files,
        })
    }

    pub(super) fn abort(&self) -> Result<(), XetError> {
        self.task_runtime.cancel_subtree()?;
        let file_uploads = std::mem::take(&mut *self.file_handles.lock()?);
        for upload in file_uploads {
            upload.abort_task();
        }
        let stream_uploads = std::mem::take(&mut *self.stream_handles.lock()?);
        for stream in stream_uploads {
            stream.abort();
        }
        Ok(())
    }
}

// ── XetUploadCommit (public wrapper) ────────────────────────────────────────

/// API for grouping related file uploads into a single atomic commit.
///
/// Obtain via [`XetSession::new_upload_commit`] — configure per-commit
/// auth on the returned [`AuthGroupBuilder`], then call
/// [`build`](AuthGroupBuilder::build) (async) or
/// [`build_blocking`](AuthGroupBuilder::build_blocking) (sync).
///
/// Enqueue files with [`upload_from_path`](Self::upload_from_path) /
/// [`upload_from_path_blocking`](Self::upload_from_path_blocking), stream bytes
/// with [`upload_stream`](Self::upload_stream) /
/// [`upload_stream_blocking`](Self::upload_stream_blocking), or upload raw bytes
/// with [`upload_bytes`](Self::upload_bytes) /
/// [`upload_bytes_blocking`](Self::upload_bytes_blocking) — transfers start
/// immediately in the background.
///
/// Poll progress with [`progress`](Self::progress), then call
/// [`commit`](Self::commit) (async) or [`commit_blocking`](Self::commit_blocking)
/// to wait for all uploads to finish and push metadata to the CAS server.
///
/// Per-file results are available via [`XetFileUpload::finalize_ingestion`] or
/// [`XetStreamUpload::finish`] at any time after ingestion completes —
/// you do not need to wait for [`commit`](Self::commit).
///
/// This type is cheaply clonable; all clones share the same underlying state.
///
/// # Errors
///
/// Methods return [`XetError::UserCancelled`] if the parent session has been
/// aborted, and [`XetError::AlreadyCompleted`] if [`commit`](Self::commit)
/// has already been called.
#[derive(Clone)]
pub struct XetUploadCommit {
    pub(super) inner: Arc<XetUploadCommitInner>,
    pub(super) task_runtime: Arc<TaskRuntime>,
}

impl XetUploadCommit {
    pub(super) async fn new(
        session: XetSession,
        task_runtime: Arc<TaskRuntime>,
        auth_options: AuthOptions,
    ) -> Result<Self, XetError> {
        let commit_id = UniqueID::new();
        let config = create_translator_config(&session, auth_options).await?;
        let upload_session = FileUploadSession::new(Arc::new(config)).await?;

        let inner = Arc::new(XetUploadCommitInner {
            commit_id,
            session,
            task_runtime: task_runtime.clone(),
            upload_session,
            file_handles: Mutex::new(Vec::new()),
            stream_handles: Mutex::new(Vec::new()),
        });

        Ok(Self { inner, task_runtime })
    }

    /// Unique identifier for this upload commit.
    pub fn id(&self) -> UniqueID {
        self.inner.commit_id
    }

    fn session(&self) -> &XetSession {
        &self.inner.session
    }

    // ===== Async public API =====

    /// Queue a file for upload from a path on disk.
    ///
    /// The file is read in a background task. Returns a [`XetFileUpload`]
    /// whose [`finalize_ingestion`](XetFileUpload::finalize_ingestion) method yields per-file
    /// [`XetFileMetadata`] once ingestion completes.
    ///
    /// # Parameters
    ///
    /// - `file_path`: path to the file. Resolved to an absolute path so the upload is unaffected by later
    ///   working-directory changes.
    /// - `sha256`: SHA-256 handling policy for this file.
    pub async fn upload_from_path(&self, file_path: PathBuf, sha256: Sha256Policy) -> Result<XetFileUpload, XetError> {
        info!(commit_id = %self.id(), path = ?file_path, "Upload from path");
        let inner = Arc::clone(&self.inner);
        self.task_runtime
            .bridge_async("upload_from_path", async move { inner.upload_from_path(file_path, sha256).await })
            .await
    }

    /// Begin an incremental streaming upload.
    ///
    /// Returns an [`XetStreamUpload`] for writing data in chunks.  Call
    /// [`write`](XetStreamUpload::write) to feed bytes, then
    /// [`finish`](XetStreamUpload::finish) to finalise ingestion.
    /// **`finish` must be called before [`commit`](Self::commit).**
    ///
    /// # Parameters
    ///
    /// - `tracking_name`: optional display name for progress and [`XetFileMetadata::tracking_name`].
    /// - `sha256`: SHA-256 handling policy for this file.
    pub async fn upload_stream(
        &self,
        tracking_name: Option<String>,
        sha256: Sha256Policy,
    ) -> Result<XetStreamUpload, XetError> {
        info!(commit_id = %self.id(), name = ?tracking_name, "Upload stream");
        let inner = Arc::clone(&self.inner);
        self.task_runtime
            .bridge_async("upload_stream", async move { inner.upload_stream(tracking_name, sha256).await })
            .await
    }

    /// Queue raw bytes for upload, starting the transfer immediately.
    ///
    /// Returns a [`XetFileUpload`] whose
    /// [`finalize_ingestion`](XetFileUpload::finalize_ingestion) method yields
    /// per-file [`XetFileMetadata`] once ingestion completes.
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
    ) -> Result<XetFileUpload, XetError> {
        info!(
            commit_id = %self.id(),
            name = ?tracking_name,
            size = bytes.len(),
            "Upload bytes"
        );
        let inner = Arc::clone(&self.inner);
        self.task_runtime
            .bridge_async("upload_bytes", async move { inner.upload_bytes(bytes, sha256, tracking_name).await })
            .await
    }

    /// Return a snapshot of aggregate progress for every queued upload.
    pub fn progress(&self) -> GroupProgressReport {
        self.inner.progress()
    }

    pub fn status(&self) -> Result<XetTaskState, XetError> {
        self.task_runtime.status()
    }

    /// Wait for all uploads to complete and push metadata to the CAS server.
    ///
    /// Returns a [`XetCommitReport`] with aggregate dedup metrics, progress,
    /// and per-file [`XetFileMetadata`].  Subsequent calls return
    /// [`XetError::AlreadyCompleted`].
    pub async fn commit(&self) -> Result<XetCommitReport, XetError> {
        info!(commit_id = %self.id(), "Commit starting");
        let inner = Arc::clone(&self.inner);
        self.task_runtime
            .bridge_async_finalizing("commit", false, async move { inner.commit().await })
            .await
    }

    /// Returns `true` if [`commit`](Self::commit) has completed.
    #[cfg(test)]
    fn is_committed(&self) -> bool {
        matches!(self.inner.task_runtime.status(), Ok(XetTaskState::Completed))
    }

    // ===== Blocking (sync) variants =====

    /// Blocking version of [`upload_from_path`](Self::upload_from_path).
    ///
    /// Reads a file from disk in a background task and returns an
    /// [`XetFileUpload`] for tracking the result.
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn upload_from_path_blocking(
        &self,
        file_path: PathBuf,
        sha256: Sha256Policy,
    ) -> Result<XetFileUpload, XetError> {
        info!(commit_id = %self.id(), path = ?file_path, "Upload from path");
        let inner = Arc::clone(&self.inner);
        self.task_runtime
            .bridge_sync("upload_from_path_blocking", async move { inner.upload_from_path(file_path, sha256).await })
    }

    /// Blocking version of [`upload_stream`](Self::upload_stream).
    ///
    /// Returns an [`XetStreamUpload`] for incrementally writing data.
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn upload_stream_blocking(
        &self,
        tracking_name: Option<String>,
        sha256: Sha256Policy,
    ) -> Result<XetStreamUpload, XetError> {
        info!(commit_id = %self.id(), name = ?tracking_name, "Upload stream");
        let inner = Arc::clone(&self.inner);
        self.task_runtime
            .bridge_sync("upload_stream_blocking", async move { inner.upload_stream(tracking_name, sha256).await })
    }

    /// Blocking version of [`upload_bytes`](Self::upload_bytes).
    ///
    /// Queues raw bytes and returns a [`XetFileUpload`].
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn upload_bytes_blocking(
        &self,
        bytes: Vec<u8>,
        sha256: Sha256Policy,
        tracking_name: Option<String>,
    ) -> Result<XetFileUpload, XetError> {
        info!(
            commit_id = %self.id(),
            name = ?tracking_name,
            size = bytes.len(),
            "Upload bytes"
        );
        let inner = Arc::clone(&self.inner);
        self.task_runtime
            .bridge_sync("upload_bytes_blocking", async move { inner.upload_bytes(bytes, sha256, tracking_name).await })
    }

    /// Blocking version of [`commit`](Self::commit).
    ///
    /// Waits for all uploads to complete and pushes metadata to the CAS
    /// server.  Returns a [`XetCommitReport`].
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn commit_blocking(&self) -> Result<XetCommitReport, XetError> {
        info!(commit_id = %self.id(), "Commit starting");
        let inner = Arc::clone(&self.inner);
        self.task_runtime
            .bridge_sync_finalizing("commit_blocking", false, async move { inner.commit().await })
    }

    /// Cancel all active uploads in this commit.
    pub fn abort(&self) -> Result<(), XetError> {
        info!(commit_id = %self.id(), "Commit abort");
        self.inner.abort()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::time::Duration;

    use tempfile::tempdir;
    use xet_runtime::core::RuntimeMode;

    use super::super::session::XetSessionBuilder;
    use super::*;

    // ── Mutex guard / concurrency test ───────────────────────────────────────

    #[test]
    fn test_commit_blocked_while_upload_registration_holds_state_lock() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let cas_path = temp.path().join("cas");
        let endpoint = format!("local://{}", cas_path.display());
        let session = XetSessionBuilder::new().build()?;
        let threadpool = session.inner.runtime.threadpool.clone();
        let commit = session.new_upload_commit()?.with_endpoint(&endpoint).build_blocking()?;
        let commit_for_thread = commit.clone();
        let threadpool_for_thread = threadpool.clone();

        let guard = commit.inner.file_handles.lock().unwrap();

        let (done_tx, done_rx) = mpsc::channel::<()>();
        let join_handle = std::thread::spawn(move || {
            let _ = threadpool_for_thread.external_run_async_task(async move { commit_for_thread.commit().await });
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
        let session = XetSessionBuilder::new().build().unwrap();
        let c1 = session.new_upload_commit().unwrap().build().await.unwrap();
        let c2 = session.new_upload_commit().unwrap().build().await.unwrap();
        assert_ne!(c1.id(), c2.id());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_commit_clone_shares_id() {
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session.new_upload_commit().unwrap().build().await.unwrap();
        let commit2 = commit.clone();
        assert_eq!(commit.id(), commit2.id());
    }

    // ── Initial state ────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_progress_empty_initially() {
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session.new_upload_commit().unwrap().build().await.unwrap();
        let report = commit.progress();
        assert_eq!(report.total_bytes, 0);
        assert_eq!(report.total_bytes_completed, 0);
    }

    // ── Commit lifecycle ─────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_commit_empty_succeeds() {
        let session = XetSessionBuilder::new().build().unwrap();
        session
            .new_upload_commit()
            .unwrap()
            .build()
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_commit_marks_as_committed() {
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session.new_upload_commit().unwrap().build().await.unwrap();
        let commit_clone = commit.clone();
        commit.commit().await.unwrap();
        assert!(commit_clone.is_committed());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_second_commit_fails() {
        let session = XetSessionBuilder::new().build().unwrap();
        let c1 = session.new_upload_commit().unwrap().build().await.unwrap();
        let c2 = c1.clone();
        c1.commit().await.unwrap();
        let err = c2.commit().await.unwrap_err();
        assert!(matches!(err, XetError::AlreadyCompleted | XetError::Internal(_)));
    }

    // ── Session-abort guards ─────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_file_on_aborted_session_returns_error() {
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session.new_upload_commit().unwrap().build().await.unwrap();
        session.abort().unwrap();
        let err = commit
            .upload_from_path(PathBuf::from("nonexistent.bin"), Sha256Policy::Compute)
            .await
            .unwrap_err();
        assert!(matches!(err, XetError::UserCancelled(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_bytes_on_aborted_session_returns_error() {
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session.new_upload_commit().unwrap().build().await.unwrap();
        session.abort().unwrap();
        let err = commit
            .upload_bytes(b"data".to_vec(), Sha256Policy::Compute, Some("bytes 1".into()))
            .await
            .unwrap_err();
        assert!(matches!(err, XetError::UserCancelled(_)));
    }

    // ── Post-commit guards (AlreadyCompleted) ────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_from_path_after_commit_fails() {
        let session = XetSessionBuilder::new().build().unwrap();
        let c1 = session.new_upload_commit().unwrap().build().await.unwrap();
        let c2 = c1.clone();
        c1.commit().await.unwrap();
        let err = c2
            .upload_from_path(PathBuf::from("any.bin"), Sha256Policy::Compute)
            .await
            .unwrap_err();
        assert!(matches!(err, XetError::AlreadyCompleted));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_bytes_after_commit_fails() {
        let session = XetSessionBuilder::new().build().unwrap();
        let c1 = session.new_upload_commit().unwrap().build().await.unwrap();
        let c2 = c1.clone();
        c1.commit().await.unwrap();
        let err = c2
            .upload_bytes(b"hello".to_vec(), Sha256Policy::Compute, None)
            .await
            .unwrap_err();
        assert!(matches!(err, XetError::AlreadyCompleted));
    }

    // ── API coverage & abort ─────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_file_returns_stream_handle() {
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session.new_upload_commit().unwrap().build().await.unwrap();
        let handle = commit
            .upload_stream(Some("stream.bin".into()), Sha256Policy::Compute)
            .await
            .unwrap();
        assert!(handle.try_finish().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_abort_cancels_tracked_uploads() {
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session.new_upload_commit().unwrap().build().await.unwrap();
        let handle = commit
            .upload_bytes(b"data".to_vec(), Sha256Policy::Compute, None)
            .await
            .unwrap();
        commit.abort().unwrap();
        assert!(commit.inner.file_handles.lock().unwrap().is_empty());
        let result = handle.finalize_ingestion().await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_abort_while_state_lock_held_still_drains_tasks() {
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session.new_upload_commit().unwrap().build().await.unwrap();
        let _handle = commit
            .upload_bytes(b"data".to_vec(), Sha256Policy::Compute, None)
            .await
            .unwrap();

        commit.abort().unwrap();
        assert!(matches!(commit.inner.task_runtime.status().unwrap(), XetTaskState::UserCancelled));

        assert!(commit.inner.file_handles.lock().unwrap().is_empty());
    }

    // ── Independence ─────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_two_commits_are_independent() {
        let session = XetSessionBuilder::new().build().unwrap();
        let c1 = session.new_upload_commit().unwrap().build().await.unwrap();
        let c2 = session.new_upload_commit().unwrap().build().await.unwrap();
        c1.commit().await.unwrap();
        assert!(!c2.is_committed());
    }

    // ── Round-trip tests ─────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_bytes_round_trip() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let data = b"Hello, upload commit round-trip!";
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let task_handle = commit
            .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("hello.bin".into()))
            .await
            .unwrap();

        commit.commit().await.unwrap();

        let meta = task_handle.try_finish().unwrap();
        assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        assert!(!meta.xet_info.hash.is_empty());
        assert!(meta.xet_info.sha256.is_some());
        assert_eq!(meta.xet_info.sha256.as_deref().unwrap().len(), 64);
        assert_eq!(meta.tracking_name.as_deref(), Some("hello.bin"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_bytes_task_id_matches_progress() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();

        let handle = commit
            .upload_bytes(b"id-match".to_vec(), Sha256Policy::Compute, Some("id.bin".into()))
            .await
            .unwrap();

        for _ in 0..50 {
            if handle.progress().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(handle.progress().is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_handle_file_path_none_for_bytes_upload() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let handle = commit
            .upload_bytes(b"no-path".to_vec(), Sha256Policy::Compute, Some("bytes.bin".into()))
            .await
            .unwrap();
        assert_eq!(handle.file_path(), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_from_path_round_trip() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let src = temp.path().join("data.bin");
        let data = b"file path upload content";
        std::fs::write(&src, data).unwrap();
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let handle = commit.upload_from_path(src, Sha256Policy::Compute).await.unwrap();
        commit.commit().await.unwrap();
        let meta = handle.try_finish().unwrap();
        assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        assert!(!meta.xet_info.hash.is_empty());
        assert!(meta.xet_info.sha256.is_some());
        assert_eq!(meta.xet_info.sha256.as_deref().unwrap().len(), 64);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_handle_file_path_for_path_upload() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let src = temp.path().join("path_meta.bin");
        std::fs::write(&src, b"path metadata").unwrap();
        let absolute = std::path::absolute(&src).unwrap();
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let handle = commit.upload_from_path(src, Sha256Policy::Compute).await.unwrap();
        assert_eq!(handle.file_path(), Some(absolute));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_bytes_sha256_policy_metadata() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
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

        let compute_meta = compute_handle.try_finish().unwrap();
        assert!(compute_meta.xet_info.sha256.is_some());
        assert_eq!(compute_meta.xet_info.sha256.as_deref().unwrap().len(), 64);

        let provided_meta = provided_handle.try_finish().unwrap();
        assert_eq!(provided_meta.xet_info.sha256.as_deref(), Some(provided_sha256.as_str()));

        let skip_meta = skip_handle.try_finish().unwrap();
        assert!(skip_meta.xet_info.sha256.is_none());
    }

    // ── finish() before and after commit ─────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_finish_returns_result_before_commit() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let data = b"finish before commit";
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let handle = commit
            .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("early.bin".into()))
            .await
            .unwrap();
        let meta = handle.finalize_ingestion().await.unwrap();
        assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        commit.commit().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_finish_second_call_returns_cached_result() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let handle = commit
            .upload_bytes(b"idem".to_vec(), Sha256Policy::Compute, None)
            .await
            .unwrap();
        let meta1 = handle.finalize_ingestion().await.unwrap();
        let meta2 = handle.finalize_ingestion().await.unwrap();
        assert_eq!(meta1.xet_info.hash, meta2.xet_info.hash);
        let cached = handle.try_finish().unwrap();
        assert_eq!(meta1.xet_info.hash, cached.xet_info.hash);
        commit.commit().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_finish_includes_dedup_metrics() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let data = b"dedup metrics check";
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let handle = commit.upload_bytes(data.to_vec(), Sha256Policy::Compute, None).await.unwrap();
        let meta = handle.finalize_ingestion().await.unwrap();
        assert_eq!(meta.dedup_metrics.total_bytes, data.len() as u64);
        commit.commit().await.unwrap();
    }

    // ── Streaming upload ─────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_streaming_round_trip() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let data = b"streamed upload bytes";
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
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
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let handle = commit
            .upload_stream(Some("unfinished.bin".into()), Sha256Policy::Compute)
            .await
            .unwrap();
        handle.write(&b"partial data"[..]).await.unwrap();
        let err = commit.commit().await.unwrap_err();
        assert!(matches!(err, XetError::Internal(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_finish_second_call_is_already_completed() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let handle = commit
            .upload_stream(Some("idem.bin".into()), Sha256Policy::Compute)
            .await
            .unwrap();
        handle.write(&b"idem"[..]).await.unwrap();
        let meta1 = handle.finish().await.unwrap();
        let err = handle.finish().await.unwrap_err();
        assert!(matches!(err, XetError::AlreadyCompleted));
        let meta2 = handle.try_finish().unwrap();
        assert_eq!(meta1.xet_info.hash, meta2.xet_info.hash);
        commit.commit().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_write_after_finish_errors() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let handle = commit
            .upload_stream(Some("done.bin".into()), Sha256Policy::Compute)
            .await
            .unwrap();
        handle.write(&b"done"[..]).await.unwrap();
        handle.finish().await.unwrap();
        let err = handle.write(&b"more"[..]).await.unwrap_err();
        assert!(matches!(err, XetError::AlreadyCompleted));
        commit.commit().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_abort_prevents_further_writes() {
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session.new_upload_commit().unwrap().build().await.unwrap();
        let handle = commit
            .upload_stream(Some("abort.bin".into()), Sha256Policy::Compute)
            .await
            .unwrap();
        handle.abort();
        let err = handle.write(&b"data"[..]).await.unwrap_err();
        assert!(matches!(err, XetError::UserCancelled(_)));
        let result = handle.finish().await;
        assert!(result.is_err());
    }

    // ── Multiple files ───────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_multiple_files_in_one_commit() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
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
        assert!(h1.finalize_ingestion().await.is_ok());
        assert!(h2.finalize_ingestion().await.is_ok());
        assert!(h3.finalize_ingestion().await.is_ok());
    }

    // ── Progress ─────────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_progress_reflects_bytes_after_commit() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let data = b"progress tracking upload data";
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let progress_observer = commit.clone();
        commit
            .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("prog.bin".into()))
            .await
            .unwrap();
        commit.commit().await.unwrap();
        let report = progress_observer.progress();
        assert_eq!(report.total_bytes, data.len() as u64);
        assert_eq!(report.total_bytes_completed, data.len() as u64);
        assert_eq!(report.total_transfer_bytes, report.total_transfer_bytes_completed);
        assert!(report.total_transfer_bytes_completed <= data.len() as u64);
    }

    // ── Non-tokio executor (Owned-mode bridge) ────────────────────────────────

    #[test]
    fn test_async_bridge_works_from_futures_executor() {
        let temp = tempdir().unwrap();

        let endpoint = format!("local://{}", temp.path().join("cas").display());
        futures::executor::block_on(async {
            let session = XetSessionBuilder::new().build().unwrap();
            assert_eq!(session.inner.runtime.threadpool.mode(), RuntimeMode::Owned);

            let data = b"hello from non-tokio executor";
            let commit = session
                .new_upload_commit()
                .unwrap()
                .with_endpoint(&endpoint)
                .build()
                .await
                .unwrap();
            let handle = commit
                .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
                .await
                .unwrap();
            commit.commit().await.unwrap();

            let meta = handle.try_finish().unwrap();
            assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        });
    }

    #[test]
    fn test_async_bridge_works_from_smol_executor() {
        let temp = tempdir().unwrap();

        let endpoint = format!("local://{}", temp.path().join("cas").display());
        smol::block_on(async {
            let session = XetSessionBuilder::new().build().unwrap();
            assert_eq!(session.inner.runtime.threadpool.mode(), RuntimeMode::Owned);

            let data = b"hello from smol executor";
            let commit = session
                .new_upload_commit()
                .unwrap()
                .with_endpoint(&endpoint)
                .build()
                .await
                .unwrap();
            let handle = commit
                .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
                .await
                .unwrap();
            commit.commit().await.unwrap();

            let meta = handle.try_finish().unwrap();
            assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        });
    }

    #[test]
    fn test_async_bridge_works_from_async_std_executor() {
        let temp = tempdir().unwrap();

        let endpoint = format!("local://{}", temp.path().join("cas").display());
        async_std::task::block_on(async {
            let session = XetSessionBuilder::new().build().unwrap();
            assert_eq!(session.inner.runtime.threadpool.mode(), RuntimeMode::Owned);

            let data = b"hello from async-std executor";
            let commit = session
                .new_upload_commit()
                .unwrap()
                .with_endpoint(&endpoint)
                .build()
                .await
                .unwrap();
            let handle = commit
                .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
                .await
                .unwrap();
            commit.commit().await.unwrap();

            let meta = handle.try_finish().unwrap();
            assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        });
    }

    // ── Blocking API tests ────────────────────────────────────────────────────

    #[test]
    fn test_blocking_upload_bytes_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = XetSessionBuilder::new().build()?;
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let data = b"Hello, upload commit round-trip!";
        let commit = session.new_upload_commit()?.with_endpoint(&endpoint).build_blocking()?;
        let task_handle =
            commit.upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some("hello.bin".into()))?;
        let results = commit.commit_blocking()?;
        assert_eq!(results.uploads.len(), 1);
        let meta = results.uploads.get(&task_handle.task_id()).unwrap();
        assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        assert!(!meta.xet_info.hash.is_empty());
        Ok(())
    }

    #[test]
    fn test_blocking_upload_from_path_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = XetSessionBuilder::new().build()?;
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let src = temp.path().join("data.bin");
        let data = b"file path upload content";
        std::fs::write(&src, data)?;
        let commit = session.new_upload_commit()?.with_endpoint(&endpoint).build_blocking()?;
        let handle = commit.upload_from_path_blocking(src, Sha256Policy::Compute)?;
        commit.commit_blocking()?;
        let meta = handle.try_finish().unwrap();
        assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        assert!(!meta.xet_info.hash.is_empty());
        assert!(meta.xet_info.sha256.is_some());
        Ok(())
    }

    #[test]
    fn test_blocking_upload_result_access_patterns() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = XetSessionBuilder::new().build()?;
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let data = b"result access patterns";
        let src = temp.path().join("data.bin");
        std::fs::write(&src, data)?;
        let commit = session.new_upload_commit()?.with_endpoint(&endpoint).build_blocking()?;
        let handle = commit.upload_from_path_blocking(src, Sha256Policy::Compute)?;

        // Before commit, per-task result is not available yet.
        assert!(handle.try_finish().is_none());

        let results = commit.commit_blocking()?;

        // Result should be available in the commit map by task id.
        let map_result = results
            .uploads
            .get(&handle.task_id())
            .expect("task_id must be present in results");
        assert_eq!(map_result.xet_info.file_size, Some(data.len() as u64));

        // Result should also be available via the task handle.
        let handle_result = handle.try_finish().expect("result must be set after commit");
        assert_eq!(handle_result.xet_info.file_size, Some(data.len() as u64));
        Ok(())
    }

    #[test]
    fn test_blocking_upload_streaming_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = XetSessionBuilder::new().build()?;
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let data = b"streamed upload bytes";
        let commit = session.new_upload_commit()?.with_endpoint(&endpoint).build_blocking()?;
        let stream = commit.upload_stream_blocking(Some("stream.bin".into()), Sha256Policy::Compute)?;
        stream.write_blocking(data.to_vec())?;
        let meta = stream.finish_blocking()?;
        let results = commit.commit_blocking()?;
        assert_eq!(results.uploads.len(), 1);
        assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
        assert!(!meta.xet_info.hash.is_empty());
        Ok(())
    }

    #[test]
    fn test_blocking_upload_multiple_files_in_one_commit() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = XetSessionBuilder::new().build()?;
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let commit = session.new_upload_commit()?.with_endpoint(&endpoint).build_blocking()?;
        commit.upload_bytes_blocking(b"file one".to_vec(), Sha256Policy::Compute, Some("a.bin".into()))?;
        commit.upload_bytes_blocking(b"file two".to_vec(), Sha256Policy::Compute, Some("b.bin".into()))?;
        commit.upload_bytes_blocking(b"file three".to_vec(), Sha256Policy::Compute, Some("c.bin".into()))?;
        let results = commit.commit_blocking()?;
        assert_eq!(results.uploads.len(), 3);
        Ok(())
    }

    #[test]
    fn test_blocking_upload_progress_reflects_bytes_after_commit() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = XetSessionBuilder::new().build()?;
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let data = b"progress tracking upload data";
        let commit = session.new_upload_commit()?.with_endpoint(&endpoint).build_blocking()?;
        let progress_observer = commit.clone();
        commit.upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some("prog.bin".into()))?;
        commit.commit_blocking()?;
        let snapshot = progress_observer.progress();
        assert_eq!(snapshot.total_bytes, data.len() as u64);
        assert_eq!(snapshot.total_bytes_completed, data.len() as u64);
        assert_eq!(snapshot.total_transfer_bytes, snapshot.total_transfer_bytes_completed);
        assert!(snapshot.total_transfer_bytes_completed <= data.len() as u64);
        Ok(())
    }

    #[test]
    fn test_blocking_upload_file_returns_handle_without_status() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = XetSessionBuilder::new().build()?;
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let commit = session.new_upload_commit()?.with_endpoint(&endpoint).build_blocking()?;
        let handle = commit.upload_stream_blocking(Some("stream.bin".into()), Sha256Policy::Compute)?;
        assert!(handle.try_finish().is_none());
        Ok(())
    }

    fn assert_blocking_upload_round_trip<R>(run: R)
    where
        R: FnOnce(std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>),
    {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());

        run(Box::pin(async move {
            let data = b"upload from smol executor";
            let commit = session
                .new_upload_commit()
                .unwrap()
                .with_endpoint(&endpoint)
                .build_blocking()
                .unwrap();
            let handle = commit
                .upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
                .unwrap();
            let results = commit.commit_blocking().unwrap();
            let meta = results.uploads.get(&handle.task_id()).unwrap();
            assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
            assert!(!meta.xet_info.hash.is_empty());
        }));
    }

    #[test]
    fn test_blocking_upload_round_trip_in_smol() {
        assert_blocking_upload_round_trip(|fut| smol::block_on(fut));
    }

    #[test]
    fn test_blocking_upload_round_trip_in_futures_executor() {
        assert_blocking_upload_round_trip(|fut| futures::executor::block_on(fut));
    }

    #[test]
    fn test_blocking_upload_round_trip_in_async_std() {
        assert_blocking_upload_round_trip(|fut| async_std::task::block_on(fut));
    }

    // ── External-mode _blocking guard ────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_blocking_methods_error_in_external_mode() {
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session.new_upload_commit().unwrap().build().await.unwrap();

        let err = commit
            .upload_from_path_blocking(PathBuf::from("/nonexistent"), Sha256Policy::Compute)
            .err()
            .unwrap();
        assert!(matches!(err, XetError::WrongRuntimeMode(_)));

        let err = commit.upload_bytes_blocking(vec![], Sha256Policy::Compute, None).err().unwrap();
        assert!(matches!(err, XetError::WrongRuntimeMode(_)));

        let err = commit.upload_stream_blocking(None, Sha256Policy::Compute).err().unwrap();
        assert!(matches!(err, XetError::WrongRuntimeMode(_)));
    }

    // ── Owned-mode _blocking panic guard ─────────────────────────────────────

    #[test]
    fn test_upload_from_path_blocking_panics_in_async_context() {
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session.new_upload_commit().unwrap().build_blocking().unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async {
                commit.upload_from_path_blocking(PathBuf::from("/nonexistent"), Sha256Policy::Compute)
            })
        }));
        assert!(result.is_err(), "upload_from_path_blocking() must panic when called from async");
    }

    #[test]
    fn test_upload_bytes_blocking_panics_in_async_context() {
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session.new_upload_commit().unwrap().build_blocking().unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async { commit.upload_bytes_blocking(vec![], Sha256Policy::Compute, None) })
        }));
        assert!(result.is_err(), "upload_bytes_blocking() must panic when called from async");
    }

    #[test]
    fn test_upload_file_blocking_panics_in_async_context() {
        let session = XetSessionBuilder::new().build().unwrap();
        let commit = session.new_upload_commit().unwrap().build_blocking().unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async { commit.upload_stream_blocking(None, Sha256Policy::Compute) })
        }));
        assert!(result.is_err(), "upload_stream_blocking() must panic when called from async");
    }
}
