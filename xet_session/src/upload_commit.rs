//! UploadCommit - groups related uploads

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use data::data_client::{clean_bytes, clean_file};
use data::{FileUploadSession, Sha256Policy, SingleFileCleaner, XetFileInfo};
use tokio::task::JoinHandle;
use ulid::Ulid;
use xet_runtime::XetRuntime;

use crate::common::{GroupState, create_translator_config};
use crate::errors::SessionError;
use crate::progress::{GroupProgress, ProgressSnapshot, TaskHandle, TaskStatus};
use crate::session::XetSession;

/// Groups related file uploads into a single atomic commit.
///
/// Enqueue files with [`upload_from_path`](Self::upload_from_path) or stream
/// bytes with [`upload_file`](Self::upload_file) — transfers start immediately
/// in the background.  Poll progress with [`get_progress`](Self::get_progress),
/// then call [`commit`](Self::commit) to wait for all uploads to finish and
/// push the final metadata to the CAS server.
///
/// # Cloning
///
/// Cloning is cheap — it simply increments an atomic reference count.
/// All clones share the same upload session and task state.
///
/// # Errors
///
/// Methods return [`SessionError::Aborted`] if the parent session has been
/// aborted, and [`SessionError::AlreadyCommitted`] if [`commit`](Self::commit)
/// has already been called.
#[derive(Clone)]
pub struct UploadCommit {
    inner: Arc<UploadCommitInner>,
}

impl std::ops::Deref for UploadCommit {
    type Target = UploadCommitInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl UploadCommit {
    /// Create a new upload commit
    pub(crate) fn new(session: XetSession) -> Result<Self, SessionError> {
        let commit_id = Ulid::new();

        let progress = Arc::new(GroupProgress::new());
        let progress_clone = progress.clone();
        let config = create_translator_config(&session)?;
        let upload_session = session.runtime.external_run_async_task(async move {
            let progress_updater = progress_clone as Arc<dyn progress_tracking::TrackingProgressUpdater>;
            FileUploadSession::new(Arc::new(config), Some(progress_updater)).await
        })??;

        let inner = Arc::new(UploadCommitInner {
            commit_id,
            session,
            active_tasks: RwLock::new(HashMap::new()),
            progress,
            upload_session: Mutex::new(Some(upload_session)),
            state: Mutex::new(GroupState::Alive),
        });

        Ok(Self { inner })
    }

    /// Get the commit ID.
    pub(crate) fn id(&self) -> Ulid {
        self.commit_id
    }

    /// Abort this upload commit.
    pub(crate) fn abort(&self) -> Result<(), SessionError> {
        self.inner.abort()
    }

    // ===== Public synchronous methods =====

    /// Queue a file for upload, starting the transfer immediately if system resource permits.
    ///
    /// Returns a [`TaskHandle`] that can be used to poll status and per-file
    /// progress without taking the GIL.
    ///
    /// # Errors
    ///
    /// Returns [`SessionError::Aborted`] if the session has been aborted, or
    /// [`SessionError::AlreadyCommitted`] if [`commit`](Self::commit) has
    /// already been called.
    pub fn upload_from_path(&self, file_path: PathBuf) -> Result<TaskHandle, SessionError> {
        self.session.check_alive()?;

        // Use the absolute path in case the process current working directory changes
        // while the task is queued.
        let absolute_path = std::path::absolute(file_path)?;
        self.inner.start_upload_file_from_path(absolute_path)
    }

    /// Begin an incremental file upload, returning a [`SingleFileCleaner`] that the
    /// caller uses to stream bytes.
    ///
    /// This is the low-level streaming counterpart to [`upload_from_path`](Self::upload_from_path).
    ///
    /// ```rust,no_run
    /// # use std::fs::File;
    /// # use std::io::Read;
    /// # use xet_session::SessionError;
    /// # async fn example(commit: xet_session::UploadCommit, filename: &str, filesize: u64) -> Result<(), Box<dyn std::error::Error>> {
    /// let (task_id, mut cleaner) = commit.upload_file(Some(filename.into()), filesize)?;
    /// let mut reader = File::open(&filename)?;
    /// let mut buffer = vec![0u8; 65536];
    /// loop {
    ///     let bytes = reader.read(&mut buffer)?;
    ///     if bytes == 0 {
    ///         break;
    ///     }
    ///     cleaner.add_data(&buffer[0..bytes]).await?;
    /// }
    /// let (file_info, _metrics) = cleaner.finish().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Parameters
    ///
    /// - `file_name`: optional name used for progress/telemetry reporting.
    /// - `file_size`: expected size in bytes (used for progress tracking; `0` is valid if unknown).
    pub fn upload_file(
        &self,
        file_name: Option<String>,
        file_size: u64,
    ) -> Result<(TaskHandle, SingleFileCleaner), SessionError> {
        self.session.check_alive()?;

        self.inner.start_upload_file(file_name, file_size)
    }

    /// Queue raw bytes for upload, starting the transfer immediately if system resource permits.
    ///
    /// Returns a [`TaskHandle`]. See [`upload_from_path`](Self::upload_from_path) for details.
    pub fn upload_bytes(&self, bytes: Vec<u8>, tracking_name: Option<String>) -> Result<TaskHandle, SessionError> {
        self.session.check_alive()?;
        self.inner.start_upload_bytes(bytes, tracking_name)
    }

    /// Returns `true` if [`commit`](Self::commit) has been called and completed.
    #[cfg(test)]
    fn is_committed(&self) -> bool {
        match self.state.lock() {
            Ok(state) => *state == GroupState::Finished,
            Err(_) => false,
        }
    }

    /// Return a snapshot of progress for every queued upload.
    pub fn get_progress(&self) -> Result<ProgressSnapshot, SessionError> {
        self.progress.snapshot()
    }

    /// Wait for all uploads to complete and push metadata to the CAS server.
    ///
    /// Blocks until every queued upload finishes (or fails), then finalises
    /// the upload session.  Returns one [`FileMetadata`] entry per uploaded
    /// file.
    ///
    /// Consumes `self` — subsequent calls on any clone will return
    /// [`SessionError::AlreadyCommitted`].
    pub fn commit(self) -> Result<Vec<Result<FileMetadata, SessionError>>, SessionError> {
        let inner = self.inner.clone();
        self.session
            .runtime
            .external_run_async_task(async move { inner.handle_commit().await })?
    }
}

/// Handle for a single upload task tracked internally by UploadCommit.
pub(crate) struct InnerUploadTaskHandle {
    status: Arc<Mutex<TaskStatus>>,
    tracking_name: Option<String>,
    join_handle: JoinHandle<Result<XetFileInfo, SessionError>>,
}

/// All shared state owned by a single UploadCommit instance.
/// Accessed through `Arc<UploadCommitInner>`; do not use this type directly.
#[doc(hidden)]
pub struct UploadCommitInner {
    commit_id: Ulid,
    session: XetSession,

    // Active upload tasks for this commit
    active_tasks: RwLock<HashMap<Ulid, InnerUploadTaskHandle>>,

    // Aggregate + per-file progress, fed into FileUploadSession as a TrackingProgressUpdater
    progress: Arc<GroupProgress>,

    // Shared upload session (FileUploadSession from data crate)
    upload_session: Mutex<Option<Arc<FileUploadSession>>>,

    // State
    state: Mutex<GroupState>,
}

impl UploadCommitInner {
    // ===== State helpers =====

    /// Check whether the commit is still accepting new tasks.
    fn check_accepting_tasks(state: &MutexGuard<GroupState>) -> Result<(), SessionError> {
        match **state {
            GroupState::Finished => Err(SessionError::AlreadyCommitted),
            GroupState::Aborted => Err(SessionError::Aborted),
            GroupState::Alive => Ok(()),
        }
    }

    /// Spawn a runtime task that performs the actual file upload from path
    fn spawn_upload_from_path_task(
        &self,
        upload_session: Arc<FileUploadSession>,
        file_path: PathBuf,
        status: Arc<Mutex<TaskStatus>>,
        tracking_id: Ulid,
    ) -> JoinHandle<Result<XetFileInfo, SessionError>> {
        let semaphore = self.runtime().common().file_ingestion_semaphore.clone();
        self.runtime().spawn(async move {
            // Update status from "Queued" to "Running" once a semaphore permit is acquired.
            let _permit = semaphore.acquire().await?;

            *status.lock()? = TaskStatus::Running;

            let result = clean_file(upload_session, &file_path, "", Some(tracking_id))
                .await
                .map_err(SessionError::from)
                .map(|(file_info, _metrics)| file_info);

            let new_status = if result.is_ok() {
                TaskStatus::Completed
            } else {
                TaskStatus::Failed
            };
            *status.lock()? = new_status;

            result
        })
    }

    /// Spawn a runtime task that performs the actual bytes upload
    fn spawn_upload_bytes_task(
        &self,
        upload_session: Arc<FileUploadSession>,
        bytes: Vec<u8>,
        status: Arc<Mutex<TaskStatus>>,
        tracking_id: Ulid,
    ) -> JoinHandle<Result<XetFileInfo, SessionError>> {
        let semaphore = self.runtime().common().file_ingestion_semaphore.clone();
        self.runtime().spawn(async move {
            // Update status from "Queued" to "Running" once a semaphore permit is acquired.
            let _permit = semaphore.acquire().await?;

            *status.lock()? = TaskStatus::Running;

            let result = clean_bytes(upload_session, bytes, Some(tracking_id))
                .await
                .map_err(SessionError::from)
                .map(|(file_info, _metrics)| file_info);

            let new_status = if result.is_ok() {
                TaskStatus::Completed
            } else {
                TaskStatus::Failed
            };
            *status.lock()? = new_status;

            result
        })
    }

    fn start_upload_file_from_path(&self, file_path: PathBuf) -> Result<TaskHandle, SessionError> {
        // Hold the state lock guard for the duration of this function so commit() will not run
        // when an upload task is registering.
        let state = self.state.lock()?;
        Self::check_accepting_tasks(&state)?;

        let tracking_id = Ulid::new();
        let status = Arc::new(Mutex::new(TaskStatus::Queued));
        let task_handle = TaskHandle {
            status: Some(status.clone()),
            group_progress: self.progress.clone(),
            tracking_id,
        };

        let Some(upload_session) = self.upload_session.lock()?.clone() else {
            return Err(SessionError::other("Upload session not initialized"));
        };

        let join_handle =
            self.spawn_upload_from_path_task(upload_session, file_path.clone(), status.clone(), tracking_id);

        let handle = InnerUploadTaskHandle {
            status,
            tracking_name: file_path.to_str().map(|s| s.to_owned()),
            join_handle,
        };

        self.active_tasks.write()?.insert(tracking_id, handle);

        Ok(task_handle)
    }

    /// Handle a `StartClean` command: initialise the upload session and return a
    /// [`SingleFileCleaner`] that the caller drives incrementally.
    fn start_upload_file(
        &self,
        tracking_name: Option<String>,
        file_size: u64,
    ) -> Result<(TaskHandle, SingleFileCleaner), SessionError> {
        // Hold the state lock guard for the duration of this function so commit() will not run
        // when an upload task is registering.
        let state = self.state.lock()?;
        Self::check_accepting_tasks(&state)?;

        let tracking_id = Ulid::new();
        let task_handle = TaskHandle {
            status: None, // upload directly managed by user - not internally managed
            group_progress: self.progress.clone(),
            tracking_id,
        };

        let Some(upload_session) = self.upload_session.lock()?.clone() else {
            return Err(SessionError::other("Upload session not initialized"));
        };

        let tracking_name: Option<Arc<str>> = tracking_name.as_deref().map(Arc::from);
        let cleaner = self.runtime().external_run_async_task(async move {
            upload_session
                .start_clean(tracking_name, file_size, Sha256Policy::Compute, tracking_id)
                .await
        })?;

        Ok((task_handle, cleaner))
    }

    /// Handle an `UploadBytes` command from the public API.
    fn start_upload_bytes(&self, bytes: Vec<u8>, tracking_name: Option<String>) -> Result<TaskHandle, SessionError> {
        // Hold the state lock guard for the duration of this function so commit() will not run
        // when an upload task is registering.
        let state = self.state.lock()?;
        Self::check_accepting_tasks(&state)?;

        let tracking_id = Ulid::new();
        let status = Arc::new(Mutex::new(TaskStatus::Queued));
        let task_handle = TaskHandle {
            status: Some(status.clone()),
            group_progress: self.progress.clone(),
            tracking_id,
        };

        let Some(upload_session) = self.upload_session.lock()?.clone() else {
            return Err(SessionError::other("Upload session not initialized"));
        };

        let join_handle = self.spawn_upload_bytes_task(upload_session, bytes, status.clone(), tracking_id);

        let handle = InnerUploadTaskHandle {
            status,
            tracking_name,
            join_handle,
        };

        self.active_tasks.write()?.insert(tracking_id, handle);

        Ok(task_handle)
    }

    /// Handle a `Commit` command from the public API.
    async fn handle_commit(&self) -> Result<Vec<Result<FileMetadata, SessionError>>, SessionError> {
        // Mark as not accepting new tasks
        {
            let mut state_guard = self.state.lock()?;
            if *state_guard == GroupState::Finished {
                return Err(SessionError::AlreadyCommitted);
            }
            *state_guard = GroupState::Aborted; // stop new tasks while draining
        }

        // Wait for all uploads to complete
        // Swap out the task map atomically while holding the write lock.
        // The guard is dropped immediately so the lock is not held across any `.await`.
        let active_tasks = std::mem::take(&mut *self.active_tasks.write()?);

        let mut results = Vec::new();
        let mut join_err = None;
        // Join all tasks first and then propogate errors.
        for (_task_id, handle) in active_tasks {
            match handle.join_handle.await.map_err(SessionError::TaskJoinError) {
                Ok(Ok(file_info)) => {
                    results.push(Ok(FileMetadata {
                        tracking_name: handle.tracking_name,
                        hash: file_info.hash().to_string(),
                        file_size: file_info.file_size(),
                    }));
                },
                Ok(Err(task_err)) => {
                    results.push(Err(task_err));
                },
                Err(e) => {
                    if join_err.is_none() {
                        join_err = Some(e);
                    }
                },
            };
        }
        if let Some(e) = join_err {
            return Err(e);
        }

        // Finalize upload session
        let session = self.upload_session.lock()?.take();
        if let Some(session) = session {
            session.finalize().await?;
        }

        // Mark as committed
        *self.state.lock()? = GroupState::Finished;

        // Unregister from session
        self.session.finish_upload_commit(self.commit_id)?;

        Ok(results)
    }

    fn runtime(&self) -> &XetRuntime {
        &self.session.runtime
    }

    /// Cancle all tasks and set task status to "Cancelled"
    fn abort(&self) -> Result<(), SessionError> {
        *self.state.lock()? = GroupState::Aborted;
        let active_tasks = std::mem::take(&mut *self.active_tasks.write()?);
        for (_tracking_id, inner_task_handle) in active_tasks {
            inner_task_handle.join_handle.abort();
            let _ = inner_task_handle.status.lock().map(|mut s| *s = TaskStatus::Cancelled);
        }

        Ok(())
    }
}

/// Per-file metadata returned by [`UploadCommit::commit`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FileMetadata {
    /// Original file name or designated tracking name
    pub tracking_name: Option<String>,
    /// File Xet hash.
    pub hash: String,
    /// File size in bytes.
    pub file_size: u64,
}

#[cfg(test)]
mod tests {
    use tempfile::{TempDir, tempdir};

    use super::*;
    use crate::session::XetSession;

    fn local_session(temp: &TempDir) -> Result<XetSession, Box<dyn std::error::Error>> {
        let cas_path = temp.path().join("cas");
        Ok(XetSession::new(Some(format!("local://{}", cas_path.display())), None, None, None)?)
    }

    // ── Identity ─────────────────────────────────────────────────────────────

    #[test]
    // Two separate commits from the same session have distinct IDs.
    fn test_commit_has_unique_id() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let c1 = session.new_upload_commit()?;
        let c2 = session.new_upload_commit()?;
        assert_ne!(c1.id(), c2.id());
        Ok(())
    }

    #[test]
    // A clone refers to the same inner Arc, so their IDs must match.
    fn test_commit_clone_shares_id() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let commit = session.new_upload_commit()?;
        let commit2 = commit.clone();
        assert_eq!(commit.id(), commit2.id());
        Ok(())
    }

    // ── Initial state ────────────────────────────────────────────────────────

    #[test]
    // A fresh commit has all-zero aggregate progress.
    fn test_get_progress_empty_initially() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let commit = session.new_upload_commit()?;
        let snapshot = commit.get_progress()?;
        let total = snapshot.total();
        assert_eq!(total.total_bytes, 0);
        assert_eq!(total.total_bytes_completed, 0);
        Ok(())
    }

    // ── Commit lifecycle ─────────────────────────────────────────────────────

    #[test]
    // An empty commit succeeds and returns an empty result set.
    fn test_commit_empty_succeeds() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let results = session.new_upload_commit()?.commit()?;
        assert!(results.is_empty());
        Ok(())
    }

    #[test]
    // commit() transitions the commit into the Finished state.
    fn test_commit_marks_as_committed() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let commit = session.new_upload_commit()?;
        let commit_clone = commit.clone();
        commit.commit().unwrap();
        assert!(commit_clone.is_committed());
        Ok(())
    }

    #[test]
    // A second commit() call on any clone returns AlreadyCommitted.
    fn test_second_commit_fails() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let c1 = session.new_upload_commit()?;
        let c2 = c1.clone();
        c1.commit()?;
        let err = c2.commit().unwrap_err();
        assert!(matches!(err, SessionError::AlreadyCommitted | SessionError::Other(_)));
        Ok(())
    }

    #[test]
    // commit() unregisters the commit from the session's active set.
    fn test_commit_unregisters_from_session() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let commit = session.new_upload_commit()?;
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
        commit.commit().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 0);
        Ok(())
    }

    // ── Session-abort guards ─────────────────────────────────────────────────

    #[test]
    // upload_from_path returns Aborted when the parent session has been aborted.
    fn test_upload_file_on_aborted_session_returns_error() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let commit = session.new_upload_commit()?;
        session.abort().unwrap();
        let err = commit.upload_from_path(PathBuf::from("nonexistent.bin")).unwrap_err();
        assert!(matches!(err, SessionError::Aborted));
        Ok(())
    }

    #[test]
    // upload_bytes returns Aborted when the parent session has been aborted.
    fn test_upload_bytes_on_aborted_session_returns_error() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let commit = session.new_upload_commit()?;
        session.abort().unwrap();
        let err = commit.upload_bytes(b"data".to_vec(), Some("bytes 1".into())).unwrap_err();
        assert!(matches!(err, SessionError::Aborted));
        Ok(())
    }

    // ── Post-commit guards (AlreadyCommitted) ────────────────────────────────

    #[test]
    // upload_from_path after commit returns AlreadyCommitted (synchronous path).
    fn test_upload_from_path_after_commit_fails() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let c1 = session.new_upload_commit()?;
        let c2 = c1.clone();
        c1.commit()?;
        let err = c2.upload_from_path(PathBuf::from("any.bin")).unwrap_err();
        assert!(matches!(err, SessionError::AlreadyCommitted));
        Ok(())
    }

    #[test]
    // upload_bytes after commit returns AlreadyCommitted (via external_run_async_task).
    fn test_upload_bytes_after_commit_fails() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let c1 = session.new_upload_commit()?;
        let c2 = c1.clone();
        c1.commit()?;
        let err = c2.upload_bytes(b"hello".to_vec(), None).unwrap_err();
        assert!(matches!(err, SessionError::AlreadyCommitted));
        Ok(())
    }

    // ── API coverage & abort ─────────────────────────────────────────────────

    #[test]
    // upload_file returns a (TaskHandle, SingleFileCleaner) pair; the handle has no internal status.
    fn test_upload_file_returns_handle_and_cleaner() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let commit = session.new_upload_commit()?;
        let (handle, _cleaner) = commit.upload_file(Some("stream.bin".into()), 1024)?;
        // Streaming uploads have no internally-managed status; status() returns an error.
        assert!(handle.status().is_err());
        Ok(())
    }

    #[test]
    // abort() drains active_tasks and sets each task's status to Cancelled.
    fn test_abort_marks_queued_task_as_cancelled() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let commit = session.new_upload_commit()?;
        let handle = commit.upload_bytes(b"data".to_vec(), None)?;
        commit.abort()?;
        assert!(matches!(handle.status()?, TaskStatus::Cancelled));
        Ok(())
    }

    #[test]
    // Committing one commit does not affect the state of another from the same session.
    fn test_two_commits_are_independent() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let c1 = session.new_upload_commit()?;
        let c2 = session.new_upload_commit()?;
        c1.commit()?;
        assert!(!c2.is_committed());
        Ok(())
    }

    // ── Round-trip tests ─────────────────────────────────────────────────────

    #[test]
    // Uploading raw bytes and committing returns a non-empty hash and the correct file size.
    fn test_upload_bytes_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let data = b"Hello, upload commit round-trip!";
        let commit = session.new_upload_commit()?;
        commit.upload_bytes(data.to_vec(), Some("hello.bin".into()))?;
        let results = commit.commit()?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap().file_size, data.len() as u64);
        assert!(!results[0].as_ref().unwrap().hash.is_empty());
        Ok(())
    }

    #[test]
    // Uploading a file from disk and committing returns the correct file size.
    fn test_upload_from_path_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let src = temp.path().join("data.bin");
        let data = b"file path upload content";
        std::fs::write(&src, data)?;
        let commit = session.new_upload_commit()?;
        commit.upload_from_path(src)?;
        let results = commit.commit()?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap().file_size, data.len() as u64);
        assert!(!results[0].as_ref().unwrap().hash.is_empty());
        Ok(())
    }

    #[test]
    // Streaming upload via upload_file + SingleFileCleaner: the caller receives XetFileInfo
    // directly from cleaner.finish(); commit() returns no entry for user-managed uploads.
    fn test_upload_streaming_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let data = b"streamed upload bytes";
        let runtime = session.runtime.clone();
        let commit = session.new_upload_commit()?;
        let (_handle, mut cleaner) = commit.upload_file(Some("stream.bin".into()), data.len() as u64)?;
        // Drive the cleaner and extract only Send + 'static fields from XetFileInfo.
        let (hash, file_size) = runtime.external_run_async_task(async move {
            cleaner.add_data(data).await.unwrap();
            let (xfi, _) = cleaner.finish().await.unwrap();
            (xfi.hash, xfi.file_size)
        })?;
        // Streaming uploads are user-managed; commit() returns results only for internally
        // tracked tasks (upload_from_path / upload_bytes).
        let results = commit.commit()?;
        assert!(results.is_empty());
        assert_eq!(file_size, data.len() as u64);
        assert!(!hash.is_empty());
        Ok(())
    }

    #[test]
    // Uploading multiple blobs in one commit returns one result per upload.
    fn test_upload_multiple_files_in_one_commit() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let commit = session.new_upload_commit()?;
        commit.upload_bytes(b"file one".to_vec(), Some("a.bin".into()))?;
        commit.upload_bytes(b"file two".to_vec(), Some("b.bin".into()))?;
        commit.upload_bytes(b"file three".to_vec(), Some("c.bin".into()))?;
        let results = commit.commit()?;
        assert_eq!(results.len(), 3);
        Ok(())
    }

    #[test]
    // After a successful commit the aggregate progress reflects bytes processed.
    fn test_upload_progress_reflects_bytes_after_commit() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let data = b"progress tracking upload data";
        let commit = session.new_upload_commit()?;
        let progress_observer = commit.clone();
        commit.upload_bytes(data.to_vec(), Some("prog.bin".into()))?;
        commit.commit()?;
        let snapshot = progress_observer.get_progress()?;
        assert!(snapshot.total().total_bytes_completed > 0);
        Ok(())
    }

    // ── Mutex guard / concurrency test ───────────────────────────────────────
    //
    // All three enqueue methods (upload_from_path, upload_bytes, upload_file)
    // hold `self.state` for their entire execution so that commit() cannot
    // race against an in-progress registration.  Because they all share the
    // same mutex, a single test covers all three: we lock the mutex directly
    // from the test thread (valid because `mod tests` is a descendant of
    // `upload_commit` and can access private fields), which simulates any of
    // the three methods being mid-registration.

    #[test]
    // commit() must block while any enqueue method holds the state lock.
    fn test_commit_blocked_while_upload_registration_holds_state_lock() -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::mpsc;
        use std::time::Duration;

        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let commit = session.new_upload_commit()?;
        let commit_for_thread = commit.clone();

        // Simulate an enqueue method (upload_from_path / upload_bytes /
        // upload_file) holding the state lock mid-registration.
        let guard = commit.inner.state.lock().unwrap();

        let (done_tx, done_rx) = mpsc::channel::<()>();
        let join_handle = std::thread::spawn(move || {
            let _ = commit_for_thread.commit();
            let _ = done_tx.send(());
        });

        std::thread::sleep(Duration::from_millis(50));
        assert!(done_rx.try_recv().is_err(), "commit() should be blocked while state lock is held");

        // Release the lock — simulates the enqueue method completing its registration.
        drop(guard);

        assert!(
            done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "commit() should complete after state lock is released"
        );
        let _ = join_handle.join();
        Ok(())
    }
}
