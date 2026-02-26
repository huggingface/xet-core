//! UploadCommit - groups related uploads

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use data::{FileUploadSession, SingleFileCleaner, XetFileInfo};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use ulid::Ulid;

use crate::common::{GroupState, create_translator_config};
use crate::errors::SessionError;
use crate::progress::{FileProgressSnapshot, GroupProgress, TaskStatus};
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
/// All clones share the same background worker and task state.
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

        let (command_tx, mut command_rx) = mpsc::unbounded_channel::<CommitCommand>();

        let inner = Arc::new(UploadCommitInner {
            commit_id,
            session,
            active_tasks: RwLock::new(HashMap::new()),
            progress: Arc::new(GroupProgress::new()),
            upload_session: TokioMutex::new(None),
            state: Mutex::new(GroupState::Alive),
            command_tx,
        });

        // Spawn background worker task on the runtime directly
        let inner_clone = inner.clone();
        inner.session.runtime.spawn(async move {
            while let Some(command) = command_rx.recv().await {
                match command {
                    CommitCommand::UploadFromPath { file_path, response_tx } => {
                        let result = inner_clone.start_upload_file_from_path(file_path).await;
                        let _ = response_tx.send(result);
                    },
                    CommitCommand::UploadBytes { bytes, response_tx } => {
                        let result = inner_clone.start_upload_bytes(bytes).await;
                        let _ = response_tx.send(result);
                    },
                    CommitCommand::UploadFile {
                        file_name,
                        file_size,
                        response_tx,
                    } => {
                        let result = inner_clone.start_upload_file(file_name, file_size).await;
                        let _ = response_tx.send(result);
                    },
                    CommitCommand::Commit { response_tx } => {
                        let result = inner_clone.handle_commit().await;
                        let _ = response_tx.send(result);
                        // After commit, stop processing commands
                        break;
                    },
                }
            }
        });

        Ok(Self { inner })
    }

    /// Get the commit ID.
    pub(crate) fn id(&self) -> Ulid {
        self.commit_id
    }

    /// Abort this upload commit.
    pub(crate) fn abort(&self) -> Result<(), SessionError> {
        *self.state.lock()? = GroupState::Aborted;
        Ok(())
    }

    // ===== Public synchronous methods =====

    /// Queue a file for upload, starting the transfer immediately.
    ///
    /// Returns the task ID that can be used to correlate entries returned by
    /// [`get_progress`](Self::get_progress).
    ///
    /// # Errors
    ///
    /// Returns [`SessionError::Aborted`] if the session has been aborted, or
    /// [`SessionError::AlreadyCommitted`] if [`commit`](Self::commit) has
    /// already been called.
    pub fn upload_from_path(&self, file_path: PathBuf) -> Result<Ulid, SessionError> {
        self.session.check_alive()?;
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(CommitCommand::UploadFromPath { file_path, response_tx })
            .map_err(|_| SessionError::other("Failed to send upload command"))?;

        response_rx
            .blocking_recv()
            .map_err(|_| SessionError::other("Failed to receive upload response"))?
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
    ) -> Result<(Ulid, SingleFileCleaner), SessionError> {
        self.session.check_alive()?;
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(CommitCommand::UploadFile {
                file_name,
                file_size,
                response_tx,
            })
            .map_err(|_| SessionError::other("Failed to send start_clean command"))?;

        response_rx
            .blocking_recv()
            .map_err(|_| SessionError::other("Failed to receive start_clean response"))?
    }

    /// Queue raw bytes for upload, starting the transfer immediately.
    ///
    /// Returns the task ID. See [`upload_file`](Self::upload_file) for details.
    pub fn upload_bytes(&self, bytes: Vec<u8>) -> Result<Ulid, SessionError> {
        self.session.check_alive()?;
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(CommitCommand::UploadBytes { bytes, response_tx })
            .map_err(|_| SessionError::other("Failed to send upload_bytes command"))?;

        response_rx
            .blocking_recv()
            .map_err(|_| SessionError::other("Failed to receive upload_bytes response"))?
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
    ///
    /// Per-file byte counts come from [`TaskProgress`], which is updated by
    /// the `FileUploadSession` callback.  The method is safe to call frequently
    /// from Python: integer counters are read atomically and the per-file map
    /// requires only a brief lock.
    pub fn get_progress(&self) -> Vec<UploadProgress> {
        let tasks = match self.active_tasks.read() {
            Ok(tasks) => tasks,
            Err(e) => {
                tracing::error!("Failed to acquire read lock on active_tasks: {}", e);
                return Vec::new();
            },
        };

        // Build a name→(total, completed) lookup from TaskProgress.
        let (_, files) = self.progress.snapshot();
        let file_snapshots: std::collections::HashMap<String, FileProgressSnapshot> =
            files.into_iter().map(|s| (s.item_name.to_string(), s)).collect();

        tasks
            .values()
            .filter_map(|handle| match handle.status.lock() {
                Ok(status) => {
                    let (bytes_total, bytes_completed) = handle
                        .file_name
                        .as_deref()
                        .and_then(|name| file_snapshots.get(name))
                        .map(|s| (s.total_bytes, s.bytes_completed))
                        .unwrap_or((0, 0));

                    Some(UploadProgress {
                        task_id: handle.task_id,
                        file_name: handle.file_name.clone(),
                        bytes_completed,
                        bytes_total,
                        status: *status,
                    })
                },
                Err(e) => {
                    tracing::error!("Failed to acquire lock on task status for {}: {}", handle.task_id, e);
                    None
                },
            })
            .collect()
    }

    /// Wait for all uploads to complete and push metadata to the CAS server.
    ///
    /// Blocks until every queued upload finishes (or fails), then finalises
    /// the upload session.  Returns one [`FileMetadata`] entry per uploaded
    /// file in the order they were registered.
    ///
    /// Consumes `self` — subsequent calls on any clone will return
    /// [`SessionError::AlreadyCommitted`] (or a channel-closed error if the
    /// background worker has already exited).
    pub fn commit(self) -> Result<Vec<FileMetadata>, SessionError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(CommitCommand::Commit { response_tx })
            .map_err(|_| SessionError::other("Failed to send commit command"))?;

        response_rx
            .blocking_recv()
            .map_err(|_| SessionError::other("Failed to receive commit response"))?
    }
}

/// Commands sent to the background worker thread
pub(crate) enum CommitCommand {
    UploadFromPath {
        file_path: PathBuf,
        response_tx: oneshot::Sender<Result<Ulid, SessionError>>,
    },
    UploadBytes {
        bytes: Vec<u8>,
        response_tx: oneshot::Sender<Result<Ulid, SessionError>>,
    },
    UploadFile {
        file_name: Option<String>,
        file_size: u64,
        response_tx: oneshot::Sender<Result<(Ulid, SingleFileCleaner), SessionError>>,
    },
    Commit {
        response_tx: oneshot::Sender<Result<Vec<FileMetadata>, SessionError>>,
    },
}

/// Handle for a single upload task
pub(crate) struct UploadTaskHandle {
    task_id: Ulid,
    file_name: Option<String>,
    join_handle: JoinHandle<Result<XetFileInfo, SessionError>>,
    status: Arc<Mutex<TaskStatus>>,
}

/// All shared state owned by a single UploadCommit instance.
/// Accessed through `Arc<UploadCommitInner>`; do not use this type directly.
#[doc(hidden)]
pub struct UploadCommitInner {
    commit_id: Ulid,
    session: XetSession,

    // Active upload tasks for this commit
    active_tasks: RwLock<HashMap<Ulid, UploadTaskHandle>>,

    // Aggregate + per-file progress, fed into FileUploadSession as a TrackingProgressUpdater
    progress: Arc<GroupProgress>,

    // Shared upload session (FileUploadSession from data crate)
    // Uses tokio::Mutex because it's held across await in get_or_create_upload_session
    upload_session: TokioMutex<Option<Arc<FileUploadSession>>>,

    // State
    state: Mutex<GroupState>,

    // Command channel to background worker task
    command_tx: mpsc::UnboundedSender<CommitCommand>,
}

impl UploadCommitInner {
    // ===== State helpers =====

    /// Check whether the commit is still accepting new tasks.
    fn check_accepting_tasks(&self) -> Result<(), SessionError> {
        match *self.state.lock()? {
            GroupState::Finished => Err(SessionError::AlreadyCommitted),
            GroupState::Aborted => Err(SessionError::Aborted),
            GroupState::Alive => Ok(()),
        }
    }

    // ===== Async handlers (called by the background worker thread) =====

    /// Get or create the shared `FileUploadSession`.
    async fn get_or_create_upload_session(&self) -> Result<Arc<FileUploadSession>, SessionError> {
        let mut session_lock = self.upload_session.lock().await;
        if session_lock.is_none() {
            let config = create_translator_config(&self.session)?;
            let progress_updater = self.progress.clone() as Arc<dyn progress_tracking::TrackingProgressUpdater>;
            let new_session = FileUploadSession::new(Arc::new(config), Some(progress_updater)).await?;
            *session_lock = Some(new_session);
        }
        Ok(session_lock
            .as_ref()
            .ok_or_else(|| SessionError::other("Upload session not initialized"))?
            .clone())
    }

    /// Spawn a runtime task that performs the actual file upload.
    fn spawn_upload_task(
        &self,
        upload_session: Arc<FileUploadSession>,
        file_path: PathBuf,
        status: Arc<Mutex<TaskStatus>>,
    ) -> JoinHandle<Result<XetFileInfo, SessionError>> {
        self.session.runtime.spawn(async move {
            *status.lock()? = TaskStatus::Running;

            let files = vec![(file_path.as_path(), None)];

            let result: Result<XetFileInfo, SessionError> = upload_session
                .upload_files(files)
                .await
                .map_err(SessionError::from)
                .and_then(|mut results| results.pop().ok_or_else(|| SessionError::other("No upload result")));

            let new_status = if result.is_ok() {
                TaskStatus::Completed
            } else {
                TaskStatus::Failed
            };
            *status.lock()? = new_status;

            result
        })
    }

    /// Handle an `UploadFile` command from the public API.
    async fn start_upload_file_from_path(&self, file_path: PathBuf) -> Result<Ulid, SessionError> {
        self.check_accepting_tasks()?;

        let task_id = Ulid::new();
        let status = Arc::new(Mutex::new(TaskStatus::Queued));

        let upload_session_arc = self.get_or_create_upload_session().await?;

        let join_handle = self.spawn_upload_task(upload_session_arc, file_path.clone(), status.clone());

        let handle = UploadTaskHandle {
            task_id,
            file_name: file_path.file_name().and_then(|n| n.to_str()).map(|s| s.to_string()),
            join_handle,
            status,
        };

        self.active_tasks.write()?.insert(task_id, handle);

        Ok(task_id)
    }

    /// Handle a `StartClean` command: initialise the upload session and return a
    /// [`SingleFileCleaner`] that the caller drives incrementally.
    async fn start_upload_file(
        &self,
        file_name: Option<String>,
        file_size: u64,
    ) -> Result<(Ulid, SingleFileCleaner), SessionError> {
        self.check_accepting_tasks()?;

        let task_id = Ulid::new();
        let upload_session = self.get_or_create_upload_session().await?;
        let file_name_arc: Option<std::sync::Arc<str>> = file_name.as_deref().map(std::sync::Arc::from);
        let cleaner = upload_session.start_clean(file_name_arc, file_size, None).await;

        Ok((task_id, cleaner))
    }

    /// Handle an `UploadBytes` command from the public API.
    async fn start_upload_bytes(&self, _bytes: Vec<u8>) -> Result<Ulid, SessionError> {
        // TODO: Implement upload_bytes
        Err(SessionError::other("upload_bytes not yet implemented"))
    }

    /// Handle a `Commit` command from the public API.
    async fn handle_commit(&self) -> Result<Vec<FileMetadata>, SessionError> {
        // Mark as not accepting new tasks
        {
            let mut state_guard = self.state.lock()?;
            if *state_guard == GroupState::Finished {
                return Err(SessionError::AlreadyCommitted);
            }
            *state_guard = GroupState::Aborted; // stop new tasks while draining
        }

        // Wait for all uploads to complete
        let handles: Vec<_> = {
            let mut tasks = self.active_tasks.write()?;
            tasks.drain().collect()
        };

        let mut results = Vec::new();
        for (_task_id, handle) in handles {
            let file_info = handle.join_handle.await.map_err(SessionError::TaskJoinError)??;

            results.push(FileMetadata {
                file_name: handle.file_name,
                hash: file_info.hash().to_string(),
                file_size: file_info.file_size(),
            });
        }

        // Finalize upload session
        if let Some(session_arc) = self.upload_session.lock().await.take() {
            session_arc.finalize().await?;
        }

        // Mark as committed
        *self.state.lock()? = GroupState::Finished;

        // Unregister from session
        self.session.finish_upload_commit(self.commit_id)?;

        Ok(results)
    }
}

/// Per-file metadata returned by [`UploadCommit::commit`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FileMetadata {
    /// Original file name, if known.
    pub file_name: Option<String>,
    /// File Xet hash.
    pub hash: String,
    /// File size in bytes.
    pub file_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::XetSession;

    #[test]
    fn test_commit_not_committed_initially() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let commit = session.new_upload_commit()?;
        assert!(!commit.is_committed());
        Ok(())
    }

    #[test]
    fn test_commit_has_unique_id() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let c1 = session.new_upload_commit()?;
        let c2 = session.new_upload_commit()?;
        assert_ne!(c1.id(), c2.id());
        Ok(())
    }

    #[test]
    fn test_commit_clone_shares_id() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let commit = session.new_upload_commit()?;
        let commit2 = commit.clone();
        assert_eq!(commit.id(), commit2.id());
        Ok(())
    }

    #[test]
    fn test_upload_file_on_aborted_session_returns_error() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let commit = session.new_upload_commit()?;
        session.abort().unwrap();
        let err = commit.upload_from_path(PathBuf::from("nonexistent.bin")).unwrap_err();
        assert!(matches!(err, SessionError::Aborted));
        Ok(())
    }

    #[test]
    fn test_upload_bytes_on_aborted_session_returns_error() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let commit = session.new_upload_commit()?;
        session.abort().unwrap();
        let err = commit.upload_bytes(b"data".to_vec()).unwrap_err();
        assert!(matches!(err, SessionError::Aborted));
        Ok(())
    }

    #[test]
    fn test_get_progress_empty_initially() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let commit = session.new_upload_commit()?;
        let progress = commit.get_progress();
        assert!(progress.is_empty());
        Ok(())
    }

    #[test]
    fn test_commit_empty_succeeds() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let results = session.new_upload_commit()?.commit()?;
        assert!(results.is_empty());
        Ok(())
    }

    #[test]
    fn test_commit_marks_as_committed() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let commit = session.new_upload_commit()?;
        let commit_clone = commit.clone();
        commit.commit().unwrap();
        assert!(commit_clone.is_committed());
        Ok(())
    }

    #[test]
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
    fn test_commit_unregisters_from_session() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let commit = session.new_upload_commit()?;
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
        commit.commit().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 0);
        Ok(())
    }
}
