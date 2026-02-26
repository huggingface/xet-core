//! DownloadGroup - groups related downloads

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use data::{FileDownloadSession, XetFileInfo};
use tokio::sync::{Mutex as TokioMutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use ulid::Ulid;

use crate::common::{GroupState, create_translator_config};
use crate::errors::SessionError;
use crate::progress::{GroupProgress, ProgressSnapshot, TaskHandle, TaskStatus};
use crate::session::XetSession;

/// Groups related file downloads into a single unit of work.
///
/// Queue files with [`download_file`](Self::download_file) (they start
/// downloading immediately in the background), poll progress with
/// [`get_progress`](Self::get_progress), then call
/// [`finish`](Self::finish) to wait for all downloads to complete.
///
/// # Cloning
///
/// Cloning is cheap — it simply increments an atomic reference count.
/// All clones share the same background worker and task state.
///
/// # Errors
///
/// Methods return [`SessionError::Aborted`] if the parent session has been
/// aborted, and [`SessionError::AlreadyFinished`] if
/// [`finish`](Self::finish) has already been called.
#[derive(Clone)]
pub struct DownloadGroup {
    inner: Arc<DownloadGroupInner>,
}

impl std::ops::Deref for DownloadGroup {
    type Target = DownloadGroupInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DownloadGroup {
    /// Create a new download group
    pub(crate) fn new(session: XetSession) -> Result<Self, SessionError> {
        let group_id = Ulid::new();

        let (command_tx, mut command_rx) = mpsc::unbounded_channel::<GroupCommand>();

        let inner = Arc::new(DownloadGroupInner {
            group_id,
            session,
            active_tasks: RwLock::new(HashMap::new()),
            progress: Arc::new(GroupProgress::new()),
            download_session: TokioMutex::new(None),
            state: Mutex::new(GroupState::Alive),
            command_tx,
        });

        // Spawn background worker task on the runtime directly
        let inner_clone = inner.clone();
        inner.session.runtime.spawn(async move {
            while let Some(command) = command_rx.recv().await {
                match command {
                    GroupCommand::DownloadFile {
                        file_info,
                        dest_path,
                        response_tx,
                    } => {
                        let result = inner_clone.handle_download_file(file_info, dest_path).await;
                        let _ = response_tx.send(result);
                    },
                    GroupCommand::Finish { response_tx } => {
                        let result = inner_clone.handle_finish().await;
                        let _ = response_tx.send(result);
                        // After finish, stop processing commands
                        break;
                    },
                }
            }
        });

        Ok(Self { inner })
    }

    /// Get the group ID.
    pub(crate) fn id(&self) -> Ulid {
        self.group_id
    }

    /// Abort this download group.
    pub(crate) fn abort(&self) -> Result<(), SessionError> {
        *self.state.lock()? = GroupState::Aborted;
        Ok(())
    }

    // ===== Public synchronous methods =====

    /// Queue a file for download to `dest_path`, starting the transfer immediately.
    ///
    /// # Parameters
    ///
    /// * `file_hash` – Content-addressed hash returned by a previous
    ///   [`UploadCommit::commit`](crate::UploadCommit::commit).
    /// * `file_size` – Expected file size in bytes (used to pre-allocate buffers and report progress).
    /// * `dest_path` – Local path where the downloaded file will be written. Parent directories are created
    ///   automatically.
    ///
    /// Returns a [`TaskHandle`] that can be used to poll status and per-file
    /// progress without taking the GIL.
    ///
    /// # Errors
    ///
    /// Returns [`SessionError::Aborted`] if the session has been aborted, or
    /// [`SessionError::AlreadyFinished`] if [`finish`](Self::finish) has already
    /// been called.
    pub fn download_file(&self, file_info: XetFileInfo, dest_path: PathBuf) -> Result<TaskHandle, SessionError> {
        self.session.check_alive()?;
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(GroupCommand::DownloadFile {
                file_info,
                dest_path,
                response_tx,
            })
            .map_err(|_| SessionError::other("Failed to send download command"))?;

        response_rx
            .blocking_recv()
            .map_err(|_| SessionError::other("Failed to receive download response"))?
    }

    pub fn download_stream(&self, _file_hash: String, _file_size: u64) -> Result<(), SessionError> {
        todo!()
    }

    /// Returns `true` if [`finish`](Self::finish) has been called and completed.
    #[cfg(test)]
    fn is_finished(&self) -> bool {
        match self.state.lock() {
            Ok(state) => *state == GroupState::Finished,
            Err(_) => false,
        }
    }

    /// Return a snapshot of progress for every queued download.
    ///
    /// Per-file byte counts come from [`GroupProgress`], which is updated by
    /// the [`TrackingProgressUpdater`] implementation as data flows through
    /// `FileDownloadSession`.  The method is safe to call frequently from Python:
    /// integer counters are read atomically and the per-file map requires only
    /// a brief lock.
    pub fn get_progress(&self) -> Result<ProgressSnapshot, SessionError> {
        self.progress.snapshot()
    }

    /// Wait for all downloads to complete and return their results.
    ///
    /// Blocks until every queued download finishes (or fails).  Returns one
    /// [`DownloadResult`] entry per download in the order they were registered.
    ///
    /// Consumes `self` — subsequent calls on any clone will return
    /// [`SessionError::AlreadyFinished`] (or a channel-closed error if the
    /// background worker has already exited).
    pub fn finish(self) -> Result<Vec<DownloadResult>, SessionError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(GroupCommand::Finish { response_tx })
            .map_err(|_| SessionError::other("Failed to send finish command"))?;

        response_rx
            .blocking_recv()
            .map_err(|_| SessionError::other("Failed to receive finish response"))?
    }
}

/// Commands sent to the background worker thread
pub(crate) enum GroupCommand {
    DownloadFile {
        file_info: XetFileInfo,
        dest_path: PathBuf,
        response_tx: oneshot::Sender<Result<TaskHandle, SessionError>>,
    },
    Finish {
        response_tx: oneshot::Sender<Result<Vec<DownloadResult>, SessionError>>,
    },
}

/// Handle for a single download task tracked internally by DownloadGroup.
pub(crate) struct InnerDownloadTaskHandle {
    dest_path: PathBuf,
    join_handle: JoinHandle<Result<XetFileInfo, SessionError>>,
}

/// All shared state owned by a single DownloadGroup instance.
/// Accessed through `Arc<DownloadGroupInner>`; do not use this type directly.
#[doc(hidden)]
pub struct DownloadGroupInner {
    group_id: Ulid,
    session: XetSession,

    // Active download tasks for this group
    active_tasks: RwLock<HashMap<Ulid, InnerDownloadTaskHandle>>,

    // Aggregate + per-file progress, fed into FileDownloadSession as a TrackingProgressUpdater
    progress: Arc<GroupProgress>,

    // Shared download session (FileDownloadSession from data crate)
    // Uses tokio::Mutex because it's held across await in get_or_create_download_session
    download_session: TokioMutex<Option<Arc<FileDownloadSession>>>,

    // State
    state: Mutex<GroupState>,

    // Command channel to background worker task
    command_tx: mpsc::UnboundedSender<GroupCommand>,
}

impl DownloadGroupInner {
    // ===== State helpers =====

    /// Check whether the group is still accepting new tasks.
    fn check_accepting_tasks(&self) -> Result<(), SessionError> {
        match *self.state.lock()? {
            GroupState::Finished => Err(SessionError::AlreadyFinished),
            GroupState::Aborted => Err(SessionError::Aborted),
            GroupState::Alive => Ok(()),
        }
    }

    // ===== Async handlers (called by the background worker thread) =====

    /// Get or create the shared `FileDownloadSession`.
    async fn get_or_create_download_session(&self) -> Result<Arc<FileDownloadSession>, SessionError> {
        let mut session_lock = self.download_session.lock().await;
        if session_lock.is_none() {
            let config = create_translator_config(&self.session)?;
            let progress_updater = self.progress.clone() as Arc<dyn progress_tracking::TrackingProgressUpdater>;
            let new_session = FileDownloadSession::new(Arc::new(config), Some(progress_updater)).await?;
            *session_lock = Some(new_session);
        }
        Ok(session_lock
            .as_ref()
            .ok_or_else(|| SessionError::other("Download session not initialized"))?
            .clone())
    }

    /// Spawn a runtime task that performs the actual file download.
    fn spawn_download_task(
        self: &Arc<Self>,
        download_session: Arc<FileDownloadSession>,
        file_info: XetFileInfo,
        dest_path: PathBuf,
        status: Arc<Mutex<TaskStatus>>,
        tracking_id: Ulid,
    ) -> JoinHandle<Result<XetFileInfo, SessionError>> {
        self.session.runtime.spawn(async move {
            *status.lock()? = TaskStatus::Running;

            let result: Result<_, SessionError> = download_session
                .download_file(&file_info, &dest_path, tracking_id)
                .await
                .map_err(SessionError::from);

            let new_status = if result.is_ok() {
                TaskStatus::Completed
            } else {
                TaskStatus::Failed
            };
            *status.lock()? = new_status;

            Ok(XetFileInfo {
                hash: file_info.hash,
                file_size: result?,
            })
        })
    }

    /// Handle a `DownloadFile` command from the public API.
    pub(crate) async fn handle_download_file(
        self: &Arc<Self>,
        file_info: XetFileInfo,
        dest_path: PathBuf,
    ) -> Result<TaskHandle, SessionError> {
        self.check_accepting_tasks()?;

        let tracking_id = Ulid::new();
        let status = Arc::new(Mutex::new(TaskStatus::Queued));

        let task_handle = TaskHandle {
            status: status.clone(),
            group_progress: self.progress.clone(),
            tracking_id,
        };

        let download_session_arc = self.get_or_create_download_session().await?;

        let join_handle =
            self.spawn_download_task(download_session_arc, file_info.clone(), dest_path.clone(), status, tracking_id);

        let handle = InnerDownloadTaskHandle { dest_path, join_handle };

        self.active_tasks.write()?.insert(tracking_id, handle);

        Ok(task_handle)
    }

    /// Handle a `Finish` command from the public API.
    pub(crate) async fn handle_finish(self: &Arc<Self>) -> Result<Vec<DownloadResult>, SessionError> {
        // Mark as not accepting new tasks
        {
            let mut state_guard = self.state.lock()?;
            if *state_guard == GroupState::Finished {
                return Err(SessionError::AlreadyFinished);
            }
            *state_guard = GroupState::Aborted; // stop new tasks while draining
        }

        // Wait for all downloads to complete
        let handles: Vec<_> = {
            let mut tasks = self.active_tasks.write()?;
            tasks.drain().collect()
        };

        let mut results = Vec::new();
        for (_task_id, handle) in handles {
            let file_info = handle.join_handle.await.map_err(SessionError::TaskJoinError)??;

            results.push(DownloadResult {
                dest_path: handle.dest_path,
                file_info,
            });
        }

        // Mark as finished
        *self.state.lock()? = GroupState::Finished;

        // Unregister from session
        self.session.finish_download_group(self.group_id)?;

        Ok(results)
    }
}

/// A progress snapshot for a single queued download.
///
/// Returned by [`DownloadGroup::get_progress`].
#[derive(Clone, Debug)]
pub struct DownloadProgress {
    /// Unique identifier for this download task.
    pub task_id: Ulid,
    /// Local path where the file will be written.
    pub dest_path: PathBuf,
    /// Content-addressed hash of the file being downloaded.
    pub file_hash: String,
    /// Number of bytes downloaded so far.
    pub bytes_completed: u64,
    /// Total file size in bytes (0 if not yet known).
    pub bytes_total: u64,
    /// Current lifecycle state of the task.
    pub status: TaskStatus,
    /// Instantaneous download throughput in bytes per second.
    pub speed_bps: f64,
}

/// Per-file result returned by [`DownloadGroup::finish`].
#[derive(Clone, Debug)]
pub struct DownloadResult {
    /// Local path where the file was written.
    pub dest_path: PathBuf,
    /// Xet file hash and size of the downloaded file.
    pub file_info: XetFileInfo,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::XetSession;

    #[test]
    fn test_group_not_finished_initially() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let group = session.new_download_group()?;
        assert!(!group.is_finished());
        Ok(())
    }

    #[test]
    fn test_group_has_unique_id() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let g1 = session.new_download_group()?;
        let g2 = session.new_download_group()?;
        assert_ne!(g1.id(), g2.id());
        Ok(())
    }

    #[test]
    fn test_group_clone_shares_id() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let group = session.new_download_group()?;
        let group2 = group.clone();
        assert_eq!(group.id(), group2.id());
        Ok(())
    }

    #[test]
    fn test_download_file_on_aborted_session_returns_error() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let group = session.new_download_group()?;
        session.abort().unwrap();
        let err = group
            .download_file(
                XetFileInfo {
                    hash: "abc123".to_string(),
                    file_size: 1024,
                },
                PathBuf::from("dest.bin"),
            )
            .unwrap_err();
        assert!(matches!(err, SessionError::Aborted));
        Ok(())
    }

    #[test]
    fn test_get_progress_empty_initially() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let group = session.new_download_group()?;
        let snapshot = group.get_progress()?;
        let total = snapshot.total();
        assert_eq!(total.total_bytes, 0);
        assert_eq!(total.total_bytes_completed, 0);
        Ok(())
    }

    #[test]
    fn test_finish_empty_succeeds() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let group = session.new_download_group()?;
        let results = group.finish()?;
        assert!(results.is_empty());
        Ok(())
    }

    #[test]
    fn test_finish_marks_as_finished() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let group = session.new_download_group()?;
        let group_clone = group.clone();
        group.finish().unwrap();
        assert!(group_clone.is_finished());
        Ok(())
    }

    #[test]
    fn test_second_finish_fails() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let g1 = session.new_download_group()?;
        let g2 = g1.clone();
        g1.finish()?;
        let err = g2.finish().unwrap_err();
        assert!(matches!(err, SessionError::AlreadyFinished | SessionError::Other(_)));
        Ok(())
    }

    #[test]
    fn test_finish_unregisters_from_session() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let group = session.new_download_group()?;
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
        group.finish().unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 0);
        Ok(())
    }
}
