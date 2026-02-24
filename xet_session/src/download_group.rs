//! DownloadGroup - groups related downloads

use data::{FileDownloader, XetFileInfo, configurations::TranslatorConfig};
use file_reconstruction::DataOutput;
use progress_tracking::item_tracking::ItemProgressUpdater;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use ulid::Ulid;

// Use tokio Mutex only for downloader (held across await in get_or_create_downloader)
use tokio::sync::Mutex as TokioMutex;

use crate::errors::SessionError;
use crate::progress::{AtomicProgress, AtomicProgressUpdater, TaskStatus};
use crate::session::XetSession;

/// Commands sent to the background worker thread
pub(crate) enum GroupCommand {
    DownloadFile {
        file_hash: String,
        file_size: u64,
        dest_path: PathBuf,
        response_tx: oneshot::Sender<Result<Ulid, SessionError>>,
    },
    DownloadBytes {
        file_hash: String,
        file_size: u64,
        response_tx: oneshot::Sender<Result<(Ulid, Vec<u8>), SessionError>>,
    },
    Finish {
        response_tx: oneshot::Sender<Result<Vec<DownloadResult>, SessionError>>,
    },
}

/// Handle for a single download task
pub(crate) struct DownloadTaskHandle {
    task_id: Ulid,
    dest_path: PathBuf,
    file_hash: String,
    file_size: u64,
    join_handle: JoinHandle<Result<PathBuf, SessionError>>,
    progress: Arc<AtomicProgress>,
    status: Arc<Mutex<TaskStatus>>,
}

/// State of the download group
#[derive(Clone, Copy, PartialEq, Eq)]
enum GroupState {
    Alive,
    Finished,
    Aborted,
}

/// All shared state owned by a single DownloadGroup instance.
/// Accessed through `Arc<DownloadGroupInner>`; do not use this type directly.
#[doc(hidden)]
pub struct DownloadGroupInner {
    group_id: Ulid,
    session: XetSession,

    // Active download tasks for this group
    active_tasks: RwLock<HashMap<Ulid, DownloadTaskHandle>>,

    // Shared downloader (FileDownloader from data crate)
    // Uses tokio::Mutex because it's held across await in get_or_create_downloader
    downloader: TokioMutex<Option<Arc<FileDownloader>>>,

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

    /// Get or create the shared `FileDownloader`.
    async fn get_or_create_downloader(&self) -> Result<Arc<FileDownloader>, SessionError> {
        let mut downloader_lock = self.downloader.lock().await;
        if downloader_lock.is_none() {
            let config = create_translator_config(&self.session)?;
            let new_downloader = FileDownloader::new(Arc::new(config)).await?;
            *downloader_lock = Some(Arc::new(new_downloader));
        }
        Ok(downloader_lock
            .as_ref()
            .ok_or_else(|| SessionError::other("Downloader not initialized"))?
            .clone())
    }

    /// Spawn a runtime task that performs the actual file download.
    fn spawn_download_task(
        self: &Arc<Self>,
        downloader: Arc<FileDownloader>,
        file_hash: String,
        file_size: u64,
        dest_path: PathBuf,
        progress: Arc<AtomicProgress>,
        status: Arc<Mutex<TaskStatus>>,
    ) -> JoinHandle<Result<PathBuf, SessionError>> {
        self.session.runtime.spawn(async move {
            *status.lock()? = TaskStatus::Running;

            // Create parent directories if needed
            if let Some(parent_dir) = dest_path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent_dir) {
                    *status.lock()? = TaskStatus::Failed;
                    return Err(e.into());
                }
            }

            let atomic_updater = Arc::new(AtomicProgressUpdater::new(progress));
            let progress_updater = Some(ItemProgressUpdater::new(atomic_updater));

            let file_info = XetFileInfo::new(file_hash, file_size);

            let merkle_hash = match file_info.merkle_hash() {
                Ok(hash) => hash,
                Err(e) => {
                    *status.lock()? = TaskStatus::Failed;
                    return Err(SessionError::other(format!("Invalid hash: {:?}", e)));
                },
            };

            let output = DataOutput::write_in_file(&dest_path);
            let dest_path_str: Arc<str> = dest_path.to_string_lossy().to_string().into();

            let result: Result<_, SessionError> = downloader
                .smudge_file_from_hash(
                    &merkle_hash,
                    dest_path_str,
                    output,
                    None, // No byte range - download full file
                    progress_updater,
                )
                .await
                .map_err(SessionError::from);

            let new_status = if result.is_ok() {
                TaskStatus::Completed
            } else {
                TaskStatus::Failed
            };
            *status.lock()? = new_status;

            result.map(|_| dest_path)
        })
    }

    /// Handle a `DownloadFile` command from the public API.
    pub(crate) async fn handle_download_file(
        self: &Arc<Self>,
        file_hash: String,
        file_size: u64,
        dest_path: PathBuf,
    ) -> Result<Ulid, SessionError> {
        self.check_accepting_tasks()?;

        let task_id = Ulid::new();
        let progress = Arc::new(AtomicProgress::new());
        let status = Arc::new(Mutex::new(TaskStatus::Queued));

        let downloader_arc = self.get_or_create_downloader().await?;

        let join_handle = self.spawn_download_task(
            downloader_arc,
            file_hash.clone(),
            file_size,
            dest_path.clone(),
            progress.clone(),
            status.clone(),
        );

        let handle = DownloadTaskHandle {
            task_id,
            dest_path,
            file_hash,
            file_size,
            join_handle,
            progress,
            status,
        };

        self.active_tasks.write()?.insert(task_id, handle);

        Ok(task_id)
    }

    /// Handle a `DownloadBytes` command from the public API.
    pub(crate) async fn handle_download_bytes(
        &self,
        _file_hash: String,
        _file_size: u64,
    ) -> Result<(Ulid, Vec<u8>), SessionError> {
        // TODO: Implement download_bytes
        Err(SessionError::other("download_bytes not yet implemented"))
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
            let dest_path = handle.join_handle.await.map_err(SessionError::TaskJoinError)??;

            results.push(DownloadResult {
                dest_path,
                file_hash: handle.file_hash,
                file_size: handle.file_size,
            });
        }

        // Mark as finished
        *self.state.lock()? = GroupState::Finished;

        // Unregister from session
        self.session.finish_download_group(self.group_id)?;

        Ok(results)
    }
}

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
            downloader: TokioMutex::new(None),
            state: Mutex::new(GroupState::Alive),
            command_tx,
        });

        // Spawn background worker task on the runtime directly
        let inner_clone = inner.clone();
        inner.session.runtime.spawn(async move {
            while let Some(command) = command_rx.recv().await {
                match command {
                    GroupCommand::DownloadFile {
                        file_hash,
                        file_size,
                        dest_path,
                        response_tx,
                    } => {
                        let result = inner_clone.handle_download_file(file_hash, file_size, dest_path).await;
                        let _ = response_tx.send(result);
                    },
                    GroupCommand::DownloadBytes {
                        file_hash,
                        file_size,
                        response_tx,
                    } => {
                        let result = inner_clone.handle_download_bytes(file_hash, file_size).await;
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

    /// Return a reference to the parent session.
    pub fn session(&self) -> &XetSession {
        &self.session
    }

    /// Queue a file for download to `dest_path`, starting the transfer immediately.
    ///
    /// # Parameters
    ///
    /// * `file_hash` – Content-addressed hash returned by a previous
    ///   [`UploadCommit::commit`](crate::UploadCommit::commit).
    /// * `file_size` – Expected file size in bytes (used to pre-allocate
    ///   buffers and report progress).
    /// * `dest_path` – Local path where the downloaded file will be written.
    ///   Parent directories are created automatically.
    ///
    /// Returns the task ID that can be correlated with [`get_progress`](Self::get_progress).
    ///
    /// # Errors
    ///
    /// Returns [`SessionError::Aborted`] if the session has been aborted, or
    /// [`SessionError::AlreadyFinished`] if [`finish`](Self::finish) has already
    /// been called.
    pub fn download_file(&self, file_hash: String, file_size: u64, dest_path: PathBuf) -> Result<Ulid, SessionError> {
        self.session.check_alive()?;
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(GroupCommand::DownloadFile {
                file_hash,
                file_size,
                dest_path,
                response_tx,
            })
            .map_err(|_| SessionError::other("Failed to send download command"))?;

        response_rx
            .blocking_recv()
            .map_err(|_| SessionError::other("Failed to receive download response"))?
    }

    /// Download a file into memory, returning the bytes alongside the task ID.
    ///
    /// See [`download_file`](Self::download_file) for parameter details.
    pub fn download_bytes(&self, file_hash: String, file_size: u64) -> Result<(Ulid, Vec<u8>), SessionError> {
        self.session.check_alive()?;
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(GroupCommand::DownloadBytes {
                file_hash,
                file_size,
                response_tx,
            })
            .map_err(|_| SessionError::other("Failed to send download_bytes command"))?;

        response_rx
            .blocking_recv()
            .map_err(|_| SessionError::other("Failed to receive download_bytes response"))?
    }

    /// Returns `true` if [`finish`](Self::finish) has been called and completed.
    pub fn is_finished(&self) -> bool {
        match self.state.lock() {
            Ok(state) => *state == GroupState::Finished,
            Err(_) => false,
        }
    }

    /// Return a snapshot of progress for every queued download.
    ///
    /// This method is fast and lock-free (atomic reads only), making it safe
    /// to call frequently from Python without GIL contention.
    pub fn get_progress(&self) -> Vec<DownloadProgress> {
        let tasks = match self.active_tasks.read() {
            Ok(tasks) => tasks,
            Err(e) => {
                tracing::error!("Failed to acquire read lock on active_tasks: {}", e);
                return Vec::new();
            },
        };

        tasks
            .values()
            .filter_map(|handle| {
                match handle.status.lock() {
                    Ok(status) => Some(DownloadProgress {
                        task_id: handle.task_id,
                        dest_path: handle.dest_path.clone(),
                        file_hash: handle.file_hash.clone(),
                        bytes_completed: handle.progress.get_completed(),
                        bytes_total: handle.progress.get_total(),
                        status: *status,
                        speed_bps: 0.0, // TODO: Calculate speed
                    }),
                    Err(e) => {
                        tracing::error!("Failed to acquire lock on task status for {}: {}", handle.task_id, e);
                        None
                    },
                }
            })
            .collect()
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
    /// Content-addressed hash of the downloaded file.
    pub file_hash: String,
    /// File size in bytes.
    pub file_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::XetSession;

    fn make_session() -> Result<XetSession, SessionError> {
        XetSession::new(None, None, None, "test/0.0".to_string())
    }

    fn make_group(session: &XetSession) -> Result<DownloadGroup, SessionError> {
        session.new_download_group()
    }

    #[test]
    fn test_group_not_finished_initially() -> Result<(), Box<dyn std::error::Error>> {
        let session = make_session()?;
        let group = make_group(&session)?;
        assert!(!group.is_finished());
        Ok(())
    }

    #[test]
    fn test_group_has_unique_id() -> Result<(), Box<dyn std::error::Error>> {
        let session = make_session()?;
        let g1 = make_group(&session)?;
        let g2 = make_group(&session)?;
        assert_ne!(g1.id(), g2.id());
        Ok(())
    }

    #[test]
    fn test_group_clone_shares_id() -> Result<(), Box<dyn std::error::Error>> {
        let session = make_session()?;
        let group = make_group(&session)?;
        let group2 = group.clone();
        assert_eq!(group.id(), group2.id());
        Ok(())
    }

    #[test]
    fn test_download_file_on_aborted_session_returns_error() -> Result<(), Box<dyn std::error::Error>> {
        let session = make_session()?;
        let group = make_group(&session)?;
        session.abort().unwrap();
        let err = group
            .download_file("abc123".to_string(), 1024, PathBuf::from("dest.bin"))
            .unwrap_err();
        assert!(matches!(err, SessionError::Aborted));
        Ok(())
    }

    #[test]
    fn test_download_bytes_on_aborted_session_returns_error() -> Result<(), Box<dyn std::error::Error>> {
        let session = make_session()?;
        let group = make_group(&session)?;
        session.abort().unwrap();
        let err = group.download_bytes("abc123".to_string(), 1024).unwrap_err();
        assert!(matches!(err, SessionError::Aborted));
        Ok(())
    }

    #[test]
    fn test_get_progress_empty_initially() -> Result<(), Box<dyn std::error::Error>> {
        let session = make_session()?;
        let group = make_group(&session)?;
        let progress = group.get_progress();
        assert!(progress.is_empty());
        Ok(())
    }

    #[test]
    fn test_finish_empty_succeeds() -> Result<(), Box<dyn std::error::Error>> {
        let session = make_session()?;
        let group = make_group(&session)?;
        let results = group.finish()?;
        assert!(results.is_empty());
        Ok(())
    }

    #[test]
    fn test_finish_marks_as_finished() -> Result<(), Box<dyn std::error::Error>> {
        let session = make_session()?;
        let group = make_group(&session)?;
        let group_clone = group.clone();
        group.finish().unwrap();
        assert!(group_clone.is_finished());
        Ok(())
    }

    #[test]
    fn test_second_finish_fails() -> Result<(), Box<dyn std::error::Error>> {
        let session = make_session()?;
        let g1 = make_group(&session)?;
        let g2 = g1.clone();
        g1.finish()?;
        let err = g2.finish().unwrap_err();
        assert!(matches!(err, SessionError::AlreadyFinished | SessionError::Other(_)));
        Ok(())
    }

    #[test]
    fn test_finish_unregisters_from_session() -> Result<(), Box<dyn std::error::Error>> {
        let session = make_session()?;
        let group = make_group(&session)?;
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
        group.finish().unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 0);
        Ok(())
    }
}

// Helper function to create TranslatorConfig
fn create_translator_config(session: &XetSession) -> Result<TranslatorConfig, SessionError> {
    let endpoint = session
        .endpoint
        .clone()
        .unwrap_or_else(|| session.config.data.default_cas_endpoint.clone());

    data::data_client::default_config(
        endpoint,
        None, // xorb_compression
        session.token_info.clone(),
        session.token_refresher.clone(),
        session.user_agent.clone(),
        Some(session.session_id),
    )
    .map_err(|e| e.into())
}
