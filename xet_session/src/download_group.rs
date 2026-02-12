//! DownloadGroup - groups related downloads

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use tokio::task::JoinHandle;

// Use tokio Mutex only for downloader (held across await in get_or_create_downloader)
use tokio::sync::Mutex as TokioMutex;
use ulid::Ulid;
use data::{XetFileInfo, configurations::TranslatorConfig, FileDownloader};
use data::errors::DataProcessingError;
use file_reconstruction::DataOutput;
use progress_tracking::item_tracking::ItemProgressUpdater;

use crate::session::XetSession;
use crate::errors::SessionError;
use crate::progress::{AtomicProgress, TaskStatus, AtomicProgressUpdater};

/// Handle for a single download task
struct DownloadTaskHandle {
    task_id: Ulid,
    dest_path: String,
    file_hash: String,
    file_size: u64,
    join_handle: JoinHandle<Result<String, DataProcessingError>>,
    progress: Arc<AtomicProgress>,
    status: Arc<Mutex<TaskStatus>>,
}

/// State of the download group
struct GroupState {
    accepting_tasks: bool,
    finished: bool,
}

/// Groups related downloads
#[derive(Clone)]
pub struct DownloadGroup {
    group_id: Ulid,
    session: Arc<XetSession>,

    // Active download tasks for this group
    active_tasks: Arc<RwLock<HashMap<Ulid, DownloadTaskHandle>>>,

    // Shared downloader (FileDownloader from data crate)
    // Uses tokio::Mutex because it's held across await in get_or_create_downloader
    downloader: Arc<TokioMutex<Option<Arc<FileDownloader>>>>,

    // State
    state: Arc<Mutex<GroupState>>,
}

impl DownloadGroup {
    /// Create a new download group
    pub(crate) fn new(session: Arc<XetSession>) -> Result<Self, SessionError> {
        Ok(Self {
            group_id: Ulid::new(),
            session,
            active_tasks: Arc::new(RwLock::new(HashMap::new())),
            downloader: Arc::new(TokioMutex::new(None)),
            state: Arc::new(Mutex::new(GroupState {
                accepting_tasks: true,
                finished: false,
            })),
        })
    }

    /// Get the group ID
    pub(crate) fn group_id(&self) -> Ulid {
        self.group_id
    }

    /// Get the session (for Python bindings to access runtime)
    pub fn session(&self) -> &Arc<XetSession> {
        &self.session
    }

    /// Download file - starts immediately, returns task ID
    pub async fn download_file(
        &self,
        file_hash: String,
        file_size: u64,
        dest_path: String,
    ) -> Result<Ulid, SessionError> {
        // Check state
        {
            let state = self.state.lock()?;
            if state.finished {
                return Err(SessionError::AlreadyFinished);
            }
            if !state.accepting_tasks {
                return Err(SessionError::NotAcceptingTasks);
            }
        }

        let task_id = Ulid::new();

        // Create atomic progress tracker
        let progress = Arc::new(AtomicProgress::new());
        let status = Arc::new(Mutex::new(TaskStatus::Queued));

        // Clone for task
        let file_hash_clone = file_hash.clone();
        let dest_path_clone = dest_path.clone();
        let progress_clone = progress.clone();
        let status_clone = status.clone();

        // Get or create downloader
        let downloader = self.get_or_create_downloader().await?;

        // Spawn task IMMEDIATELY
        let join_handle = self.session.runtime().spawn(async move {
            *status_clone.lock().expect("Status lock poisoned") = TaskStatus::Running;

            // Create parent directories if needed
            let path = PathBuf::from(&dest_path_clone);
            if let Some(parent_dir) = path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent_dir) {
                    *status_clone.lock().expect("Status lock poisoned") = TaskStatus::Failed;
                    return Err(DataProcessingError::InternalError(format!("Failed to create parent directory: {}", e)));
                }
            }

            // Create progress updater that updates atomic variables
            let atomic_updater = Arc::new(AtomicProgressUpdater::new(progress_clone));

            // Wrap in ItemProgressUpdater
            let progress_updater = Some(ItemProgressUpdater::new(atomic_updater));

            // Create XetFileInfo
            let file_info = XetFileInfo::new(file_hash_clone.clone(), file_size);

            // Get MerkleHash from file_info
            let merkle_hash = match file_info.merkle_hash() {
                Ok(hash) => hash,
                Err(e) => {
                    *status_clone.lock().expect("Status lock poisoned") = TaskStatus::Failed;
                    return Err(DataProcessingError::InternalError(format!("Invalid hash: {:?}", e)));
                }
            };

            // Create DataOutput for the destination path
            let output = DataOutput::write_in_file(&path);

            // Execute download using FileDownloader
            let result = downloader.smudge_file_from_hash(
                &merkle_hash,
                dest_path_clone.clone().into(),
                output,
                None, // No byte range - download full file
                progress_updater,
            ).await;

            match &result {
                Ok(_) => *status_clone.lock().expect("Status lock poisoned") = TaskStatus::Completed,
                Err(_) => *status_clone.lock().expect("Status lock poisoned") = TaskStatus::Failed,
            }

            result.map(|_| dest_path_clone)
        });

        // Register task
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

    /// Download bytes - starts immediately, returns task ID and future bytes
    pub async fn download_bytes(
        &self,
        _file_hash: String,
        _file_size: u64,
    ) -> Result<(Ulid, Vec<u8>), SessionError> {
        // TODO: Implement download_bytes
        Err(SessionError::Other("download_bytes not yet implemented".to_string()))
    }

    /// Get progress for all downloads in this group
    pub fn get_progress(&self) -> Vec<DownloadProgress> {
        let tasks = match self.active_tasks.read() {
            Ok(tasks) => tasks,
            Err(e) => {
                tracing::error!("Failed to acquire read lock on active_tasks: {}", e);
                return Vec::new();
            }
        };

        tasks.values().filter_map(|handle| {
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
                }
            }
        }).collect()
    }

    /// Finish - wait for all downloads, return metadata
    pub async fn finish(self) -> Result<Vec<DownloadResult>, SessionError> {
        // Mark as not accepting new tasks
        {
            let mut state = self.state.lock()?;
            if state.finished {
                return Err(SessionError::AlreadyFinished);
            }
            state.accepting_tasks = false;
        }

        // Wait for all downloads to complete
        // Collect handles first, drop lock, then await
        let handles: Vec<_> = {
            let mut tasks = self.active_tasks.write()?;
            tasks.drain().collect()
        };

        let mut results = Vec::new();
        for (_task_id, handle) in handles {
            let dest_path = handle.join_handle.await
                .map_err(|e| SessionError::TaskJoinError(e))??;

            results.push(DownloadResult {
                dest_path,
                file_hash: handle.file_hash,
                file_size: handle.file_size,
            });
        }

        // Mark as finished
        {
            let mut state = self.state.lock()?;
            state.finished = true;
        }

        // Unregister from session
        self.session.unregister_group(self.group_id);

        Ok(results)
    }

    // Helper to get or create downloader
    async fn get_or_create_downloader(&self) -> Result<Arc<FileDownloader>, SessionError> {
        let mut downloader_lock = self.downloader.lock().await;
        if downloader_lock.is_none() {
            // Create TranslatorConfig
            let config = create_translator_config(&self.session)?;

            // Create new FileDownloader
            let new_downloader = FileDownloader::new(Arc::new(config)).await?;
            *downloader_lock = Some(Arc::new(new_downloader));
        }
        // downloader_lock.as_ref() returns Option<&Arc<FileDownloader>>
        // We just set it above, so it's guaranteed to be Some
        Ok(downloader_lock.as_ref()
            .expect("Downloader should be initialized")
            .clone())
    }
}

/// Progress for a single download task
#[derive(Clone, Debug)]
pub struct DownloadProgress {
    pub task_id: Ulid,
    pub dest_path: String,
    pub file_hash: String,
    pub bytes_completed: u64,
    pub bytes_total: u64,
    pub status: TaskStatus,
    pub speed_bps: f64,
}

/// Result returned after finish
#[derive(Clone, Debug)]
pub struct DownloadResult {
    pub dest_path: String,
    pub file_hash: String,
    pub file_size: u64,
}

// Helper function to create TranslatorConfig
fn create_translator_config(session: &XetSession) -> Result<TranslatorConfig, SessionError> {
    let endpoint = session.endpoint()
        .clone()
        .unwrap_or_else(|| session.config().data.default_cas_endpoint.clone());

    data::data_client::default_config(
        endpoint,
        None, // xorb_compression
        session.token_info().clone(),
        session.token_refresher().clone(),
        "xet-session/0.1.0".to_string(), // user_agent
    ).map_err(|e| e.into())
}
