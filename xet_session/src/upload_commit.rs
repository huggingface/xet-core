//! UploadCommit - groups related uploads

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use tokio::task::JoinHandle;
use ulid::Ulid;
use data::{XetFileInfo, configurations::TranslatorConfig, FileUploadSession};
use data::errors::DataProcessingError;

// Use tokio Mutex only for upload_session (held across await in get_or_create_upload_session)
use tokio::sync::Mutex as TokioMutex;

use crate::session::XetSession;
use crate::errors::SessionError;
use crate::progress::{AtomicProgress, TaskStatus, AtomicProgressUpdater};

/// Handle for a single upload task
struct UploadTaskHandle {
    task_id: Ulid,
    file_name: Option<String>,
    join_handle: JoinHandle<Result<XetFileInfo, DataProcessingError>>,
    progress: Arc<AtomicProgress>,
    status: Arc<Mutex<TaskStatus>>,
}

/// State of the upload commit
struct CommitState {
    accepting_tasks: bool,
    committed: bool,
}

/// Groups related uploads (like git staging + commit)
#[derive(Clone)]
pub struct UploadCommit {
    commit_id: Ulid,
    session: Arc<XetSession>,

    // Active upload tasks for this commit
    active_tasks: Arc<RwLock<HashMap<Ulid, UploadTaskHandle>>>,

    // Shared upload session (FileUploadSession from data crate)
    // Uses tokio::Mutex because it's held across await in get_or_create_upload_session
    upload_session: Arc<TokioMutex<Option<Arc<FileUploadSession>>>>,

    // State
    state: Arc<Mutex<CommitState>>,
}

impl UploadCommit {
    /// Create a new upload commit
    pub(crate) fn new(session: Arc<XetSession>) -> Result<Self, SessionError> {
        Ok(Self {
            commit_id: Ulid::new(),
            session,
            active_tasks: Arc::new(RwLock::new(HashMap::new())),
            upload_session: Arc::new(TokioMutex::new(None)),
            state: Arc::new(Mutex::new(CommitState {
                accepting_tasks: true,
                committed: false,
            })),
        })
    }

    /// Get the commit ID
    pub(crate) fn commit_id(&self) -> Ulid {
        self.commit_id
    }

    /// Get the session (for Python bindings to access runtime)
    pub fn session(&self) -> &Arc<XetSession> {
        &self.session
    }

    /// Upload a file - starts immediately, returns task ID
    pub async fn upload_file(&self, file_path: String) -> Result<Ulid, SessionError> {
        // Check state
        {
            let state = self.state.lock()?;
            if state.committed {
                return Err(SessionError::AlreadyCommitted);
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
        let file_path_clone = file_path.clone();
        let progress_clone = progress.clone();
        let status_clone = status.clone();

        // Get or create upload session
        let upload_session = self.get_or_create_upload_session().await?;

        // Spawn task IMMEDIATELY
        let join_handle = self.session.runtime().spawn(async move {
            // Note: Inside spawned task, lock failures are fatal - we can use expect()
            *status_clone.lock().expect("Status lock poisoned") = TaskStatus::Running;

            // Create progress updater that updates atomic variables
            let _atomic_updater = Arc::new(AtomicProgressUpdater::new(progress_clone)) as Arc<_>;

            // Execute upload using existing FileUploadSession
            // Note: FileUploadSession::upload_files expects an iterator of (path, Option<Sha256>)
            let files = vec![(file_path_clone.as_ref() as &Path, None)];

            let result = upload_session.upload_files(files).await
                .and_then(|mut results| {
                    results.pop().ok_or_else(|| {
                        DataProcessingError::InternalError("No upload result".to_string())
                    })
                });

            match &result {
                Ok(_) => *status_clone.lock().expect("Status lock poisoned") = TaskStatus::Completed,
                Err(_) => *status_clone.lock().expect("Status lock poisoned") = TaskStatus::Failed,
            }

            result
        });

        // Register task
        let handle = UploadTaskHandle {
            task_id,
            file_name: Some(file_path),
            join_handle,
            progress,
            status,
        };

        self.active_tasks.write()?.insert(task_id, handle);

        Ok(task_id)
    }

    /// Upload bytes - starts immediately, returns task ID
    pub async fn upload_bytes(&self, _bytes: Vec<u8>) -> Result<Ulid, SessionError> {
        // TODO: Implement upload_bytes
        // For now, return an error
        Err(SessionError::Other("upload_bytes not yet implemented".to_string()))
    }

    /// Get progress for all uploads in this commit
    pub fn get_progress(&self) -> Vec<UploadProgress> {
        let tasks = match self.active_tasks.read() {
            Ok(tasks) => tasks,
            Err(e) => {
                tracing::error!("Failed to acquire read lock on active_tasks: {}", e);
                return Vec::new();
            }
        };

        tasks.values().filter_map(|handle| {
            match handle.status.lock() {
                Ok(status) => Some(UploadProgress {
                    task_id: handle.task_id,
                    file_name: handle.file_name.clone(),
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

    /// Commit - finalize all uploads, push to remote, return metadata
    pub async fn commit(self) -> Result<Vec<FileMetadata>, SessionError> {
        // Mark as not accepting new tasks
        {
            let mut state = self.state.lock()?;
            if state.committed {
                return Err(SessionError::AlreadyCommitted);
            }
            state.accepting_tasks = false;
        }

        // Wait for all uploads to complete
        // Collect handles first, drop lock, then await
        let handles: Vec<_> = {
            let mut tasks = self.active_tasks.write()?;
            tasks.drain().collect()
        };

        let mut results = Vec::new();
        for (_task_id, handle) in handles {
            let file_info = handle.join_handle.await
                .map_err(|e| SessionError::TaskJoinError(e))??;

            results.push(FileMetadata {
                file_name: handle.file_name,
                hash: file_info.hash().to_string(),
                file_size: file_info.file_size(),
            });
        }

        // Finalize upload session (flush remaining data to remote)
        if let Some(upload_session) = self.upload_session.lock().await.take() {
            upload_session.finalize().await?;
        }

        // Mark as committed
        {
            let mut state = self.state.lock()?;
            state.committed = true;
        }

        // Unregister from session
        self.session.unregister_commit(self.commit_id);

        Ok(results)
    }

    // Helper to get or create upload session
    async fn get_or_create_upload_session(&self) -> Result<Arc<FileUploadSession>, SessionError> {
        let mut session_lock = self.upload_session.lock().await;
        if session_lock.is_none() {
            // Create TranslatorConfig
            let config = create_translator_config(&self.session)?;

            // Create new FileUploadSession (returns Arc<FileUploadSession>)
            let new_session = FileUploadSession::new(
                Arc::new(config),
                None, // No session-wide progress callback
            ).await?;
            *session_lock = Some(new_session);
        }
        // session_lock.as_ref() returns Option<&Arc<FileUploadSession>>
        // We just set it above, so it's guaranteed to be Some
        Ok(session_lock.as_ref()
            .expect("Upload session should be initialized")
            .clone())
    }
}

/// Progress for a single upload task
#[derive(Clone, Debug)]
pub struct UploadProgress {
    pub task_id: Ulid,
    pub file_name: Option<String>,
    pub bytes_completed: u64,
    pub bytes_total: u64,
    pub status: TaskStatus,
    pub speed_bps: f64,
}

/// Metadata returned after commit
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FileMetadata {
    pub file_name: Option<String>,
    pub hash: String,
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
