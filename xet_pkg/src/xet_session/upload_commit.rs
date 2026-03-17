//! UploadCommit - groups related uploads

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use tokio::task::JoinHandle;
use ulid::Ulid;
use xet_data::processing::data_client::{clean_bytes, clean_file};
use xet_data::processing::{FileUploadSession, Sha256Policy, SingleFileCleaner, XetFileInfo};
use xet_runtime::core::XetRuntime;

use super::common::{GroupState, create_translator_config};
use super::progress::{GroupProgress, ProgressSnapshot, TaskHandle, TaskStatus, UploadTaskHandle};
use super::session::XetSession;
use crate::error::XetError;

/// API for grouping related file uploads into a single atomic commit.
///
/// Obtain via [`XetSession::new_upload_commit`] (async) or
/// [`XetSession::new_upload_commit_blocking`] (sync).
///
/// Enqueue files with [`upload_from_path`](Self::upload_from_path) /
/// [`upload_from_path_blocking`](Self::upload_from_path_blocking) or stream
/// bytes with [`upload_file`](Self::upload_file) /
/// [`upload_file_blocking`](Self::upload_file_blocking) — transfers start
/// immediately in the background.  Poll progress with
/// [`get_progress`](Self::get_progress), then call
/// [`commit`](Self::commit) (async) or
/// [`commit_blocking`](Self::commit_blocking) (sync) to wait for all uploads
/// to finish and push the final metadata to the CAS server.
///
/// # Cloning
///
/// Cloning is cheap — it simply increments an atomic reference count.
/// All clones share the same upload session and task state.
///
/// # Errors
///
/// Methods return [`XetError::Aborted`] if the parent session has been
/// aborted, and [`XetError::AlreadyCommitted`] if [`commit`](Self::commit)
/// has already been called.
#[derive(Clone)]
pub struct UploadCommit {
    pub(super) inner: Arc<UploadCommitInner>,
}

impl std::ops::Deref for UploadCommit {
    type Target = UploadCommitInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl UploadCommit {
    /// Create a new upload commit from an **async** context. Initialisation logic shared by the sync and async
    /// constructors.
    pub(super) async fn new(session: XetSession) -> Result<Self, XetError> {
        let commit_id = Ulid::new();
        let progress = Arc::new(GroupProgress::new());
        let config = create_translator_config(&session)?;
        let progress_updater = progress.clone() as Arc<dyn xet_data::progress_tracking::TrackingProgressUpdater>;
        let upload_session = FileUploadSession::new(Arc::new(config), Some(progress_updater)).await?;

        let inner = Arc::new(UploadCommitInner {
            commit_id,
            session,
            active_tasks: RwLock::new(HashMap::new()),
            progress,
            upload_session: Mutex::new(Some(upload_session)),
            state: tokio::sync::Mutex::new(GroupState::Alive),
        });

        Ok(Self { inner })
    }

    /// Get the commit ID.
    pub(super) fn id(&self) -> Ulid {
        self.commit_id
    }

    /// Abort this upload commit.
    pub(super) fn abort(&self) -> Result<(), XetError> {
        self.inner.abort()
    }

    /// Returns the runtime used by this commit.
    pub(super) fn runtime(&self) -> &XetRuntime {
        &self.inner.session.runtime
    }

    /// Queue a file for upload, starting the transfer immediately if system resource permits.
    ///
    /// Returns an [`UploadTaskHandle`] that can be used to poll status and per-file
    /// progress without taking the GIL.
    ///
    /// # Parameters
    ///
    /// - `file_path`: path to the file on disk. Resolved to an absolute path internally so the upload is not affected
    ///   by subsequent changes to the process working directory.
    /// - `sha256`: controls SHA-256 handling during upload. Use [`Sha256Policy::Compute`] to compute it from the data,
    ///   [`Sha256Policy::Provided`] to supply a pre-computed digest, or [`Sha256Policy::Skip`] to omit it entirely.
    ///
    /// # Errors
    ///
    /// Returns [`XetError::Aborted`] if the session has been aborted, or
    /// [`XetError::AlreadyCommitted`] if [`commit`](Self::commit) has
    /// already been called.
    pub async fn upload_from_path(
        &self,
        file_path: PathBuf,
        sha256: Sha256Policy,
    ) -> Result<UploadTaskHandle, XetError> {
        self.session.check_alive()?;

        // Use the absolute path in case the process current working directory changes
        // while the task is queued.
        let absolute_path = std::path::absolute(file_path)?;

        let inner = self.inner.clone();
        self.session
            .dispatch("upload_from_path", async move { inner.start_upload_file_from_path(absolute_path, sha256).await })
            .await?
    }

    /// Begin an incremental file upload, returning a [`SingleFileCleaner`] that the
    /// caller uses to stream bytes.
    ///
    /// This is the low-level streaming counterpart to [`upload_from_path`](Self::upload_from_path).
    ///
    /// ```rust,no_run
    /// # use std::fs::File;
    /// # use std::io::Read;
    /// # use xet::XetError;
    /// # async fn example(commit: xet::xet_session::UploadCommit, filename: &str, filesize: u64) -> Result<(), Box<dyn std::error::Error>> {
    /// # use xet::xet_session::Sha256Policy;
    /// let (handle, mut cleaner) = commit.upload_file(Some(filename.into()), filesize, Sha256Policy::Compute).await?;
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
    /// - `file_name`: optional display name used for progress and telemetry reporting; does not affect the upload
    ///   itself.
    /// - `file_size`: expected total size in bytes, used for progress tracking. Pass `0` when the size is not known in
    ///   advance.
    /// - `sha256`: controls SHA-256 handling during upload. Use [`Sha256Policy::Compute`] to compute it from the data,
    ///   [`Sha256Policy::Provided`] to supply a pre-computed digest, or [`Sha256Policy::Skip`] to omit it entirely.
    ///
    /// # Returns
    ///
    /// A `(`[`TaskHandle`]`, `[`SingleFileCleaner`]`)` pair. The [`TaskHandle`] tracks task
    /// lifecycle but carries no upload result — call [`SingleFileCleaner::finish`] on the cleaner
    /// to obtain the [`FileMetadata`](xet_data::processing::FileMetadata) once all bytes have
    /// been streamed.
    pub async fn upload_file(
        &self,
        file_name: Option<String>,
        file_size: u64,
        sha256: Sha256Policy,
    ) -> Result<(TaskHandle, SingleFileCleaner), XetError> {
        self.session.check_alive()?;

        let inner = self.inner.clone();
        self.session
            .dispatch("upload_file", async move { inner.start_upload_file(file_name, file_size, sha256).await })
            .await?
    }

    /// Queue raw bytes for upload, starting the transfer immediately if system resource permits.
    ///
    /// Returns an [`UploadTaskHandle`] that can be used to poll status and retrieve the per-file
    /// result after [`commit`](Self::commit) completes.
    ///
    /// # Parameters
    ///
    /// - `bytes`: the raw byte content to upload.
    /// - `sha256`: controls SHA-256 handling during upload. Use [`Sha256Policy::Compute`] to compute it from the data,
    ///   [`Sha256Policy::Provided`] to supply a pre-computed digest, or [`Sha256Policy::Skip`] to omit it entirely.
    /// - `tracking_name`: optional display name used for progress and telemetry reporting; does not affect the upload
    ///   itself.
    ///
    /// # Errors
    ///
    /// Returns [`XetError::Aborted`] if the session has been aborted, or
    /// [`XetError::AlreadyCommitted`] if [`commit`](Self::commit) has already been called.
    pub async fn upload_bytes(
        &self,
        bytes: Vec<u8>,
        sha256: Sha256Policy,
        tracking_name: Option<String>,
    ) -> Result<UploadTaskHandle, XetError> {
        self.session.check_alive()?;

        let inner = self.inner.clone();
        self.session
            .dispatch("upload_bytes", async move { inner.start_upload_bytes(bytes, sha256, tracking_name).await })
            .await?
    }

    /// Return a snapshot of progress for every queued upload.
    pub fn get_progress(&self) -> Result<ProgressSnapshot, XetError> {
        self.progress.snapshot()
    }

    /// Wait for all uploads to complete and push metadata to the CAS server.
    ///
    /// Returns a `HashMap` keyed by task ID where each value is
    /// [`UploadResult`] (= `Arc<Result<`[`FileMetadata`]`, [`XetError`]`>>`).
    /// A single failed upload does not prevent the others from being collected.
    ///
    /// Consumes `self` — subsequent calls on any clone will return
    /// [`XetError::AlreadyCommitted`].
    pub async fn commit(self) -> Result<HashMap<Ulid, UploadResult>, XetError> {
        let inner = self.inner.clone();
        self.session
            .dispatch("commit", async move { inner.handle_commit().await })
            .await?
    }

    /// Returns `true` if [`commit`](Self::commit) has been called and completed.
    #[cfg(test)]
    async fn is_committed(&self) -> bool {
        *self.state.lock().await == GroupState::Finished
    }

    // ===== Blocking (sync) variants =====
    //
    // These methods block the calling thread via `external_run_async_task`.
    // **Do not call from within a tokio runtime** — it will panic.
    // Use the async counterparts instead.

    /// Blocking version of [`upload_from_path`](Self::upload_from_path).
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn upload_from_path_blocking(
        &self,
        file_path: PathBuf,
        sha256: Sha256Policy,
    ) -> Result<UploadTaskHandle, XetError> {
        self.session.check_alive()?;

        let absolute_path = std::path::absolute(file_path)?;
        let commit_inner = self.inner.clone();
        self.runtime().external_run_async_task(async move {
            commit_inner.start_upload_file_from_path(absolute_path, sha256).await
        })?
    }

    /// Blocking version of [`upload_bytes`](Self::upload_bytes).
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn upload_bytes_blocking(
        &self,
        bytes: Vec<u8>,
        sha256: Sha256Policy,
        tracking_name: Option<String>,
    ) -> Result<UploadTaskHandle, XetError> {
        self.session.check_alive()?;

        let commit_inner = self.inner.clone();
        self.runtime().external_run_async_task(async move {
            commit_inner.start_upload_bytes(bytes, sha256, tracking_name).await
        })?
    }

    /// Blocking version of [`upload_file`](Self::upload_file).
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn upload_file_blocking(
        &self,
        file_name: Option<String>,
        file_size: u64,
        sha256: Sha256Policy,
    ) -> Result<(TaskHandle, SingleFileCleaner), XetError> {
        self.session.check_alive()?;

        let commit_inner = self.inner.clone();
        self.runtime().external_run_async_task(async move {
            commit_inner.start_upload_file(file_name, file_size, sha256).await
        })?
    }

    /// Blocking version of [`commit`](Self::commit).
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn commit_blocking(self) -> Result<HashMap<Ulid, UploadResult>, XetError> {
        let commit = self.clone();
        self.runtime().external_run_async_task(commit.commit())?
    }
}

/// Per-file result type returned by [`UploadCommit::commit`].
///
/// The `Arc` lets the same value be stored in both the `commit()` return map
/// and the per-task [`UploadTaskHandle`] without requiring the inner
/// `Result` to be `Clone`.
pub type UploadResult = Arc<Result<FileMetadata, XetError>>;

/// Handle for a single upload task tracked internally by UploadCommit.
struct InnerUploadTaskHandle {
    status: Arc<Mutex<TaskStatus>>,
    tracking_name: Option<String>,
    join_handle: JoinHandle<Result<XetFileInfo, XetError>>,
    result: Arc<OnceLock<UploadResult>>,
}

/// All shared state owned by a single UploadCommit instance.
/// Accessed through `Arc<UploadCommitInner>`; do not use this type directly.
#[doc(hidden)]
pub struct UploadCommitInner {
    commit_id: Ulid,
    pub(super) session: XetSession,

    // Active upload tasks for this commit
    active_tasks: RwLock<HashMap<Ulid, InnerUploadTaskHandle>>,

    // Aggregate + per-file progress, fed into FileUploadSession as a TrackingProgressUpdater
    progress: Arc<GroupProgress>,

    // Shared upload session (FileUploadSession from data crate)
    upload_session: Mutex<Option<Arc<FileUploadSession>>>,

    // tokio::sync::Mutex (not std) because registration methods hold this lock across
    // .await points (e.g. start_clean in start_upload_file) to serialise with commit.
    // DownloadGroupInner uses std::sync::Mutex because its registration is synchronous.
    state: tokio::sync::Mutex<GroupState>,
}

impl UploadCommitInner {
    // ===== State helpers =====

    /// Check whether the commit is still accepting new tasks.
    fn check_accepting_tasks(state: &GroupState) -> Result<(), XetError> {
        match *state {
            GroupState::Finished => Err(XetError::AlreadyCommitted),
            GroupState::Aborted => Err(XetError::Aborted),
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
        sha256: Sha256Policy,
    ) -> JoinHandle<Result<XetFileInfo, XetError>> {
        let semaphore = self.runtime().common().file_ingestion_semaphore.clone();
        self.runtime().spawn(async move {
            let _permit = semaphore.acquire().await?;

            // Only transition Queued → Running; bail if abort() already set Cancelled.
            {
                let mut s = status.lock()?;
                if !matches!(*s, TaskStatus::Queued) {
                    return Err(XetError::Aborted);
                }
                *s = TaskStatus::Running;
            }

            let result = clean_file(upload_session, &file_path, sha256, Some(tracking_id))
                .await
                .map_err(XetError::from)
                .map(|(file_info, _metrics)| file_info);

            let new_status = if result.is_ok() {
                TaskStatus::Completed
            } else {
                TaskStatus::Failed
            };
            // Only overwrite if still Running — abort() may have set Cancelled concurrently.
            let mut s = status.lock()?;
            if matches!(*s, TaskStatus::Running) {
                *s = new_status;
            }

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
        sha256: Sha256Policy,
    ) -> JoinHandle<Result<XetFileInfo, XetError>> {
        let semaphore = self.runtime().common().file_ingestion_semaphore.clone();
        self.runtime().spawn(async move {
            let _permit = semaphore.acquire().await?;

            // Only transition Queued → Running; bail if abort() already set Cancelled.
            {
                let mut s = status.lock()?;
                if !matches!(*s, TaskStatus::Queued) {
                    return Err(XetError::Aborted);
                }
                *s = TaskStatus::Running;
            }

            let result = clean_bytes(upload_session, bytes, Some(tracking_id), sha256)
                .await
                .map_err(XetError::from)
                .map(|(file_info, _metrics)| file_info);

            let new_status = if result.is_ok() {
                TaskStatus::Completed
            } else {
                TaskStatus::Failed
            };
            // Only overwrite if still Running — abort() may have set Cancelled concurrently.
            let mut s = status.lock()?;
            if matches!(*s, TaskStatus::Running) {
                *s = new_status;
            }

            result
        })
    }

    pub(super) async fn start_upload_file_from_path(
        &self,
        file_path: PathBuf,
        sha256: Sha256Policy,
    ) -> Result<UploadTaskHandle, XetError> {
        // Hold the state lock for the duration of this function so commit() will not run
        // when an upload task is registering.
        let state = self.state.lock().await;
        Self::check_accepting_tasks(&state)?;

        let tracking_id = Ulid::new();
        let status = Arc::new(Mutex::new(TaskStatus::Queued));
        let result: Arc<OnceLock<UploadResult>> = Arc::new(OnceLock::new());
        let task_handle = UploadTaskHandle {
            inner: TaskHandle {
                status: Some(status.clone()),
                group_progress: self.progress.clone(),
                task_id: tracking_id,
            },
            result: result.clone(),
        };

        let Some(upload_session) = self.upload_session.lock()?.clone() else {
            return Err(XetError::other("Upload session not initialized"));
        };

        let join_handle =
            self.spawn_upload_from_path_task(upload_session, file_path.clone(), status.clone(), tracking_id, sha256);

        let handle = InnerUploadTaskHandle {
            status,
            tracking_name: file_path.to_str().map(|s| s.to_owned()),
            join_handle,
            result,
        };

        self.active_tasks.write()?.insert(tracking_id, handle);

        Ok(task_handle)
    }

    /// Begin a streaming upload: check state, then await `start_clean` on the upload session
    /// and return a [`SingleFileCleaner`] that the caller drives incrementally.
    ///
    /// The state lock is held across `start_clean(...).await` so that a concurrent
    /// `handle_commit` cannot finalise the upload session between the state check and the
    /// creation of the cleaner.
    pub(super) async fn start_upload_file(
        &self,
        tracking_name: Option<String>,
        file_size: u64,
        sha256: Sha256Policy,
    ) -> Result<(TaskHandle, SingleFileCleaner), XetError> {
        let tracking_id = Ulid::new();
        // Hold the state lock across start_clean so handle_commit cannot finalise
        // the session between the state check and the creation of the cleaner.
        let state = self.state.lock().await;
        Self::check_accepting_tasks(&state)?;

        let Some(upload_session) = self.upload_session.lock()?.clone() else {
            return Err(XetError::other("Upload session not initialized"));
        };

        let task_handle = TaskHandle {
            status: None, // upload directly managed by user - not internally managed
            group_progress: self.progress.clone(),
            task_id: tracking_id,
        };
        let tracking_name: Option<Arc<str>> = tracking_name.as_deref().map(Arc::from);
        let cleaner = upload_session.start_clean(tracking_name, file_size, sha256, tracking_id).await;

        Ok((task_handle, cleaner))
    }

    /// Enqueue a bytes upload task, spawning it on the runtime immediately.
    pub(super) async fn start_upload_bytes(
        &self,
        bytes: Vec<u8>,
        sha256: Sha256Policy,
        tracking_name: Option<String>,
    ) -> Result<UploadTaskHandle, XetError> {
        // Hold the state lock for the duration of this function so commit() will not run
        // when an upload task is registering.
        let state = self.state.lock().await;
        Self::check_accepting_tasks(&state)?;

        let tracking_id = Ulid::new();
        let status = Arc::new(Mutex::new(TaskStatus::Queued));
        let result: Arc<OnceLock<UploadResult>> = Arc::new(OnceLock::new());
        let task_handle = UploadTaskHandle {
            inner: TaskHandle {
                status: Some(status.clone()),
                group_progress: self.progress.clone(),
                task_id: tracking_id,
            },
            result: result.clone(),
        };

        let Some(upload_session) = self.upload_session.lock()?.clone() else {
            return Err(XetError::other("Upload session not initialized"));
        };

        let join_handle = self.spawn_upload_bytes_task(upload_session, bytes, status.clone(), tracking_id, sha256);

        let handle = InnerUploadTaskHandle {
            status,
            tracking_name,
            join_handle,
            result,
        };

        self.active_tasks.write()?.insert(tracking_id, handle);

        Ok(task_handle)
    }

    /// Join all active upload tasks and finalise the upload session.
    pub(super) async fn handle_commit(&self) -> Result<HashMap<Ulid, UploadResult>, XetError> {
        // Mark as not accepting new tasks. The tokio state lock serialises this
        // against all three registration methods, including start_upload_file
        // which holds it across the start_clean await.
        {
            let mut state_guard = self.state.lock().await;
            if *state_guard == GroupState::Finished {
                return Err(XetError::AlreadyCommitted);
            }
            *state_guard = GroupState::Aborted; // stop new tasks while draining
        }

        // Swap out the task map while holding the write lock (guard dropped before any `.await`),
        // then await each task and collect results; propagate the first join error after all tasks complete.
        let active_tasks = std::mem::take(&mut *self.active_tasks.write()?);

        let mut results = HashMap::new();
        let mut join_err = None;
        for (task_id, handle) in active_tasks {
            match handle.join_handle.await.map_err(XetError::from) {
                Ok(Ok(file_info)) => {
                    let result = Arc::new(Ok(FileMetadata {
                        tracking_name: handle.tracking_name,
                        hash: file_info.hash().to_string(),
                        file_size: file_info.file_size(),
                        sha256: file_info.sha256().map(str::to_owned),
                    }));
                    results.insert(task_id, result.clone());
                    // Update result to the external task handle, this is the only place setting
                    // the result, so no error will happen.
                    let _ = handle.result.set(result);
                },
                Ok(Err(task_err)) => {
                    let result = Arc::new(Err(task_err));
                    results.insert(task_id, result.clone());
                    // Update result to the external task handle, this is the only place setting
                    // the result, so no error will happen.
                    let _ = handle.result.set(result);
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
        *self.state.lock().await = GroupState::Finished;

        // Unregister from session
        self.session.finish_upload_commit(self.commit_id)?;

        Ok(results)
    }

    fn runtime(&self) -> &XetRuntime {
        &self.session.runtime
    }

    /// Cancel all tasks and set task status to "Cancelled".
    ///
    /// Called only from [`XetSession::abort`], which always calls
    /// `perform_sigint_shutdown()` first — so the runtime is already shutting
    /// down when this runs.
    ///
    /// The state flag update uses `try_lock` on the tokio state mutex and is
    /// best-effort: if a registration method currently holds the lock, the flag
    /// is left unchanged.  This is safe because `upload_session` is
    /// unconditionally cleared (set to `None`) and all active task handles are
    /// aborted, so the commit is effectively dead regardless of the state flag
    /// value.  A `blocking_lock()` is not used because `abort()` can be called
    /// from within a tokio async context (e.g. tests), where it would panic.
    ///
    /// Clearing `upload_session` prevents future `start_upload_file` calls from
    /// obtaining a session and prevents `handle_commit` from calling `finalize`.
    /// It does not invalidate any `SingleFileCleaner` already in the caller's hands,
    /// since the cleaner holds its own `Arc` to the session.
    fn abort(&self) -> Result<(), XetError> {
        if let Ok(mut guard) = self.state.try_lock() {
            *guard = GroupState::Aborted;
        }
        self.upload_session.lock()?.take();
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
    /// SHA-256 hash of the file content, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::time::Duration;

    use tempfile::{TempDir, tempdir};

    use super::*;
    use crate::xet_session::progress::TaskStatus;
    use crate::xet_session::session::{RuntimeMode, XetSession, XetSessionBuilder};

    async fn local_session(temp: &TempDir) -> Result<XetSession, Box<dyn std::error::Error>> {
        let cas_path = temp.path().join("cas");
        Ok(XetSessionBuilder::new()
            .with_endpoint(format!("local://{}", cas_path.display()))
            .build_async()
            .await?)
    }

    // ── Mutex guard / concurrency test ───────────────────────────────────────
    //
    // All three registration methods (upload_from_path, upload_bytes,
    // start_upload_file) hold `self.state` (a tokio::sync::Mutex) for
    // their entire execution so that commit() cannot race against an
    // in-progress registration.
    //
    // We verify this by locking the same mutex directly from the test thread
    // (valid because `mod tests` is a descendant of `upload_commit` and can
    // access private fields), simulating a method being mid-registration.

    #[test]
    // commit() must block while any enqueue method holds the state lock.
    fn test_commit_blocked_while_upload_registration_holds_state_lock() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let cas_path = temp.path().join("cas");
        let session = XetSessionBuilder::new()
            .with_endpoint(format!("local://{}", cas_path.display()))
            .build()?;
        let runtime = session.runtime.clone();
        // Create UploadCommit directly so we can access its private state field
        // (accessible here because mod tests is a submodule of upload_commit).
        let commit = runtime.external_run_async_task(UploadCommit::new(session.clone()))??;
        let commit_for_thread = commit.clone();
        let runtime_for_thread = runtime.clone();

        // Simulate an enqueue method holding the state lock mid-registration.
        let guard = commit.inner.state.blocking_lock();

        let (done_tx, done_rx) = mpsc::channel::<()>();
        let join_handle = std::thread::spawn(move || {
            let _ = runtime_for_thread.external_run_async_task(async move { commit_for_thread.commit().await });
            let _ = done_tx.send(());
        });

        std::thread::sleep(Duration::from_millis(50));
        assert!(done_rx.try_recv().is_err(), "commit() should be blocked while state lock is held");

        drop(guard);

        assert!(
            done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "commit() should complete after state lock is released"
        );
        let _ = join_handle.join();
        Ok(())
    }

    // ── Identity ─────────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // Two separate commits from the same session have distinct IDs.
    async fn test_commit_has_unique_id() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let c1 = session.new_upload_commit().await.unwrap();
        let c2 = session.new_upload_commit().await.unwrap();
        assert_ne!(c1.id(), c2.id());
    }

    #[tokio::test(flavor = "multi_thread")]
    // A clone refers to the same inner Arc, so their IDs must match.
    async fn test_commit_clone_shares_id() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let commit2 = commit.clone();
        assert_eq!(commit.id(), commit2.id());
    }

    // ── Initial state ────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // A fresh commit has all-zero aggregate progress.
    async fn test_get_progress_empty_initially() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let snapshot = commit.get_progress().unwrap();
        let total = snapshot.total();
        assert_eq!(total.total_bytes, 0);
        assert_eq!(total.total_bytes_completed, 0);
    }

    // ── Commit lifecycle ─────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // An empty commit succeeds and returns an empty result set.
    async fn test_commit_empty_succeeds() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let results = session.new_upload_commit().await.unwrap().commit().await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    // commit() transitions the commit into the Finished state.
    async fn test_commit_marks_as_committed() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let commit_clone = commit.clone();
        commit.commit().await.unwrap();
        assert!(commit_clone.is_committed().await);
    }

    #[tokio::test(flavor = "multi_thread")]
    // A second commit() call on any clone returns AlreadyCommitted.
    async fn test_second_commit_fails() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let c1 = session.new_upload_commit().await.unwrap();
        let c2 = c1.clone();
        c1.commit().await.unwrap();
        let err = c2.commit().await.unwrap_err();
        assert!(matches!(err, XetError::AlreadyCommitted | XetError::Internal(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    // commit() unregisters the commit from the session's active set.
    async fn test_commit_unregisters_from_session() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
        commit.commit().await.unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 0);
    }

    // ── Session-abort guards ─────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // upload_from_path returns Aborted when the parent session has been aborted.
    async fn test_upload_file_on_aborted_session_returns_error() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        session.abort().unwrap();
        let err = commit
            .upload_from_path(PathBuf::from("nonexistent.bin"), Sha256Policy::Compute)
            .await
            .unwrap_err();
        assert!(matches!(err, XetError::Aborted));
    }

    #[tokio::test(flavor = "multi_thread")]
    // upload_bytes returns Aborted when the parent session has been aborted.
    async fn test_upload_bytes_on_aborted_session_returns_error() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        session.abort().unwrap();
        let err = commit
            .upload_bytes(b"data".to_vec(), Sha256Policy::Compute, Some("bytes 1".into()))
            .await
            .unwrap_err();
        assert!(matches!(err, XetError::Aborted));
    }

    // ── Post-commit guards (AlreadyCommitted) ────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // upload_from_path after commit returns AlreadyCommitted.
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
    // upload_bytes after commit returns AlreadyCommitted.
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
    // upload_file returns a (TaskHandle, SingleFileCleaner) pair; the handle has no internal status.
    async fn test_upload_file_returns_handle_and_cleaner() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let (handle, _cleaner) = commit
            .upload_file(Some("stream.bin".into()), 1024, Sha256Policy::Compute)
            .await
            .unwrap();
        assert!(handle.status().is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    // abort() drains active_tasks and sets each task's status to Cancelled.
    async fn test_abort_marks_queued_task_as_cancelled() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit
            .upload_bytes(b"data".to_vec(), Sha256Policy::Compute, None)
            .await
            .unwrap();
        commit.inner.abort().unwrap();
        assert!(matches!(handle.status().unwrap(), TaskStatus::Cancelled));
    }

    #[tokio::test(flavor = "multi_thread")]
    // abort() uses try_lock on the state mutex, so it does not deadlock or return an
    // error when a concurrent registration holds the lock.  In that case the state flag
    // is silently left as Alive — the runtime shutdown cancels the in-flight task.
    // abort() still drains active_tasks and marks already-queued tasks Cancelled.
    async fn test_abort_while_state_lock_held_skips_state_update_but_drains_tasks() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();

        // Queue a task so we can verify active_tasks draining still happens.
        let handle = commit
            .upload_bytes(b"data".to_vec(), Sha256Policy::Compute, None)
            .await
            .unwrap();

        // Hold the state lock — simulates a registration method mid-execution.
        let guard = commit.inner.state.try_lock().expect("lock should be free before abort");

        // abort() must succeed (no deadlock, no error) even though try_lock will fail.
        commit.inner.abort().unwrap();

        // State was NOT updated — abort() skipped the state flag when lock was held.
        assert!(matches!(*guard, GroupState::Alive), "state must remain Alive when lock was contended");
        drop(guard);

        // active_tasks are always drained; already-queued tasks are Cancelled.
        assert!(matches!(handle.status().unwrap(), TaskStatus::Cancelled));

        // upload_session is always cleared, preventing future start_upload_file calls
        // from obtaining a session and preventing handle_commit from calling finalize.
        assert!(commit.inner.upload_session.lock().unwrap().is_none());
    }

    // ── Independence ─────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // Committing one commit does not affect the state of another from the same session.
    async fn test_two_commits_are_independent() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let c1 = session.new_upload_commit().await.unwrap();
        let c2 = session.new_upload_commit().await.unwrap();
        c1.commit().await.unwrap();
        assert!(!c2.is_committed().await);
    }

    // ── Round-trip tests ─────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // Uploading raw bytes and committing returns a non-empty hash and the correct file size.
    async fn test_upload_bytes_round_trip() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let data = b"Hello, upload commit round-trip!";
        let commit = session.new_upload_commit().await.unwrap();
        let task_handle = commit
            .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("hello.bin".into()))
            .await
            .unwrap();
        let results = commit.commit().await.unwrap();
        assert_eq!(results.len(), 1);
        let meta = results.get(&task_handle.task_id).unwrap().as_ref().as_ref().unwrap();
        assert_eq!(meta.file_size, data.len() as u64);
        assert!(!meta.hash.is_empty());
        assert!(meta.sha256.is_some());
        assert_eq!(meta.sha256.as_deref().unwrap().len(), 64);
    }

    #[tokio::test(flavor = "multi_thread")]
    // Uploading a file from disk and committing returns the correct file size.
    async fn test_upload_from_path_round_trip() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let src = temp.path().join("data.bin");
        let data = b"file path upload content";
        std::fs::write(&src, data).unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit.upload_from_path(src, Sha256Policy::Compute).await.unwrap();
        commit.commit().await.unwrap();
        let meta = handle.result().unwrap();
        let meta = meta.as_ref().as_ref().unwrap();
        assert_eq!(meta.file_size, data.len() as u64);
        assert!(!meta.hash.is_empty());
        assert!(meta.sha256.is_some());
        assert_eq!(meta.sha256.as_deref().unwrap().len(), 64);
    }

    #[tokio::test(flavor = "multi_thread")]
    // SHA-256 metadata follows policy: Compute/Provided populate it, Skip omits it.
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

        let results = commit.commit().await.unwrap();

        let compute_meta = results.get(&compute_handle.task_id).unwrap().as_ref().as_ref().unwrap();
        assert!(compute_meta.sha256.is_some());
        assert_eq!(compute_meta.sha256.as_deref().unwrap().len(), 64);

        let provided_meta = results.get(&provided_handle.task_id).unwrap().as_ref().as_ref().unwrap();
        assert_eq!(provided_meta.sha256.as_deref(), Some(provided_sha256.as_str()));

        let skip_meta = results.get(&skip_handle.task_id).unwrap().as_ref().as_ref().unwrap();
        assert!(skip_meta.sha256.is_none());
    }

    // ── Per-task result access patterns ──────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // UploadTaskHandle::result() returns None before commit() is called.
    async fn test_upload_result_none_before_commit() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let src = temp.path().join("data.bin");
        std::fs::write(&src, b"content").unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit.upload_from_path(src, Sha256Policy::Compute).await.unwrap();
        assert!(handle.result().is_none(), "result must be None before commit()");
        commit.commit().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    // Pattern 1: per-task result is accessible via task_id in the commit() HashMap.
    async fn test_upload_result_accessible_via_task_id_in_commit_map() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let data = b"result via task_id";
        let src = temp.path().join("data.bin");
        std::fs::write(&src, data).unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit.upload_from_path(src, Sha256Policy::Compute).await.unwrap();
        let results = commit.commit().await.unwrap();
        let result = results.get(&handle.task_id).expect("task_id must be present in results");
        assert_eq!(result.as_ref().as_ref().unwrap().file_size, data.len() as u64);
    }

    #[tokio::test(flavor = "multi_thread")]
    // Pattern 2: per-task result is accessible directly from the UploadTaskHandle after commit().
    async fn test_upload_result_accessible_via_handle_after_commit() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let data = b"result via handle";
        let src = temp.path().join("data.bin");
        std::fs::write(&src, data).unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit.upload_from_path(src, Sha256Policy::Compute).await.unwrap();
        commit.commit().await.unwrap();
        let result = handle.result().expect("result must be set after commit");
        assert_eq!(result.as_ref().as_ref().unwrap().file_size, data.len() as u64);
    }

    #[tokio::test(flavor = "multi_thread")]
    // Streaming upload via upload_file + SingleFileCleaner.
    async fn test_upload_streaming_round_trip() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let data = b"streamed upload bytes";
        let commit = session.new_upload_commit().await.unwrap();
        let (_handle, mut cleaner) = commit
            .upload_file(Some("stream.bin".into()), data.len() as u64, Sha256Policy::Compute)
            .await
            .unwrap();
        cleaner.add_data(data).await.unwrap();
        let (xfi, _) = cleaner.finish().await.unwrap();
        let results = commit.commit().await.unwrap();
        assert!(results.is_empty());
        assert_eq!(xfi.file_size, data.len() as u64);
        assert!(!xfi.hash.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    // Uploading multiple blobs in one commit returns one result per upload.
    async fn test_upload_multiple_files_in_one_commit() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let commit = session.new_upload_commit().await.unwrap();
        commit
            .upload_bytes(b"file one".to_vec(), Sha256Policy::Compute, Some("a.bin".into()))
            .await
            .unwrap();
        commit
            .upload_bytes(b"file two".to_vec(), Sha256Policy::Compute, Some("b.bin".into()))
            .await
            .unwrap();
        commit
            .upload_bytes(b"file three".to_vec(), Sha256Policy::Compute, Some("c.bin".into()))
            .await
            .unwrap();
        let results = commit.commit().await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    // After a successful commit the aggregate progress reflects bytes processed.
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
        let snapshot = progress_observer.get_progress().unwrap();
        assert!(snapshot.total().total_bytes_completed > 0);
    }

    // ── Non-tokio executor (Owned-mode bridge) ────────────────────────────────

    #[test]
    // build_async() falls back to Owned mode from a futures executor, and the
    // bridge correctly routes all async method calls to the hf-xet-* thread pool
    // while the futures are driven by the caller's executor (futures::block_on).
    fn test_async_bridge_works_from_futures_executor() {
        let temp = tempdir().unwrap();

        futures::executor::block_on(async {
            // Outside tokio → try_current() fails → build_async() picks Owned mode.
            let session = local_session(&temp).await.unwrap();
            assert_eq!(session.runtime_mode, RuntimeMode::Owned);

            // Exercise every bridge: new_upload_commit, upload_bytes, commit.
            let data = b"hello from non-tokio executor";
            let commit = session.new_upload_commit().await.unwrap();
            let handle = commit
                .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
                .await
                .unwrap();
            let results = commit.commit().await.unwrap();

            let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
            assert_eq!(meta.file_size, data.len() as u64);
        });
    }

    #[test]
    // Same as above but driven by the smol executor.
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
            let results = commit.commit().await.unwrap();

            let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
            assert_eq!(meta.file_size, data.len() as u64);
        });
    }

    #[test]
    // Same as above but driven by the async-std executor.
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
            let results = commit.commit().await.unwrap();

            let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
            assert_eq!(meta.file_size, data.len() as u64);
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
        let results = commit.commit_blocking()?;
        assert_eq!(results.len(), 1);
        let meta = results.get(&task_handle.task_id).unwrap().as_ref().as_ref().unwrap();
        assert_eq!(meta.file_size, data.len() as u64);
        assert!(!meta.hash.is_empty());
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
        let meta = handle.result().unwrap();
        let meta = meta.as_ref().as_ref().unwrap();
        assert_eq!(meta.file_size, data.len() as u64);
        assert!(!meta.hash.is_empty());
        Ok(())
    }

    #[test]
    fn test_blocking_upload_result_access_patterns() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session_sync(&temp)?;
        let data = b"result access patterns";
        let src = temp.path().join("data.bin");
        std::fs::write(&src, data)?;
        let commit = session.new_upload_commit_blocking()?;
        let handle = commit.upload_from_path_blocking(src, Sha256Policy::Compute)?;

        // Before commit, per-task result is not available yet.
        assert!(handle.result().is_none());

        let results = commit.commit_blocking()?;

        // Result should be available in the commit map by task id.
        let map_result = results.get(&handle.task_id).expect("task_id must be present in results");
        assert_eq!(map_result.as_ref().as_ref().unwrap().file_size, data.len() as u64);

        // Result should also be available via the task handle.
        let handle_result = handle.result().expect("result must be set after commit");
        assert_eq!(handle_result.as_ref().as_ref().unwrap().file_size, data.len() as u64);
        Ok(())
    }

    #[test]
    fn test_blocking_upload_streaming_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session_sync(&temp)?;
        let data = b"streamed upload bytes";
        let runtime = session.runtime.clone();
        let commit = session.new_upload_commit_blocking()?;
        let (_handle, mut cleaner) =
            commit.upload_file_blocking(Some("stream.bin".into()), data.len() as u64, Sha256Policy::Compute)?;
        let (hash, file_size) = runtime.external_run_async_task(async move {
            cleaner.add_data(data).await.unwrap();
            let (xfi, _) = cleaner.finish().await.unwrap();
            (xfi.hash, xfi.file_size)
        })?;
        let results = commit.commit_blocking()?;
        assert!(results.is_empty());
        assert_eq!(file_size, data.len() as u64);
        assert!(!hash.is_empty());
        Ok(())
    }

    #[test]
    fn test_blocking_upload_multiple_files_in_one_commit() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session_sync(&temp)?;
        let commit = session.new_upload_commit_blocking()?;
        commit.upload_bytes_blocking(b"file one".to_vec(), Sha256Policy::Compute, Some("a.bin".into()))?;
        commit.upload_bytes_blocking(b"file two".to_vec(), Sha256Policy::Compute, Some("b.bin".into()))?;
        commit.upload_bytes_blocking(b"file three".to_vec(), Sha256Policy::Compute, Some("c.bin".into()))?;
        let results = commit.commit_blocking()?;
        assert_eq!(results.len(), 3);
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
        let snapshot = progress_observer.get_progress()?;
        assert!(snapshot.total().total_bytes_completed > 0);
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
            let results = commit.commit_blocking().unwrap();
            let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
            assert_eq!(meta.file_size, data.len() as u64);
            assert!(!meta.hash.is_empty());
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
}
