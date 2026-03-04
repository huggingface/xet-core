//! DownloadGroup - groups related downloads

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use data::{FileDownloadSession, XetFileInfo};
use tokio::task::JoinHandle;
use ulid::Ulid;
use xet_runtime::XetRuntime;

use crate::common::{GroupState, create_translator_config};
use crate::errors::SessionError;
use crate::progress::{GroupProgress, ProgressSnapshot, TaskHandle, TaskStatus};
use crate::session::XetSession;

/// Groups related file downloads into a single unit of work.
///
/// Queue files with [`download_file_to_path`](Self::download_file_to_path) (they start
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

        let progress = Arc::new(GroupProgress::new());
        let progress_clone = progress.clone();
        let config = create_translator_config(&session)?;
        let download_session = session.runtime.external_run_async_task(async move {
            let progress_updater = progress_clone as Arc<dyn progress_tracking::TrackingProgressUpdater>;
            FileDownloadSession::new(Arc::new(config), Some(progress_updater)).await
        })??;

        let inner = Arc::new(DownloadGroupInner {
            group_id,
            session,
            active_tasks: RwLock::new(HashMap::new()),
            progress,
            download_session: Mutex::new(Some(download_session)),
            state: Mutex::new(GroupState::Alive),
        });

        Ok(Self { inner })
    }

    /// Get the group ID.
    pub(crate) fn id(&self) -> Ulid {
        self.group_id
    }

    /// Abort this download group.
    pub(crate) fn abort(&self) -> Result<(), SessionError> {
        self.inner.abort()
    }

    // ===== Public synchronous methods =====

    /// Queue a file for download to `dest_path`, starting the transfer immediately if system resource permits.
    ///
    /// # Parameters
    ///
    /// * `file_info` – Content-addressed hash and size returned by a previous
    ///   [`UploadCommit::commit`](crate::UploadCommit::commit).
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
    pub fn download_file_to_path(
        &self,
        file_info: XetFileInfo,
        dest_path: PathBuf,
    ) -> Result<TaskHandle, SessionError> {
        self.session.check_alive()?;
        self.inner.start_download_file_to_path(file_info, dest_path)
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
    pub fn get_progress(&self) -> Result<ProgressSnapshot, SessionError> {
        self.progress.snapshot()
    }

    /// Wait for all downloads to complete and return their results.
    ///
    /// Blocks until every queued download finishes (or fails).  Returns one
    /// [`DownloadResult`] entry per download.
    ///
    /// Consumes `self` — subsequent calls on any clone will return
    /// [`SessionError::AlreadyFinished`] (or a channel-closed error if the
    /// background worker has already exited).
    pub fn finish(self) -> Result<Vec<Result<DownloadResult, SessionError>>, SessionError> {
        let inner = self.inner.clone();
        self.session
            .runtime
            .external_run_async_task(async move { inner.handle_finish().await })?
    }
}

/// Handle for a single download task tracked internally by DownloadGroup.
pub(crate) struct InnerDownloadTaskHandle {
    status: Arc<Mutex<TaskStatus>>,
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
    download_session: Mutex<Option<Arc<FileDownloadSession>>>,

    // State
    state: Mutex<GroupState>,
}

impl DownloadGroupInner {
    // ===== State helpers =====

    /// Check whether the group is still accepting new tasks.
    fn check_accepting_tasks(state: &MutexGuard<GroupState>) -> Result<(), SessionError> {
        match **state {
            GroupState::Finished => Err(SessionError::AlreadyFinished),
            GroupState::Aborted => Err(SessionError::Aborted),
            GroupState::Alive => Ok(()),
        }
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
        let semaphore = self.runtime().common().file_download_semaphore.clone();
        self.runtime().spawn(async move {
            // Update status from "Queued" to "Running" once a semaphore permit is acquired.
            let _permit = semaphore.acquire().await?;

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

    fn start_download_file_to_path(
        self: &Arc<Self>,
        file_info: XetFileInfo,
        dest_path: PathBuf,
    ) -> Result<TaskHandle, SessionError> {
        // Hold the state lock guard for the duration of this function so finish() will not run
        // when a download task is registering.
        let state = self.state.lock()?;
        Self::check_accepting_tasks(&state)?;

        let tracking_id = Ulid::new();
        let status = Arc::new(Mutex::new(TaskStatus::Queued));

        let task_handle = TaskHandle {
            status: Some(status.clone()),
            group_progress: self.progress.clone(),
            tracking_id,
        };

        let Some(download_session) = self.download_session.lock()?.clone() else {
            return Err(SessionError::other("Download session not initialized"));
        };

        let join_handle = self.spawn_download_task(
            download_session,
            file_info.clone(),
            dest_path.clone(),
            status.clone(),
            tracking_id,
        );

        let handle = InnerDownloadTaskHandle {
            status,
            dest_path,
            join_handle,
        };

        self.active_tasks.write()?.insert(tracking_id, handle);

        Ok(task_handle)
    }

    /// Handle a `Finish` command from the public API.
    async fn handle_finish(self: &Arc<Self>) -> Result<Vec<Result<DownloadResult, SessionError>>, SessionError> {
        // Mark as not accepting new tasks
        {
            let mut state_guard = self.state.lock()?;
            if *state_guard == GroupState::Finished {
                return Err(SessionError::AlreadyFinished);
            }
            *state_guard = GroupState::Aborted; // stop new tasks while draining
        }

        // Wait for all downloads to complete
        let active_tasks = std::mem::take(&mut *self.active_tasks.write()?);

        let mut results = Vec::new();
        let mut join_err = None;
        // Join all tasks first and then propogate errors.
        for (_task_id, handle) in active_tasks {
            match handle.join_handle.await.map_err(SessionError::TaskJoinError) {
                Ok(Ok(file_info)) => {
                    results.push(Ok(DownloadResult {
                        dest_path: handle.dest_path,
                        file_info,
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
            }
        }
        if let Some(e) = join_err {
            return Err(e);
        }

        // Mark as finished
        *self.state.lock()? = GroupState::Finished;

        // Unregister from session
        self.session.finish_download_group(self.group_id)?;

        Ok(results)
    }

    fn runtime(&self) -> &XetRuntime {
        &self.session.runtime
    }

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
    use std::time::Duration;

    use tempfile::{TempDir, tempdir};

    use super::*;
    use crate::session::XetSession;

    fn local_session(temp: &TempDir) -> Result<XetSession, Box<dyn std::error::Error>> {
        let cas_path = temp.path().join("cas");
        Ok(XetSession::new(Some(format!("local://{}", cas_path.display())), None, None, None)?)
    }

    fn upload_bytes(session: &XetSession, data: &[u8], name: &str) -> Result<XetFileInfo, Box<dyn std::error::Error>> {
        let commit = session.new_upload_commit()?;
        commit.upload_bytes(data.to_vec(), Some(name.into()))?;
        let results = commit.commit()?;
        let m = &results[0];
        Ok(XetFileInfo {
            hash: m.as_ref().unwrap().hash.clone(),
            file_size: m.as_ref().unwrap().file_size,
        })
    }

    // ── Identity ─────────────────────────────────────────────────────────────

    #[test]
    // Two download groups created from the same session have distinct IDs.
    fn test_group_has_unique_id() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let g1 = session.new_download_group()?;
        let g2 = session.new_download_group()?;
        assert_ne!(g1.id(), g2.id());
        Ok(())
    }

    // ── Initial state ────────────────────────────────────────────────────────

    #[test]
    // A fresh group has all-zero aggregate progress.
    fn test_get_progress_empty_initially() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let group = session.new_download_group()?;
        let snapshot = group.get_progress()?;
        let total = snapshot.total();
        assert_eq!(total.total_bytes, 0);
        assert_eq!(total.total_bytes_completed, 0);
        Ok(())
    }

    // ── Finish lifecycle ─────────────────────────────────────────────────────

    #[test]
    // An empty finish succeeds and returns an empty result set.
    fn test_finish_empty_succeeds() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let group = session.new_download_group()?;
        let results = group.finish()?;
        assert!(results.is_empty());
        Ok(())
    }

    #[test]
    // finish() transitions the group into the Finished state.
    fn test_finish_marks_as_finished() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let group = session.new_download_group()?;
        let group_clone = group.clone();
        group.finish().unwrap();
        assert!(group_clone.is_finished());
        Ok(())
    }

    #[test]
    // A second finish() call on any clone returns AlreadyFinished.
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
    // finish() unregisters the group from the session's active set.
    fn test_finish_unregisters_from_session() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let group = session.new_download_group()?;
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
        group.finish().unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 0);
        Ok(())
    }

    // ── Guards ───────────────────────────────────────────────────────────────

    #[test]
    // download_file_to_path returns Aborted when the parent session has been aborted.
    fn test_download_file_on_aborted_session_returns_error() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let group = session.new_download_group()?;
        session.abort().unwrap();
        let err = group
            .download_file_to_path(
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
    // download_file_to_path after finish returns AlreadyFinished.
    fn test_download_file_after_finish_fails() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let g1 = session.new_download_group()?;
        let g2 = g1.clone();
        g1.finish()?;
        let err = g2
            .download_file_to_path(
                XetFileInfo {
                    hash: "abc123".to_string(),
                    file_size: 1024,
                },
                PathBuf::from("dest.bin"),
            )
            .unwrap_err();
        assert!(matches!(err, SessionError::AlreadyFinished));
        Ok(())
    }

    #[test]
    // download_file_to_path on a directly-aborted group returns Aborted (not AlreadyFinished).
    fn test_download_file_on_aborted_group_returns_aborted() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let group = session.new_download_group()?;
        group.abort()?;
        let err = group
            .download_file_to_path(
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

    // ── Independence ─────────────────────────────────────────────────────────

    #[test]
    // Finishing one group does not affect the state of another from the same session.
    fn test_two_groups_are_independent() -> Result<(), Box<dyn std::error::Error>> {
        let session = XetSession::new(None, None, None, None)?;
        let g1 = session.new_download_group()?;
        let g2 = session.new_download_group()?;
        g1.finish()?;
        assert!(!g2.is_finished());
        Ok(())
    }

    // ── Round-trip tests ─────────────────────────────────────────────────────

    #[test]
    // Downloading a previously uploaded file produces byte-identical content at the destination.
    fn test_download_file_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let original = b"Hello, download round-trip!";
        let file_info = upload_bytes(&session, original, "payload.bin")?;

        let dest = temp.path().join("downloaded.bin");
        let group = session.new_download_group()?;
        group.download_file_to_path(file_info, dest.clone())?;
        group.finish()?;

        assert_eq!(std::fs::read(&dest)?, original);
        Ok(())
    }

    #[test]
    // Downloading multiple files from a single group produces correct content for each.
    fn test_download_multiple_files() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;

        let data_a = b"First file content";
        let data_b = b"Second file content - different";

        // Upload both files in one commit; use tracking_name to locate each result.
        let commit = session.new_upload_commit()?;
        commit.upload_bytes(data_a.to_vec(), Some("a.bin".into()))?;
        commit.upload_bytes(data_b.to_vec(), Some("b.bin".into()))?;
        let results = commit.commit()?;

        let find_info = |name: &str| -> XetFileInfo {
            let m = results
                .iter()
                .find(|r| r.as_ref().unwrap().tracking_name.as_deref() == Some(name))
                .unwrap();
            XetFileInfo {
                hash: m.as_ref().unwrap().hash.clone(),
                file_size: m.as_ref().unwrap().file_size,
            }
        };

        let dest_a = temp.path().join("a_out.bin");
        let dest_b = temp.path().join("b_out.bin");
        let group = session.new_download_group()?;
        group.download_file_to_path(find_info("a.bin"), dest_a.clone())?;
        group.download_file_to_path(find_info("b.bin"), dest_b.clone())?;
        group.finish()?;

        assert_eq!(std::fs::read(&dest_a)?, data_a);
        assert_eq!(std::fs::read(&dest_b)?, data_b);
        Ok(())
    }

    #[test]
    // After a successful finish the aggregate download progress reflects bytes received.
    fn test_download_progress_reflects_bytes_after_finish() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let original = b"download progress tracking data";
        let file_info = upload_bytes(&session, original, "prog.bin")?;

        let dest = temp.path().join("out.bin");
        let group = session.new_download_group()?;
        let progress_observer = group.clone();
        group.download_file_to_path(file_info, dest)?;
        group.finish()?;

        std::thread::sleep(
            session
                .runtime
                .config()
                .data
                .progress_update_interval
                .saturating_add(Duration::from_secs(1)),
        );
        let snapshot = progress_observer.get_progress()?;
        assert!(snapshot.total().total_bytes_completed > 0);
        Ok(())
    }

    // ── Mutex guard / concurrency test ───────────────────────────────────────
    //
    // `download_file_to_path` holds `self.state` for its entire execution so
    // that `finish()` cannot race against an in-progress registration.  We
    // verify this by locking the same mutex directly from the test thread
    // (valid because `mod tests` is a descendant of `download_group` and can
    // access private fields), simulating the method being mid-registration.

    #[test]
    // finish() must block while download_file_to_path() holds the state lock.
    fn test_finish_blocked_while_download_registration_holds_state_lock() -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::mpsc;

        let session = XetSession::new(None, None, None, None)?;
        let group = session.new_download_group()?;
        let group_for_thread = group.clone();

        // Simulate download_file_to_path() holding the state lock mid-registration.
        let guard = group.inner.state.lock().unwrap();

        let (done_tx, done_rx) = mpsc::channel::<()>();
        let join_handle = std::thread::spawn(move || {
            let _ = group_for_thread.finish(); // must block until guard is dropped
            let _ = done_tx.send(());
        });

        // Give the spawned thread enough time to reach the state-lock acquisition
        // inside finish() and block there.
        std::thread::sleep(Duration::from_millis(50));
        assert!(done_rx.try_recv().is_err(), "finish() should be blocked while state lock is held");

        // Release the lock — simulates the enqueue method completing its registration.
        drop(guard);

        assert!(
            done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "finish() should complete after state lock is released"
        );
        let _ = join_handle.join();
        Ok(())
    }
}
