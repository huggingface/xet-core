//! DownloadGroup - groups related downloads

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};

use tokio::task::JoinHandle;
use ulid::Ulid;
use xet_data::processing::{FileDownloadSession, XetFileInfo};
use xet_runtime::core::XetRuntime;

use super::common::{GroupState, create_translator_config};
use super::errors::SessionError;
use super::progress::{DownloadTaskHandle, GroupProgress, ProgressSnapshot, TaskHandle, TaskStatus};
use super::session::{RuntimeMode, XetSession};

/// Async API for grouping related file downloads into a single unit of work.
///
/// Obtain via [`XetSession::new_download_group`] from an `async` context.
/// For sync / non-async code use [`DownloadGroupSync`] from
/// [`XetSession::new_download_group_blocking`] instead.
///
/// Queue files with [`download_file_to_path`](Self::download_file_to_path) (they start
/// downloading immediately in the background), poll progress with
/// [`get_progress`](Self::get_progress), then `await`
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
///
/// [`DownloadGroupSync`]: crate::xet_session::sync::DownloadGroupSync
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
    /// Create a new download group from an **async** context. Initialisation logic shared by the sync and async
    /// constructors.
    pub(super) async fn new(session: XetSession) -> Result<Self, SessionError> {
        let group_id = Ulid::new();
        let progress = Arc::new(GroupProgress::new());
        let config = create_translator_config(&session)?;
        let progress_updater = progress.clone() as Arc<dyn xet_data::progress_tracking::TrackingProgressUpdater>;
        let download_session = FileDownloadSession::new(Arc::new(config), Some(progress_updater)).await?;

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
    pub(super) fn id(&self) -> Ulid {
        self.group_id
    }

    /// Abort this download group.
    pub(super) fn abort(&self) -> Result<(), SessionError> {
        self.inner.abort()
    }

    /// Returns the runtime used by this group.
    pub(super) fn runtime(&self) -> &XetRuntime {
        &self.inner.session.runtime
    }

    /// Queue a file for download to `dest_path`, starting the transfer immediately if system resource permits.
    ///
    /// # Parameters
    ///
    /// * `file_info` – Content-addressed hash and size returned by a previous
    ///   [`UploadCommit::commit`](crate::xet_session::UploadCommit::commit).
    /// * `dest_path` – Local path where the downloaded file will be written. Parent directories are created
    ///   automatically.
    ///
    /// Returns a [`DownloadTaskHandle`] that can be used to poll status and per-file
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
    ) -> Result<DownloadTaskHandle, SessionError> {
        self.session.check_alive()?;

        // Use the absolute path in case the process current working directory changes
        // while the task is queued.
        let absolute_path = std::path::absolute(dest_path)?;
        self.inner.start_download_file_to_path(file_info, absolute_path)
    }

    /// Return a snapshot of progress for every queued download.
    pub fn get_progress(&self) -> Result<ProgressSnapshot, SessionError> {
        self.progress.snapshot()
    }

    /// Wait for all downloads to complete and return their results.
    ///
    /// Returns a `HashMap` keyed by task ID where each value is
    /// [`DownloadResult`] (= `Arc<Result<`[`DownloadedFile`]`,
    /// [`SessionError`](crate::SessionError)`>>`). A single failed download
    /// does not prevent the others from being collected.
    ///
    /// Per-task results can also be read directly from the
    /// [`DownloadTaskHandle`] returned by `download_file_to_path` via
    /// [`result`](DownloadTaskHandle::result) after this method returns.
    ///
    /// Consumes `self` — subsequent calls on any clone will return
    /// [`SessionError::AlreadyFinished`].
    pub async fn finish(self) -> Result<HashMap<Ulid, DownloadResult>, SessionError> {
        if matches!(self.session.runtime_mode, RuntimeMode::External) {
            return self.inner.handle_finish().await;
        }
        let inner = self.inner.clone();
        self.session
            .runtime
            .bridge_to_owned("finish", async move { inner.handle_finish().await })
            .await?
    }

    /// Returns `true` if [`finish`](Self::finish) has been called and completed.
    #[cfg(test)]
    fn is_finished(&self) -> bool {
        match self.state.lock() {
            Ok(state) => *state == GroupState::Finished,
            Err(_) => false,
        }
    }
}

/// Per-file result type returned by [`DownloadGroup::finish`].
///
/// The `Arc` lets the same value be stored in both the `finish()` return map
/// and the per-task [`DownloadTaskHandle`] without requiring the inner
/// `Result` to be `Clone`.
pub type DownloadResult = Arc<Result<DownloadedFile, SessionError>>;

/// Handle for a single download task tracked internally by DownloadGroup.
struct InnerDownloadTaskHandle {
    status: Arc<Mutex<TaskStatus>>,
    dest_path: PathBuf,
    join_handle: JoinHandle<Result<XetFileInfo, SessionError>>,
    result: Arc<OnceLock<DownloadResult>>,
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
            // Only overwrite if still Running — abort() may have set Cancelled concurrently.
            let mut s = status.lock()?;
            if matches!(*s, TaskStatus::Running) {
                *s = new_status;
            }

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
    ) -> Result<DownloadTaskHandle, SessionError> {
        // Hold the state lock guard for the duration of this function so finish() will not run
        // when a download task is registering.
        let state = self.state.lock()?;
        Self::check_accepting_tasks(&state)?;

        let tracking_id = Ulid::new();
        let status = Arc::new(Mutex::new(TaskStatus::Queued));

        let result: Arc<OnceLock<DownloadResult>> = Arc::new(OnceLock::new());
        let task_handle = DownloadTaskHandle {
            inner: TaskHandle {
                status: Some(status.clone()),
                group_progress: self.progress.clone(),
                task_id: tracking_id,
            },
            result: result.clone(),
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
            result,
        };

        self.active_tasks.write()?.insert(tracking_id, handle);

        Ok(task_handle)
    }

    /// Join all active download tasks and mark the group as finished.
    async fn handle_finish(&self) -> Result<HashMap<Ulid, DownloadResult>, SessionError> {
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

        let mut results = HashMap::new();
        let mut join_err = None;
        // Join all tasks first and then propogate errors.
        for (task_id, handle) in active_tasks {
            match handle.join_handle.await.map_err(SessionError::from) {
                Ok(Ok(file_info)) => {
                    let result = Arc::new(Ok(DownloadedFile {
                        dest_path: handle.dest_path,
                        file_info,
                    }));
                    results.insert(task_id, result.clone());
                    // Update result to the external task handle, this is the only place setting
                    // the result, so no error will happen.
                    let _ = handle.result.set(result);
                },
                Ok(Err(task_err)) => {
                    let result: Arc<Result<DownloadedFile, SessionError>> = Arc::new(Err(task_err));
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

/// Per-file result returned by [`DownloadGroup::finish`].
#[derive(Clone, Debug)]
pub struct DownloadedFile {
    /// Local path where the file was written.
    pub dest_path: PathBuf,
    /// Xet file hash and size of the downloaded file.
    pub file_info: XetFileInfo,
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use tempfile::{TempDir, tempdir};

    use super::*;
    use crate::xet_session::session::{XetSession, XetSessionBuilder};

    async fn local_session(temp: &TempDir) -> Result<XetSession, Box<dyn std::error::Error>> {
        let cas_path = temp.path().join("cas");
        Ok(XetSessionBuilder::new()
            .with_endpoint(format!("local://{}", cas_path.display()))
            .build_async()
            .await?)
    }

    async fn upload_bytes(
        session: &XetSession,
        data: &[u8],
        name: &str,
    ) -> Result<XetFileInfo, Box<dyn std::error::Error>> {
        let commit = session.new_upload_commit().await?;
        let handle = commit.upload_bytes(data.to_vec(), Some(name.into())).await?;
        let results = commit.commit().await?;
        let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
        Ok(XetFileInfo {
            hash: meta.hash.clone(),
            file_size: meta.file_size,
        })
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
        let session = XetSessionBuilder::new().build()?;
        let runtime = session.runtime.clone();
        // Create DownloadGroup directly so we can access its private state field
        // (accessible here because mod tests is a submodule of download_group).
        let group = runtime.external_run_async_task(DownloadGroup::new(session.clone()))??;
        let group_for_thread = group.clone();
        let runtime_for_thread = runtime.clone();

        // Simulate download_file_to_path() holding the state lock mid-registration.
        let guard = group.inner.state.lock().unwrap();

        let (done_tx, done_rx) = mpsc::channel::<()>();
        let join_handle = std::thread::spawn(move || {
            let _ = runtime_for_thread.external_run_async_task(async move { group_for_thread.finish().await });
            let _ = done_tx.send(());
        });

        std::thread::sleep(Duration::from_millis(50));
        assert!(done_rx.try_recv().is_err(), "finish() should be blocked while state lock is held");

        drop(guard);

        assert!(
            done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "finish() should complete after state lock is released"
        );
        let _ = join_handle.join();
        Ok(())
    }

    // ── Identity ─────────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // Two download groups created from the same session have distinct IDs.
    async fn test_group_has_unique_id() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let g1 = session.new_download_group().await.unwrap();
        let g2 = session.new_download_group().await.unwrap();
        assert_ne!(g1.id(), g2.id());
    }

    // ── Initial state ────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // A fresh group has all-zero aggregate progress.
    async fn test_get_progress_empty_initially() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let group = session.new_download_group().await.unwrap();
        let snapshot = group.get_progress().unwrap();
        let total = snapshot.total();
        assert_eq!(total.total_bytes, 0);
        assert_eq!(total.total_bytes_completed, 0);
    }

    // ── Finish lifecycle ─────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // An empty finish succeeds and returns an empty result set.
    async fn test_finish_empty_succeeds() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let group = session.new_download_group().await.unwrap();
        let results = group.finish().await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    // finish() transitions the group into the Finished state.
    async fn test_finish_marks_as_finished() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let group = session.new_download_group().await.unwrap();
        let group_clone = group.clone();
        group.finish().await.unwrap();
        assert!(group_clone.is_finished());
    }

    #[tokio::test(flavor = "multi_thread")]
    // A second finish() call on any clone returns AlreadyFinished.
    async fn test_second_finish_fails() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let g1 = session.new_download_group().await.unwrap();
        let g2 = g1.clone();
        g1.finish().await.unwrap();
        let err = g2.finish().await.unwrap_err();
        assert!(matches!(err, SessionError::AlreadyFinished | SessionError::Internal(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    // finish() unregisters the group from the session's active set.
    async fn test_finish_unregisters_from_session() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let group = session.new_download_group().await.unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
        group.finish().await.unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 0);
    }

    // ── Guards ───────────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // download_file_to_path returns Aborted when the parent session has been aborted.
    async fn test_download_file_on_aborted_session_returns_error() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let group = session.new_download_group().await.unwrap();
        session.abort().unwrap();
        let err = group
            .download_file_to_path(
                XetFileInfo {
                    hash: "abc123".to_string(),
                    file_size: 1024,
                },
                std::path::PathBuf::from("dest.bin"),
            )
            .unwrap_err();
        assert!(matches!(err, SessionError::Aborted));
    }

    #[tokio::test(flavor = "multi_thread")]
    // download_file_to_path after finish returns AlreadyFinished.
    async fn test_download_file_after_finish_fails() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let g1 = session.new_download_group().await.unwrap();
        let g2 = g1.clone();
        g1.finish().await.unwrap();
        let err = g2
            .download_file_to_path(
                XetFileInfo {
                    hash: "abc123".to_string(),
                    file_size: 1024,
                },
                std::path::PathBuf::from("dest.bin"),
            )
            .unwrap_err();
        assert!(matches!(err, SessionError::AlreadyFinished));
    }

    #[tokio::test(flavor = "multi_thread")]
    // download_file_to_path on a directly-aborted group returns Aborted.
    async fn test_download_file_on_aborted_group_returns_aborted() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let group = session.new_download_group().await.unwrap();
        group.abort().unwrap();
        let err = group
            .download_file_to_path(
                XetFileInfo {
                    hash: "abc123".to_string(),
                    file_size: 1024,
                },
                std::path::PathBuf::from("dest.bin"),
            )
            .unwrap_err();
        assert!(matches!(err, SessionError::Aborted));
    }

    // ── Independence ─────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // Finishing one group does not affect the state of another from the same session.
    async fn test_two_groups_are_independent() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let g1 = session.new_download_group().await.unwrap();
        let g2 = session.new_download_group().await.unwrap();
        g1.finish().await.unwrap();
        assert!(!g2.is_finished());
    }

    // ── Round-trip tests ─────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // Downloading a previously uploaded file produces byte-identical content at the destination.
    async fn test_download_file_round_trip() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let original = b"Hello, download round-trip!";
        let file_info = upload_bytes(&session, original, "payload.bin").await.unwrap();

        let dest = temp.path().join("downloaded.bin");
        let group = session.new_download_group().await.unwrap();
        group.download_file_to_path(file_info, dest.clone()).unwrap();
        group.finish().await.unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), original);
    }

    #[tokio::test(flavor = "multi_thread")]
    // Downloading multiple files from a single group produces correct content for each.
    async fn test_download_multiple_files() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();

        let data_a = b"First file content";
        let data_b = b"Second file content - different";

        let commit = session.new_upload_commit().await.unwrap();
        let handle_a = commit.upload_bytes(data_a.to_vec(), Some("a.bin".into())).await.unwrap();
        let handle_b = commit.upload_bytes(data_b.to_vec(), Some("b.bin".into())).await.unwrap();
        let results = commit.commit().await.unwrap();

        let to_file_info = |handle: &crate::xet_session::progress::UploadTaskHandle| -> XetFileInfo {
            let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
            XetFileInfo {
                hash: meta.hash.clone(),
                file_size: meta.file_size,
            }
        };

        let dest_a = temp.path().join("a_out.bin");
        let dest_b = temp.path().join("b_out.bin");
        let group = session.new_download_group().await.unwrap();
        group.download_file_to_path(to_file_info(&handle_a), dest_a.clone()).unwrap();
        group.download_file_to_path(to_file_info(&handle_b), dest_b.clone()).unwrap();
        group.finish().await.unwrap();

        assert_eq!(std::fs::read(&dest_a).unwrap(), data_a);
        assert_eq!(std::fs::read(&dest_b).unwrap(), data_b);
    }

    #[tokio::test(flavor = "multi_thread")]
    // After a successful finish the aggregate download progress reflects bytes received.
    async fn test_download_progress_reflects_bytes_after_finish() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let original = b"download progress tracking data";
        let file_info = upload_bytes(&session, original, "prog.bin").await.unwrap();

        let dest = temp.path().join("out.bin");
        let group = session.new_download_group().await.unwrap();
        let progress_observer = group.clone();
        group.download_file_to_path(file_info, dest).unwrap();
        group.finish().await.unwrap();

        tokio::time::sleep(
            session
                .runtime
                .config()
                .data
                .progress_update_interval
                .saturating_add(Duration::from_secs(1)),
        )
        .await;
        let snapshot = progress_observer.get_progress().unwrap();
        assert!(snapshot.total().total_bytes_completed > 0);
    }

    // ── Per-task result access patterns ──────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // Pattern 1: per-task result is accessible via task_id in the finish() HashMap.
    async fn test_download_result_accessible_via_task_id_in_finish_map() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let data = b"result via task_id in finish map";
        let file_info = upload_bytes(&session, data, "file.bin").await.unwrap();
        let dest = temp.path().join("out.bin");
        let group = session.new_download_group().await.unwrap();
        let handle = group.download_file_to_path(file_info, dest).unwrap();
        let results = group.finish().await.unwrap();
        let result = results.get(&handle.task_id).expect("task_id must be present in results");
        assert_eq!(result.as_ref().as_ref().unwrap().file_info.file_size, data.len() as u64);
    }

    #[tokio::test(flavor = "multi_thread")]
    // DownloadTaskHandle::result() returns None before finish() is called.
    async fn test_download_result_none_before_finish() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let file_info = upload_bytes(&session, b"some data", "file.bin").await.unwrap();
        let dest = temp.path().join("out.bin");
        let group = session.new_download_group().await.unwrap();
        let handle = group.download_file_to_path(file_info, dest).unwrap();
        assert!(handle.result().is_none(), "result must be None before finish()");
        group.finish().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    // DownloadTaskHandle::result() returns Some after finish() completes.
    async fn test_download_result_some_after_finish() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let data = b"download result test data";
        let file_info = upload_bytes(&session, data, "file.bin").await.unwrap();
        let dest = temp.path().join("out.bin");
        let group = session.new_download_group().await.unwrap();
        let handle = group.download_file_to_path(file_info.clone(), dest).unwrap();
        group.finish().await.unwrap();
        let result = handle.result().expect("result must be set after finish()");
        let dl = result.as_ref().as_ref().unwrap();
        assert_eq!(dl.file_info.file_size, data.len() as u64);
        assert_eq!(dl.file_info.hash, file_info.hash);
    }

    // ── Non-tokio executor (Owned-mode bridge) ────────────────────────────────

    #[test]
    // build_async() falls back to Owned mode from a futures executor, and the
    // bridge correctly routes download through the owned thread pool while the
    // future is driven by the caller's executor (futures::block_on).
    fn test_async_bridge_works_from_futures_executor() {
        let temp = tempdir().unwrap();

        futures::executor::block_on(async {
            let session = local_session(&temp).await.unwrap();
            assert_eq!(session.runtime_mode, RuntimeMode::Owned);

            let data = b"hello from futures executor";
            let commit = session.new_upload_commit().await.unwrap();
            let handle = commit.upload_bytes(data.to_vec(), Some("test.bin".into())).await.unwrap();
            let results = commit.commit().await.unwrap();
            let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
            let file_info = XetFileInfo {
                hash: meta.hash.clone(),
                file_size: meta.file_size,
            };

            let dest = temp.path().join("out_futures.bin");
            let group = session.new_download_group().await.unwrap();
            group.download_file_to_path(file_info, dest.clone()).unwrap();
            group.finish().await.unwrap();
            assert_eq!(std::fs::read(&dest).unwrap(), data);
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
            let handle = commit.upload_bytes(data.to_vec(), Some("test.bin".into())).await.unwrap();
            let results = commit.commit().await.unwrap();
            let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
            let file_info = XetFileInfo {
                hash: meta.hash.clone(),
                file_size: meta.file_size,
            };

            let dest = temp.path().join("out_smol.bin");
            let group = session.new_download_group().await.unwrap();
            group.download_file_to_path(file_info, dest.clone()).unwrap();
            group.finish().await.unwrap();
            assert_eq!(std::fs::read(&dest).unwrap(), data);
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
            let handle = commit.upload_bytes(data.to_vec(), Some("test.bin".into())).await.unwrap();
            let results = commit.commit().await.unwrap();
            let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
            let file_info = XetFileInfo {
                hash: meta.hash.clone(),
                file_size: meta.file_size,
            };

            let dest = temp.path().join("out_async_std.bin");
            let group = session.new_download_group().await.unwrap();
            group.download_file_to_path(file_info, dest.clone()).unwrap();
            group.finish().await.unwrap();
            assert_eq!(std::fs::read(&dest).unwrap(), data);
        });
    }
}
