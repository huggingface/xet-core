//! DownloadGroup - groups related downloads

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};

use tokio::task::JoinHandle;
use xet_data::DataError;
use xet_data::processing::{FileDownloadSession, XetFileInfo};
use xet_data::progress_tracking::{GroupProgressReport, UniqueID};
use xet_runtime::core::XetRuntime;

use super::common::{GroupState, create_translator_config};
use super::session::{RuntimeMode, XetSession};
use super::tasks::{DownloadTaskHandle, TaskHandle, TaskStatus};
use crate::error::XetError;

/// API for grouping related file downloads into a single unit of work.
///
/// Obtain via [`XetSession::new_download_group`] (async) or
/// [`XetSession::new_download_group_blocking`] (sync).
///
/// Queue files with [`download_file_to_path`](Self::download_file_to_path) (they start
/// downloading immediately in the background), poll progress with
/// [`get_progress`](Self::get_progress), then call
/// [`finish`](Self::finish) (async) or
/// [`finish_blocking`](Self::finish_blocking) (sync) to wait for all
/// downloads to complete.
///
/// # Cloning
///
/// Cloning is cheap — it simply increments an atomic reference count.
/// All clones share the same background worker and task state.
///
/// # Errors
///
/// Methods return [`XetError::Aborted`] if the parent session has been
/// aborted, and [`XetError::AlreadyFinished`] if
/// [`finish`](Self::finish) has already been called.
#[derive(Clone)]
pub struct DownloadGroup {
    pub(super) inner: Arc<DownloadGroupInner>,
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
    pub(super) async fn new(session: XetSession) -> Result<Self, XetError> {
        let group_id = UniqueID::new();
        let config = create_translator_config(&session)?;
        let download_session = FileDownloadSession::new(Arc::new(config)).await?;

        let inner = Arc::new(DownloadGroupInner {
            group_id,
            session,
            active_tasks: RwLock::new(HashMap::new()),
            download_session: Mutex::new(Some(download_session)),
            state: Mutex::new(GroupState::Alive),
        });

        Ok(Self { inner })
    }

    /// Get the group ID.
    pub(super) fn id(&self) -> UniqueID {
        self.group_id
    }

    /// Abort this download group.
    pub(super) fn abort(&self) -> Result<(), XetError> {
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
    /// Returns [`XetError::Aborted`] if the session has been aborted, or
    /// [`XetError::AlreadyFinished`] if [`finish`](Self::finish) has already
    /// been called.
    pub async fn download_file_to_path(
        &self,
        file_info: XetFileInfo,
        dest_path: PathBuf,
    ) -> Result<DownloadTaskHandle, XetError> {
        self.session.check_alive()?;

        // Use the absolute path in case the process current working directory changes
        // while the task is queued.
        let absolute_path = std::path::absolute(dest_path)?;
        self.inner.start_download_file_to_path(file_info, absolute_path).await
    }

    /// Return a snapshot of progress for every queued download.
    pub fn get_progress(&self) -> Result<GroupProgressReport, XetError> {
        let Some(download_session) = self.download_session.lock()?.clone() else {
            return Ok(GroupProgressReport::default());
        };
        Ok(download_session.report())
    }

    /// Wait for all downloads to complete and return their results.
    ///
    /// Returns a `HashMap` keyed by task ID where each value is
    /// [`DownloadResult`] (= `Arc<Result<`[`DownloadedFile`]`,
    /// [`XetError`](crate::XetError)`>>`). A single failed download
    /// does not prevent the others from being collected.
    ///
    /// Per-task results can also be read directly from the
    /// [`DownloadTaskHandle`] returned by `download_file_to_path` via
    /// [`result`](DownloadTaskHandle::result) after this method returns.
    ///
    /// Consumes `self` — subsequent calls on any clone will return
    /// [`XetError::AlreadyFinished`].
    pub async fn finish(self) -> Result<HashMap<UniqueID, DownloadResult>, XetError> {
        let inner = self.inner.clone();
        self.session
            .dispatch("finish", async move { inner.handle_finish().await })
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

    /// Blocking version of [`download_file_to_path`](Self::download_file_to_path).
    ///
    /// # Errors
    ///
    /// Returns [`XetError::WrongRuntimeMode`] if the session was created with an external
    /// tokio runtime ([`XetSessionBuilder::with_tokio_handle`] / [`XetSessionBuilder::build_async`]
    /// inside a tokio context). Use [`download_file_to_path`](Self::download_file_to_path)`.await`
    /// instead.
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime on an Owned-mode session.
    pub fn download_file_to_path_blocking(
        &self,
        file_info: XetFileInfo,
        dest_path: PathBuf,
    ) -> Result<DownloadTaskHandle, XetError> {
        if matches!(self.session.runtime_mode, RuntimeMode::External) {
            return Err(XetError::wrong_mode(
                "download_file_to_path_blocking() cannot be called on a session using an \
                 external tokio runtime (with_tokio_handle() or tokio build_async()); \
                 use download_file_to_path().await instead",
            ));
        }
        let group = self.clone();
        self.runtime()
            .external_run_async_task(async move { group.download_file_to_path(file_info, dest_path).await })?
    }

    /// Blocking version of [`get_progress`](Self::get_progress).
    pub fn get_progress_blocking(&self) -> Result<GroupProgressReport, XetError> {
        self.get_progress()
    }

    /// Blocking version of [`finish`](Self::finish).
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn finish_blocking(self) -> Result<HashMap<UniqueID, DownloadResult>, XetError> {
        let group = self.clone();
        self.runtime().external_run_async_task(group.finish())?
    }
}

/// Per-file result type returned by [`DownloadGroup::finish`].
///
/// The `Arc` lets the same value be stored in both the `finish()` return map
/// and the per-task [`DownloadTaskHandle`] without requiring the inner
/// `Result` to be `Clone`.
pub type DownloadResult = Arc<Result<DownloadedFile, XetError>>;

/// Handle for a single download task tracked internally by DownloadGroup.
struct InnerDownloadTaskHandle {
    status: Arc<Mutex<TaskStatus>>,
    dest_path: PathBuf,
    file_info: XetFileInfo,
    join_handle: JoinHandle<Result<u64, DataError>>,
    result: Arc<OnceLock<DownloadResult>>,
}

/// All shared state owned by a single DownloadGroup instance.
/// Accessed through `Arc<DownloadGroupInner>`; do not use this type directly.
#[doc(hidden)]
pub struct DownloadGroupInner {
    group_id: UniqueID,
    session: XetSession,

    // Active download tasks for this group
    active_tasks: RwLock<HashMap<UniqueID, InnerDownloadTaskHandle>>,

    // Shared download session (FileDownloadSession from data crate)
    download_session: Mutex<Option<Arc<FileDownloadSession>>>,

    // State
    state: Mutex<GroupState>,
}

impl DownloadGroupInner {
    // ===== State helpers =====

    /// Check whether the group is still accepting new tasks.
    fn check_accepting_tasks(state: &MutexGuard<GroupState>) -> Result<(), XetError> {
        match **state {
            GroupState::Finished => Err(XetError::AlreadyFinished),
            GroupState::Aborted => Err(XetError::Aborted),
            GroupState::Alive => Ok(()),
        }
    }

    async fn start_download_file_to_path(
        self: &Arc<Self>,
        file_info: XetFileInfo,
        dest_path: PathBuf,
    ) -> Result<DownloadTaskHandle, XetError> {
        let download_session = {
            let state = self.state.lock()?;
            Self::check_accepting_tasks(&state)?;

            let Some(download_session) = self.download_session.lock()?.clone() else {
                return Err(XetError::other("Download session not initialized"));
            };
            download_session
            // state guard dropped here before the .await
        };

        let (task_id, join_handle) = self
            .session
            .dispatch("spawn_download_file", {
                let file_info = file_info.clone();
                let dest_path = dest_path.clone();
                async move { download_session.download_file_background(file_info, dest_path).await }
            })
            .await??;

        // Re-check state: if finish() or abort() raced in, cancel the spawned task.
        {
            let state = self.state.lock()?;
            if !matches!(*state, GroupState::Alive) {
                join_handle.abort();
                Self::check_accepting_tasks(&state)?;
            }
        }

        let status = Arc::new(Mutex::new(TaskStatus::Queued));
        let result: Arc<OnceLock<DownloadResult>> = Arc::new(OnceLock::new());
        let task_handle = DownloadTaskHandle {
            inner: TaskHandle {
                status: Some(status.clone()),
                task_id,
            },
            result: result.clone(),
        };

        let handle = InnerDownloadTaskHandle {
            status,
            dest_path,
            file_info,
            join_handle,
            result,
        };

        TaskStatus::mark_running(&handle.status);
        self.active_tasks.write()?.insert(task_id, handle);

        Ok(task_handle)
    }

    /// Join all active download tasks and mark the group as finished.
    pub(super) async fn handle_finish(&self) -> Result<HashMap<UniqueID, DownloadResult>, XetError> {
        // Mark as not accepting new tasks
        {
            let mut state_guard = self.state.lock()?;
            if *state_guard == GroupState::Finished {
                return Err(XetError::AlreadyFinished);
            }
            *state_guard = GroupState::Aborted; // stop new tasks while draining
        }

        // Wait for all downloads to complete
        let active_tasks = std::mem::take(&mut *self.active_tasks.write()?);

        let mut results = HashMap::new();
        let mut join_err = None;
        // Join all tasks first and then propagate errors.
        for (task_id, handle) in active_tasks {
            match handle.join_handle.await {
                Ok(Ok(n_bytes)) => {
                    TaskStatus::mark_terminal(&handle.status, TaskStatus::Completed);
                    let result = Arc::new(Ok(DownloadedFile {
                        dest_path: handle.dest_path,
                        file_info: XetFileInfo {
                            hash: handle.file_info.hash,
                            file_size: Some(n_bytes),
                            sha256: None,
                        },
                    }));
                    results.insert(task_id, result.clone());
                    let _ = handle.result.set(result);
                },
                Ok(Err(data_err)) => {
                    TaskStatus::mark_terminal(&handle.status, TaskStatus::Failed);
                    let result: DownloadResult = Arc::new(Err(data_err.into()));
                    results.insert(task_id, result.clone());
                    let _ = handle.result.set(result);
                },
                Err(e) => {
                    if e.is_cancelled() {
                        TaskStatus::mark_cancelled(&handle.status);
                    } else {
                        TaskStatus::mark_terminal(&handle.status, TaskStatus::Failed);
                    }
                    if join_err.is_none() {
                        join_err = Some(XetError::from(e));
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

    fn abort(&self) -> Result<(), XetError> {
        *self.state.lock()? = GroupState::Aborted;
        let active_tasks = std::mem::take(&mut *self.active_tasks.write()?);
        for (_tracking_id, inner_task_handle) in active_tasks {
            TaskStatus::mark_cancelled(&inner_task_handle.status);
            inner_task_handle.join_handle.abort();
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
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::time::Duration;

    use anyhow::Result;
    use tempfile::{TempDir, tempdir};
    use xet_data::processing::Sha256Policy;

    use super::*;
    use crate::xet_session::session::{RuntimeMode, XetSession, XetSessionBuilder};

    async fn local_session(temp: &TempDir) -> Result<XetSession> {
        let cas_path = temp.path().join("cas");
        Ok(XetSessionBuilder::new()
            .with_endpoint(format!("local://{}", cas_path.display()))
            .build_async()
            .await?)
    }

    async fn upload_bytes(session: &XetSession, data: &[u8], name: &str) -> Result<XetFileInfo> {
        let commit = session.new_upload_commit().await?;
        let handle = commit
            .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some(name.into()))
            .await?;
        let results = commit.commit().await?;
        let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
        Ok(XetFileInfo {
            hash: meta.hash.clone(),
            file_size: Some(meta.file_size),
            sha256: meta.sha256.clone(),
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
    fn test_finish_blocked_while_download_registration_holds_state_lock() -> Result<()> {
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
        let report = group.get_progress().unwrap();
        assert_eq!(report.total_bytes, 0);
        assert_eq!(report.total_bytes_completed, 0);
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
        assert!(matches!(err, XetError::AlreadyFinished | XetError::Internal(_)));
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
                    file_size: Some(1024),
                    sha256: None,
                },
                std::path::PathBuf::from("dest.bin"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, XetError::Aborted));
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
                    file_size: Some(1024),
                    sha256: None,
                },
                std::path::PathBuf::from("dest.bin"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, XetError::AlreadyFinished));
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
                    file_size: Some(1024),
                    sha256: None,
                },
                std::path::PathBuf::from("dest.bin"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, XetError::Aborted));
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
        let handle = group.download_file_to_path(file_info, dest.clone()).await.unwrap();
        assert!(matches!(handle.status().unwrap(), TaskStatus::Queued | TaskStatus::Running | TaskStatus::Completed));
        group.finish().await.unwrap();
        assert!(matches!(handle.status().unwrap(), TaskStatus::Completed));

        assert_eq!(std::fs::read(&dest).unwrap(), original);
    }

    #[tokio::test(flavor = "multi_thread")]
    // A download task that fails transitions to Failed status.
    async fn test_download_status_failed_for_invalid_file_info() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let group = session.new_download_group().await.unwrap();
        let handle = group
            .download_file_to_path(
                XetFileInfo {
                    hash: "abc123".to_string(),
                    file_size: Some(123),
                    sha256: None,
                },
                temp.path().join("missing.bin"),
            )
            .await
            .unwrap();
        let results = group.finish().await.unwrap();
        let task_result = results.get(&handle.task_id).unwrap();
        assert!(task_result.is_err());
        assert!(matches!(handle.status().unwrap(), TaskStatus::Failed));
    }

    #[tokio::test(flavor = "multi_thread")]
    // task_id returned by download_file_to_path must match the per-item progress entry id.
    async fn test_download_task_id_matches_progress_item_id() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let original = b"download id match";
        let file_info = upload_bytes(&session, original, "id.bin").await.unwrap();

        let dest = temp.path().join("download_id.bin");
        let group = session.new_download_group().await.unwrap();
        let handle = group.download_file_to_path(file_info, dest).await.unwrap();

        let download_session = group.inner.download_session.lock().unwrap().clone().unwrap();

        let mut reports = HashMap::new();
        for _ in 0..50 {
            reports = download_session.item_reports();
            if !reports.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(reports.contains_key(&handle.task_id));

        group.finish().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    // Downloading multiple files from a single group produces correct content for each.
    async fn test_download_multiple_files() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();

        let data_a = b"First file content";
        let data_b = b"Second file content - different";

        let commit = session.new_upload_commit().await.unwrap();
        let handle_a = commit
            .upload_bytes(data_a.to_vec(), Sha256Policy::Compute, Some("a.bin".into()))
            .await
            .unwrap();
        let handle_b = commit
            .upload_bytes(data_b.to_vec(), Sha256Policy::Compute, Some("b.bin".into()))
            .await
            .unwrap();
        let results = commit.commit().await.unwrap();

        let to_file_info = |handle: &crate::xet_session::tasks::UploadTaskHandle| -> XetFileInfo {
            let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
            XetFileInfo {
                hash: meta.hash.clone(),
                file_size: Some(meta.file_size),
                sha256: meta.sha256.clone(),
            }
        };

        let dest_a = temp.path().join("a_out.bin");
        let dest_b = temp.path().join("b_out.bin");
        let group = session.new_download_group().await.unwrap();
        group
            .download_file_to_path(to_file_info(&handle_a), dest_a.clone())
            .await
            .unwrap();
        group
            .download_file_to_path(to_file_info(&handle_b), dest_b.clone())
            .await
            .unwrap();
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
        group.download_file_to_path(file_info, dest).await.unwrap();
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
        let report = progress_observer.get_progress().unwrap();
        assert_eq!(report.total_bytes, original.len() as u64);
        assert_eq!(report.total_bytes_completed, original.len() as u64);
        assert_eq!(report.total_transfer_bytes, report.total_transfer_bytes_completed);
        assert!(report.total_transfer_bytes_completed > 0);
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
        let handle = group.download_file_to_path(file_info, dest).await.unwrap();
        let results = group.finish().await.unwrap();
        let result = results.get(&handle.task_id).expect("task_id must be present in results");
        assert_eq!(result.as_ref().as_ref().unwrap().file_info.file_size, Some(data.len() as u64));
    }

    #[tokio::test(flavor = "multi_thread")]
    // DownloadTaskHandle::result() returns None before finish() is called.
    async fn test_download_result_none_before_finish() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let file_info = upload_bytes(&session, b"some data", "file.bin").await.unwrap();
        let dest = temp.path().join("out.bin");
        let group = session.new_download_group().await.unwrap();
        let handle = group.download_file_to_path(file_info, dest).await.unwrap();
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
        let handle = group.download_file_to_path(file_info.clone(), dest).await.unwrap();
        group.finish().await.unwrap();
        let result = handle.result().expect("result must be set after finish()");
        let dl = result.as_ref().as_ref().unwrap();
        assert_eq!(dl.file_info.file_size, Some(data.len() as u64));
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
            let handle = commit
                .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
                .await
                .unwrap();
            let results = commit.commit().await.unwrap();
            let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
            let file_info = XetFileInfo {
                hash: meta.hash.clone(),
                file_size: Some(meta.file_size),
                sha256: meta.sha256.clone(),
            };

            let dest = temp.path().join("out_futures.bin");
            let group = session.new_download_group().await.unwrap();
            group.download_file_to_path(file_info, dest.clone()).await.unwrap();
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
            let handle = commit
                .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
                .await
                .unwrap();
            let results = commit.commit().await.unwrap();
            let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
            let file_info = XetFileInfo {
                hash: meta.hash.clone(),
                file_size: Some(meta.file_size),
                sha256: meta.sha256.clone(),
            };

            let dest = temp.path().join("out_smol.bin");
            let group = session.new_download_group().await.unwrap();
            group.download_file_to_path(file_info, dest.clone()).await.unwrap();
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
            let handle = commit
                .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
                .await
                .unwrap();
            let results = commit.commit().await.unwrap();
            let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
            let file_info = XetFileInfo {
                hash: meta.hash.clone(),
                file_size: Some(meta.file_size),
                sha256: meta.sha256.clone(),
            };

            let dest = temp.path().join("out_async_std.bin");
            let group = session.new_download_group().await.unwrap();
            group.download_file_to_path(file_info, dest.clone()).await.unwrap();
            group.finish().await.unwrap();
            assert_eq!(std::fs::read(&dest).unwrap(), data);
        });
    }

    // ── Blocking API tests ────────────────────────────────────────────────────

    fn local_session_sync(temp: &TempDir) -> Result<XetSession> {
        let cas_path = temp.path().join("cas");
        Ok(XetSessionBuilder::new()
            .with_endpoint(format!("local://{}", cas_path.display()))
            .build()?)
    }

    fn upload_bytes_blocking(session: &XetSession, data: &[u8], name: &str) -> Result<XetFileInfo> {
        let commit = session.new_upload_commit_blocking()?;
        let handle = commit.upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some(name.into()))?;
        let results = commit.commit_blocking()?;
        let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
        Ok(XetFileInfo {
            hash: meta.hash.clone(),
            file_size: Some(meta.file_size),
            sha256: meta.sha256.clone(),
        })
    }

    #[test]
    fn test_blocking_download_file_round_trip() -> Result<()> {
        let temp = tempdir()?;
        let session = local_session_sync(&temp)?;
        let original = b"Hello, download round-trip!";
        let file_info = upload_bytes_blocking(&session, original, "payload.bin")?;

        let dest = temp.path().join("downloaded.bin");
        let group = session.new_download_group_blocking()?;
        group.download_file_to_path_blocking(file_info, dest.clone())?;
        group.finish_blocking()?;

        assert_eq!(std::fs::read(&dest)?, original);
        Ok(())
    }

    #[test]
    fn test_blocking_download_multiple_files() -> Result<()> {
        let temp = tempdir()?;
        let session = local_session_sync(&temp)?;

        let data_a = b"First file content";
        let data_b = b"Second file content - different";

        let commit = session.new_upload_commit_blocking()?;
        let handle_a = commit.upload_bytes_blocking(data_a.to_vec(), Sha256Policy::Compute, Some("a.bin".into()))?;
        let handle_b = commit.upload_bytes_blocking(data_b.to_vec(), Sha256Policy::Compute, Some("b.bin".into()))?;
        let results = commit.commit_blocking()?;

        let to_file_info = |handle: &crate::xet_session::tasks::UploadTaskHandle| -> XetFileInfo {
            let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
            XetFileInfo {
                hash: meta.hash.clone(),
                file_size: Some(meta.file_size),
                sha256: meta.sha256.clone(),
            }
        };

        let dest_a = temp.path().join("a_out.bin");
        let dest_b = temp.path().join("b_out.bin");
        let group = session.new_download_group_blocking()?;
        group.download_file_to_path_blocking(to_file_info(&handle_a), dest_a.clone())?;
        group.download_file_to_path_blocking(to_file_info(&handle_b), dest_b.clone())?;
        group.finish_blocking()?;

        assert_eq!(std::fs::read(&dest_a)?, data_a);
        assert_eq!(std::fs::read(&dest_b)?, data_b);
        Ok(())
    }

    #[test]
    fn test_blocking_download_progress_reflects_bytes_after_finish() -> Result<()> {
        let temp = tempdir()?;
        let session = local_session_sync(&temp)?;
        let original = b"download progress tracking data";
        let file_info = upload_bytes_blocking(&session, original, "prog.bin")?;

        let dest = temp.path().join("out.bin");
        let group = session.new_download_group_blocking()?;
        let progress_observer = group.clone();
        group.download_file_to_path_blocking(file_info, dest)?;
        group.finish_blocking()?;

        std::thread::sleep(
            session
                .runtime
                .config()
                .data
                .progress_update_interval
                .saturating_add(Duration::from_secs(1)),
        );
        let snapshot = progress_observer.get_progress_blocking()?;
        assert_eq!(snapshot.total_bytes, original.len() as u64);
        assert_eq!(snapshot.total_bytes_completed, original.len() as u64);
        assert_eq!(snapshot.total_transfer_bytes, snapshot.total_transfer_bytes_completed);
        assert!(snapshot.total_transfer_bytes_completed > 0);
        Ok(())
    }

    #[test]
    fn test_blocking_download_result_access_patterns() -> Result<()> {
        let temp = tempdir()?;
        let session = local_session_sync(&temp)?;
        let data = b"download result access patterns";
        let file_info = upload_bytes_blocking(&session, data, "file.bin")?;
        let dest = temp.path().join("out.bin");
        let group = session.new_download_group_blocking()?;
        let handle = group.download_file_to_path_blocking(file_info.clone(), dest)?;

        // Before finish, per-task result is not available yet.
        assert!(handle.result().is_none());

        let results = group.finish_blocking()?;

        // Result should be available in the finish map by task id.
        let map_result = results.get(&handle.task_id).expect("task_id must be present in results");
        assert_eq!(map_result.as_ref().as_ref().unwrap().file_info.file_size, Some(data.len() as u64));

        // Result should also be available via the task handle.
        let result = handle.result().expect("result must be set after finish");
        let dl = result.as_ref().as_ref().unwrap();
        assert_eq!(dl.file_info.file_size, Some(data.len() as u64));
        assert_eq!(dl.file_info.hash, file_info.hash);
        Ok(())
    }

    fn assert_blocking_download_round_trip<R>(run: R)
    where
        R: FnOnce(std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>),
    {
        let temp = tempdir().unwrap();
        let session = local_session_sync(&temp).unwrap();

        run(Box::pin(async move {
            let data = b"download from smol executor";
            let file_info = upload_bytes_blocking(&session, data, "test.bin").unwrap();
            let dest = temp.path().join("out_smol.bin");
            let group = session.new_download_group_blocking().unwrap();
            group.download_file_to_path_blocking(file_info, dest.clone()).unwrap();
            group.finish_blocking().unwrap();
            assert_eq!(std::fs::read(&dest).unwrap(), data);
        }));
    }

    #[test]
    fn test_blocking_download_round_trip_in_smol() {
        assert_blocking_download_round_trip(|fut| smol::block_on(fut));
    }

    #[test]
    fn test_blocking_download_round_trip_in_futures_executor() {
        assert_blocking_download_round_trip(|fut| futures::executor::block_on(fut));
    }

    #[test]
    fn test_blocking_download_round_trip_in_async_std() {
        assert_blocking_download_round_trip(|fut| async_std::task::block_on(fut));
    }

    // ── RuntimeMode checks ────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // download_file_to_path_blocking returns WrongRuntimeMode on an External-mode session.
    async fn test_download_file_to_path_blocking_errors_in_external_mode() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        assert_eq!(session.runtime_mode, RuntimeMode::External);
        let group = session.new_download_group().await.unwrap();
        let file_info = XetFileInfo {
            hash: String::new(),
            file_size: 0,
            sha256: None,
        };
        let err = group
            .download_file_to_path_blocking(file_info, PathBuf::from("/nonexistent"))
            .err()
            .unwrap();
        assert!(matches!(err, XetError::WrongRuntimeMode(_)));
    }

    // ── Owned-mode _blocking panic guard ─────────────────────────────────────

    #[test]
    // download_file_to_path_blocking panics when called from within a tokio runtime on an
    // Owned-mode session: external_run_async_task calls handle.block_on(), which panics
    // because tokio sets a thread-local runtime context that it detects and rejects.
    fn test_download_file_to_path_blocking_panics_in_async_context() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime_mode, RuntimeMode::Owned);
        let group = session.new_download_group_blocking().unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let file_info = XetFileInfo {
            hash: String::new(),
            file_size: 0,
            sha256: None,
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async { group.download_file_to_path_blocking(file_info, PathBuf::from("/nonexistent")) })
        }));
        assert!(result.is_err(), "download_file_to_path_blocking() must panic when called from async");
    }
}
