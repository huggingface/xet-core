//! XetFileDownloadGroup - groups related downloads

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tracing::info;
use xet_data::processing::{FileDownloadSession, XetFileInfo};
use xet_data::progress_tracking::{GroupProgressReport, UniqueID};

use super::auth_group_builder::{AuthGroupBuilder, AuthOptions};
use super::common::create_translator_config;
use super::file_download_handle::{XetDownloadReport, XetFileDownload, XetFileDownloadInner};
use super::session::XetSession;
use super::task_runtime::{BackgroundTaskState, TaskRuntime, XetTaskState};
use crate::error::XetError;

pub type XetFileDownloadGroupBuilder = AuthGroupBuilder<XetFileDownloadGroup>;

impl AuthGroupBuilder<XetFileDownloadGroup> {
    /// Create the [`XetFileDownloadGroup`] from an async context.
    pub async fn build(self) -> Result<XetFileDownloadGroup, XetError> {
        let AuthGroupBuilder {
            session, auth_options, ..
        } = self;
        let parent_runtime = session.inner.task_runtime.clone();
        let child_parent = parent_runtime.clone();
        let group = parent_runtime
            .bridge_async("new_file_download_group", async move {
                let group_runtime = child_parent.child()?;
                XetFileDownloadGroup::new(session, group_runtime, auth_options).await
            })
            .await?;
        info!("New file download group, session_id={}, group_id={}", group.session().id(), group.id());
        group.session().register_file_download_group(&group)?;

        Ok(group)
    }

    /// Create the [`XetFileDownloadGroup`] from a sync context.
    ///
    /// # Errors
    ///
    /// Returns [`XetError::WrongRuntimeMode`] if the session wraps an external
    /// tokio runtime (created via [`XetSessionBuilder::with_tokio_handle`] or
    /// auto-detected inside `#[tokio::main]`).
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime on an Owned-mode session.
    pub fn build_blocking(self) -> Result<XetFileDownloadGroup, XetError> {
        let AuthGroupBuilder {
            session, auth_options, ..
        } = self;
        let parent_runtime = session.inner.task_runtime.clone();
        let child_parent = parent_runtime.clone();
        let group = parent_runtime.bridge_sync("new_file_download_group_blocking", async move {
            let group_runtime = child_parent.child()?;
            XetFileDownloadGroup::new(session, group_runtime, auth_options).await
        })?;
        info!("New file download group, session_id={}, group_id={}", group.session().id(), group.id());
        group.session().register_file_download_group(&group)?;
        Ok(group)
    }
}

/// Report returned by [`XetFileDownloadGroup::finish`].
///
/// Contains final progress and per-file results keyed by [`UniqueID`].
/// Only created when all downloads succeed; any failure propagates as an error.
#[derive(Clone, Debug)]
pub struct XetDownloadGroupReport {
    /// Final progress snapshot at the time the group finished.
    pub progress: GroupProgressReport,
    /// Per-file download reports keyed by task ID.
    pub downloads: HashMap<UniqueID, XetDownloadReport>,
}

/// API for grouping related file downloads into a single unit of work.
///
/// Obtain via [`XetSession::new_file_download_group`] — configure per-group
/// auth on the returned [`AuthGroupBuilder`], then call
/// [`build`](AuthGroupBuilder::build) (async) or
/// [`build_blocking`](AuthGroupBuilder::build_blocking) (sync).
///
/// Queue files with [`download_file_to_path`](Self::download_file_to_path) (they start
/// downloading immediately in the background), poll progress with
/// [`progress`](Self::progress), then call
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
/// Methods return [`XetError::UserCancelled`] if the parent session has been
/// aborted, and [`XetError::AlreadyCompleted`] if
/// [`finish`](Self::finish) has already been called.
#[derive(Clone)]
pub struct XetFileDownloadGroup {
    pub(super) inner: Arc<XetFileDownloadGroupInner>,
    pub(super) task_runtime: Arc<TaskRuntime>,
}

impl XetFileDownloadGroup {
    /// Create a new download group from an **async** context. Initialisation logic shared by the sync and async
    /// constructors.
    pub(super) async fn new(
        session: XetSession,
        task_runtime: Arc<TaskRuntime>,
        auth_options: AuthOptions,
    ) -> Result<Self, XetError> {
        let group_id = UniqueID::new();
        let config = create_translator_config(&session, auth_options).await?;
        let download_session = FileDownloadSession::new(Arc::new(config), None).await?;

        let inner = Arc::new(XetFileDownloadGroupInner {
            group_id,
            session,
            active_tasks: RwLock::new(HashMap::new()),
            download_session: download_session.clone(),
        });

        Ok(Self { inner, task_runtime })
    }

    /// Unique identifier for this download group.
    pub fn id(&self) -> UniqueID {
        self.inner.group_id
    }

    fn session(&self) -> &XetSession {
        &self.inner.session
    }

    /// Cancel all active downloads in this group.
    pub fn abort(&self) -> Result<(), XetError> {
        info!(group_id = %self.id(), "Download group abort");
        self.task_runtime.cancel_subtree()?;
        for (_tracking_id, handle) in self.inner.active_tasks.read()?.iter() {
            handle.cancel();
        }
        Ok(())
    }

    /// Queue a file for download to `dest_path`, starting the transfer immediately if system resource permits.
    ///
    /// # Parameters
    ///
    /// * `file_info` – Content-addressed hash and size returned by a previous
    ///   [`XetUploadCommit::commit`](crate::xet_session::XetUploadCommit::commit).
    /// * `dest_path` – Local path where the downloaded file will be written. Parent directories are created
    ///   automatically.
    ///
    /// Returns a [`XetFileDownload`] that can be used to poll status and per-file
    /// progress without taking the GIL.
    ///
    /// # Errors
    ///
    /// Returns [`XetError::UserCancelled`] if the session has been aborted, or
    /// [`XetError::AlreadyCompleted`] if [`finish`](Self::finish) has already
    /// been called.
    pub async fn download_file_to_path(
        &self,
        file_info: XetFileInfo,
        dest_path: PathBuf,
    ) -> Result<XetFileDownload, XetError> {
        info!(
            group_id = %self.id(),
            dest_path = ?dest_path,
            hash = %file_info.hash,
            "Download file to path"
        );
        let inner = self.inner.clone();
        let task_runtime = self.task_runtime.clone();
        self.task_runtime
            .bridge_async("download_file_to_path", async move {
                inner.start_download_file_to_path(file_info, dest_path, &task_runtime).await
            })
            .await
    }

    /// Return a snapshot of progress for every queued download.
    pub fn progress(&self) -> GroupProgressReport {
        self.inner.download_session.report()
    }

    pub fn status(&self) -> Result<XetTaskState, XetError> {
        self.task_runtime.status()
    }

    /// Wait for all downloads to complete and return a report.
    ///
    /// Returns an [`XetDownloadGroupReport`] with per-file
    /// [`XetDownloadReport`] entries keyed by task ID. If any download
    /// fails, the first error is propagated immediately.
    ///
    /// Consumes `self` — subsequent calls on any clone will return
    /// [`XetError::AlreadyCompleted`].
    pub async fn finish(self) -> Result<XetDownloadGroupReport, XetError> {
        info!(group_id = %self.id(), "Download group finish");
        let inner = self.inner.clone();
        let download_session = self.inner.download_session.clone();
        let downloads = self
            .task_runtime
            .bridge_async_finalizing("download_finish", false, async move { inner.handle_finish().await })
            .await?;
        let progress = download_session.report();
        Ok(XetDownloadGroupReport { progress, downloads })
    }

    /// Returns `true` if [`finish`](Self::finish) has been called and completed.
    #[cfg(test)]
    fn is_finished(&self) -> bool {
        matches!(self.task_runtime.status(), Ok(XetTaskState::Completed))
    }

    /// Blocking version of [`download_file_to_path`](Self::download_file_to_path).
    ///
    /// # Errors
    ///
    /// Returns [`XetError::WrongRuntimeMode`] if the session was created with an external
    /// tokio runtime. Use [`download_file_to_path`](Self::download_file_to_path)`.await`
    /// instead.
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime on an Owned-mode session.
    pub fn download_file_to_path_blocking(
        &self,
        file_info: XetFileInfo,
        dest_path: PathBuf,
    ) -> Result<XetFileDownload, XetError> {
        info!(
            group_id = %self.id(),
            dest_path = ?dest_path,
            hash = %file_info.hash,
            "Download file to path"
        );
        let inner = self.inner.clone();
        let task_runtime = self.task_runtime.clone();
        self.task_runtime.bridge_sync("download_file_to_path_blocking", async move {
            inner.start_download_file_to_path(file_info, dest_path, &task_runtime).await
        })
    }

    /// Blocking version of [`finish`](Self::finish).
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn finish_blocking(self) -> Result<XetDownloadGroupReport, XetError> {
        info!(group_id = %self.id(), "Download group finish");
        let inner = self.inner.clone();
        let download_session = self.inner.download_session.clone();
        let downloads = self
            .task_runtime
            .bridge_sync_finalizing("download_finish_blocking", false, async move { inner.handle_finish().await })?;
        let progress = download_session.report();
        Ok(XetDownloadGroupReport { progress, downloads })
    }
}

pub(super) struct XetFileDownloadGroupInner {
    group_id: UniqueID,
    pub(super) session: XetSession,
    active_tasks: RwLock<HashMap<UniqueID, XetFileDownload>>,
    pub(super) download_session: Arc<FileDownloadSession>,
}

impl XetFileDownloadGroupInner {
    async fn start_download_file_to_path(
        self: &Arc<Self>,
        file_info: XetFileInfo,
        dest_path: PathBuf,
        parent_task_runtime: &Arc<TaskRuntime>,
    ) -> Result<XetFileDownload, XetError> {
        let absolute_path = std::path::absolute(dest_path)?;
        let (task_id, join_handle) = self
            .download_session
            .download_file_background(file_info.clone(), absolute_path.clone())
            .await?;

        let task_runtime = parent_task_runtime.child()?;
        let token = task_runtime.cancellation_token();

        let fi = file_info.clone();
        let dp = absolute_path.clone();
        let ds = self.download_session.clone();
        let mut download_join_handle = join_handle;
        let mapped_handle = tokio::spawn(async move {
            tokio::select! {
                // Propagate TaskRuntime cancellation through the mapped background task.
                // We abort the owned download join handle here so cancellation does not
                // leave the file download task detached in the runtime.
                _ = token.cancelled() => {
                    download_join_handle.abort();
                    Err(XetError::UserCancelled("download task cancelled by user".to_string()))
                }
                join_result = &mut download_join_handle => {
                    match join_result {
                        Ok(Ok(n_bytes)) => Ok(XetDownloadReport {
                            task_id,
                            path: Some(dp),
                            file_info: XetFileInfo {
                                hash: fi.hash,
                                file_size: Some(n_bytes),
                                sha256: fi.sha256,
                            },
                            progress: ds.item_report(task_id),
                        }),
                        Ok(Err(e)) => Err(XetError::TaskError(e.to_string())),
                        Err(e) => Err(XetError::TaskError(e.to_string())),
                    }
                }
            }
        });

        let inner = Arc::new(XetFileDownloadInner {
            task_id,
            dest_path: absolute_path,
            download_session: self.download_session.clone(),
            state: tokio::sync::Mutex::new(BackgroundTaskState::Running {
                join_handle: Some(mapped_handle),
            }),
        });

        self.active_tasks.write()?.insert(
            task_id,
            XetFileDownload {
                inner: inner.clone(),
                task_runtime: task_runtime.clone(),
            },
        );

        Ok(XetFileDownload { inner, task_runtime })
    }

    pub(super) async fn handle_finish(self: &Arc<Self>) -> Result<HashMap<UniqueID, XetDownloadReport>, XetError> {
        let active_tasks = std::mem::take(&mut *self.active_tasks.write()?);

        let mut results = HashMap::new();
        let mut first_error: Option<XetError> = None;
        for (task_id, handle) in active_tasks {
            match handle.finish().await {
                Ok(report) => {
                    results.insert(task_id, report);
                },
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    } else {
                        tracing::error!(task_id = %task_id, err = %e, "Download task failed");
                    }
                },
            }
        }
        self.session.finish_file_download_group(self.group_id)?;

        match first_error {
            Some(e) => Err(e),
            None => Ok(results),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::time::Duration;

    use anyhow::Result;
    use tempfile::tempdir;
    use xet_data::processing::Sha256Policy;
    use xet_runtime::core::RuntimeMode;

    use super::*;
    use crate::xet_session::session::{XetSession, XetSessionBuilder};

    async fn upload_bytes(session: &XetSession, endpoint: &str, data: &[u8], name: &str) -> Result<XetFileInfo> {
        let commit = session.new_upload_commit()?.with_endpoint(endpoint).build().await?;
        let _handle = commit
            .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some(name.into()))
            .await?;
        let results = commit.commit().await?;
        let meta = results.uploads.into_values().next().expect("one uploaded file");
        Ok(meta.xet_info)
    }

    // ── Mutex guard / concurrency test ───────────────────────────────────────
    //
    // `finish()` takes a write lock on `active_tasks` while draining. We verify
    // it blocks when that write lock is already held.

    #[test]
    // finish() must block while download_file_to_path() holds the state lock.
    fn test_finish_blocked_while_download_registration_holds_state_lock() -> Result<()> {
        let session = XetSessionBuilder::new().build()?;
        let runtime = session.inner.runtime.clone();
        let group = session.new_file_download_group()?.build_blocking()?;
        let group_for_thread = group.clone();
        let runtime_for_thread = runtime.clone();

        // Hold active_tasks write lock to block finish() drain.
        let guard = group.inner.active_tasks.write().unwrap();

        let (done_tx, done_rx) = mpsc::channel::<()>();
        let join_handle = std::thread::spawn(move || {
            let _ = runtime_for_thread.bridge_sync(async move { group_for_thread.finish().await });
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
        let session = XetSessionBuilder::new().build().unwrap();
        let g1 = session.new_file_download_group().unwrap().build().await.unwrap();
        let g2 = session.new_file_download_group().unwrap().build().await.unwrap();
        assert_ne!(g1.id(), g2.id());
    }

    // ── Initial state ────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // A fresh group has all-zero aggregate progress.
    async fn test_progress_empty_initially() {
        let session = XetSessionBuilder::new().build().unwrap();
        let group = session.new_file_download_group().unwrap().build().await.unwrap();
        let report = group.progress();
        assert_eq!(report.total_bytes, 0);
        assert_eq!(report.total_bytes_completed, 0);
    }

    // ── Finish lifecycle ─────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // An empty finish succeeds and returns an empty result set.
    async fn test_finish_empty_succeeds() {
        let session = XetSessionBuilder::new().build().unwrap();
        let group = session.new_file_download_group().unwrap().build().await.unwrap();
        let report = group.finish().await.unwrap();
        assert!(report.downloads.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    // finish() transitions the group into the Finished state.
    async fn test_finish_marks_as_finished() {
        let session = XetSessionBuilder::new().build().unwrap();
        let group = session.new_file_download_group().unwrap().build().await.unwrap();
        let group_clone = group.clone();
        group.finish().await.unwrap();
        assert!(group_clone.is_finished());
    }

    #[tokio::test(flavor = "multi_thread")]
    // A second finish() call on any clone returns AlreadyCompleted.
    async fn test_second_finish_fails() {
        let session = XetSessionBuilder::new().build().unwrap();
        let g1 = session.new_file_download_group().unwrap().build().await.unwrap();
        let g2 = g1.clone();
        g1.finish().await.unwrap();
        let err = g2.finish().await.unwrap_err();
        assert!(matches!(err, XetError::AlreadyCompleted));
    }

    #[tokio::test(flavor = "multi_thread")]
    // finish() unregisters the group from the session's active set.
    async fn test_finish_unregisters_from_session() {
        let session = XetSessionBuilder::new().build().unwrap();
        let group = session.new_file_download_group().unwrap().build().await.unwrap();
        assert_eq!(session.inner.active_file_download_groups.lock().unwrap().len(), 1);
        group.finish().await.unwrap();
        assert_eq!(session.inner.active_file_download_groups.lock().unwrap().len(), 0);
    }

    // ── Guards ───────────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // download_file_to_path returns UserCancelled when the parent session has been aborted.
    async fn test_download_file_on_aborted_session_returns_error() {
        let session = XetSessionBuilder::new().build().unwrap();
        let group = session.new_file_download_group().unwrap().build().await.unwrap();
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
        assert!(matches!(err, XetError::UserCancelled(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    // download_file_to_path after finish returns AlreadyCompleted.
    async fn test_download_file_after_finish_fails() {
        let session = XetSessionBuilder::new().build().unwrap();
        let g1 = session.new_file_download_group().unwrap().build().await.unwrap();
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
        assert!(matches!(err, XetError::AlreadyCompleted));
    }

    #[tokio::test(flavor = "multi_thread")]
    // download_file_to_path on a directly-aborted group returns Aborted.
    async fn test_download_file_on_aborted_group_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        let group = session.new_file_download_group().unwrap().build().await.unwrap();
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
        assert!(matches!(err, XetError::UserCancelled(_)));
    }

    // ── Independence ─────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // Finishing one group does not affect the state of another from the same session.
    async fn test_two_groups_are_independent() {
        let session = XetSessionBuilder::new().build().unwrap();
        let g1 = session.new_file_download_group().unwrap().build().await.unwrap();
        let g2 = session.new_file_download_group().unwrap().build().await.unwrap();
        g1.finish().await.unwrap();
        assert!(!g2.is_finished());
    }

    // ── Round-trip tests ─────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // Downloading a previously uploaded file produces byte-identical content at the destination.
    async fn test_download_file_round_trip() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let original = b"Hello, download round-trip!";
        let file_info = upload_bytes(&session, &endpoint, original, "payload.bin").await.unwrap();

        let dest = temp.path().join("downloaded.bin");
        let group = session
            .new_file_download_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let handle = group.download_file_to_path(file_info, dest.clone()).await.unwrap();
        assert!(matches!(handle.status().unwrap(), XetTaskState::Running | XetTaskState::Completed));
        group.finish().await.unwrap();
        assert!(matches!(handle.status().unwrap(), XetTaskState::Completed));

        assert_eq!(std::fs::read(&dest).unwrap(), original);
    }

    #[tokio::test(flavor = "multi_thread")]
    // A download task that fails transitions to Error status.
    async fn test_download_status_failed_for_invalid_file_info() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let group = session
            .new_file_download_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
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
        let err = group.finish().await.unwrap_err();
        assert!(matches!(err, XetError::TaskError(_)));
        assert!(matches!(handle.status().unwrap(), XetTaskState::Error(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    // task_id returned by download_file_to_path must match the per-item progress entry id.
    async fn test_download_task_id_matches_progress_item_id() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let original = b"download id match";
        let file_info = upload_bytes(&session, &endpoint, original, "id.bin").await.unwrap();

        let dest = temp.path().join("download_id.bin");
        let group = session
            .new_file_download_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let handle = group.download_file_to_path(file_info, dest).await.unwrap();

        let mut reports = HashMap::new();
        for _ in 0..50 {
            reports = group.inner.download_session.item_reports();
            if !reports.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(reports.contains_key(&handle.task_id()));

        group.finish().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    // Downloading multiple files from a single group produces correct content for each.
    async fn test_download_multiple_files() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());

        let data_a = b"First file content";
        let data_b = b"Second file content - different";

        let (file_a_info, file_b_info) = {
            let commit = session
                .new_upload_commit()
                .unwrap()
                .with_endpoint(&endpoint)
                .build()
                .await
                .unwrap();
            let _handle_a = commit
                .upload_bytes(data_a.to_vec(), Sha256Policy::Compute, Some("a.bin".into()))
                .await
                .unwrap();
            let _handle_b = commit
                .upload_bytes(data_b.to_vec(), Sha256Policy::Compute, Some("b.bin".into()))
                .await
                .unwrap();
            let results = commit.commit().await.unwrap();
            assert_eq!(results.uploads.len(), 2);
            let mut metas: Vec<_> = results.uploads.into_values().collect();
            metas.sort_by(|a, b| a.tracking_name.cmp(&b.tracking_name));
            (metas[0].xet_info.clone(), metas[1].xet_info.clone())
        };

        let dest_a = temp.path().join("a_out.bin");
        let dest_b = temp.path().join("b_out.bin");
        let group = session
            .new_file_download_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        group.download_file_to_path(file_a_info, dest_a.clone()).await.unwrap();
        group.download_file_to_path(file_b_info, dest_b.clone()).await.unwrap();
        group.finish().await.unwrap();

        assert_eq!(std::fs::read(&dest_a).unwrap(), data_a);
        assert_eq!(std::fs::read(&dest_b).unwrap(), data_b);
    }

    #[tokio::test(flavor = "multi_thread")]
    // After a successful finish the aggregate download progress reflects bytes received.
    async fn test_download_progress_reflects_bytes_after_finish() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let original = b"download progress tracking data";
        let file_info = upload_bytes(&session, &endpoint, original, "prog.bin").await.unwrap();

        let dest = temp.path().join("out.bin");
        let group = session
            .new_file_download_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let progress_observer = group.clone();
        group.download_file_to_path(file_info, dest).await.unwrap();
        let finish_report = group.finish().await.unwrap();
        assert_eq!(finish_report.progress.total_bytes, original.len() as u64);
        assert_eq!(finish_report.progress.total_bytes_completed, original.len() as u64);

        tokio::time::sleep(
            session
                .inner
                .runtime
                .config()
                .data
                .progress_update_interval
                .saturating_add(Duration::from_secs(1)),
        )
        .await;
        let report = progress_observer.progress();
        assert_eq!(report.total_bytes, original.len() as u64);
        assert_eq!(report.total_bytes_completed, original.len() as u64);
        assert_eq!(report.total_transfer_bytes, report.total_transfer_bytes_completed);
        assert!(report.total_transfer_bytes_completed > 0);
    }

    // ── Per-task result access patterns ──────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // Pattern 1: per-task result is accessible via task_id in the finish report downloads map.
    async fn test_download_result_accessible_via_task_id_in_finish_map() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let data = b"result via task_id in finish map";
        let file_info = upload_bytes(&session, &endpoint, data, "file.bin").await.unwrap();
        let dest = temp.path().join("out.bin");
        let group = session
            .new_file_download_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let handle = group.download_file_to_path(file_info, dest).await.unwrap();
        let report = group.finish().await.unwrap();
        let result = report
            .downloads
            .get(&handle.task_id())
            .expect("task_id must be present in results");
        assert_eq!(result.file_info.file_size, Some(data.len() as u64));
    }

    #[tokio::test(flavor = "multi_thread")]
    // XetFileDownload::result() returns None before finish() is called.
    async fn test_download_result_none_before_finish() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let file_info = upload_bytes(&session, &endpoint, b"some data", "file.bin").await.unwrap();
        let dest = temp.path().join("out.bin");
        let group = session
            .new_file_download_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let handle = group.download_file_to_path(file_info, dest).await.unwrap();
        assert!(handle.result().is_none(), "result must be None before finish()");
        group.finish().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    // XetFileDownload::result() returns Some after finish() completes.
    async fn test_download_result_some_after_finish() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let data = b"download result test data";
        let file_info = upload_bytes(&session, &endpoint, data, "file.bin").await.unwrap();
        let dest = temp.path().join("out.bin");
        let group = session
            .new_file_download_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let handle = group.download_file_to_path(file_info.clone(), dest).await.unwrap();
        group.finish().await.unwrap();
        let result = handle.result().expect("result must be set after finish()");
        let dl = result.unwrap();
        assert_eq!(dl.file_info.file_size, Some(data.len() as u64));
        assert_eq!(dl.file_info.hash, file_info.hash);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_download_finish_second_call_returns_cached_result() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let data = b"download finish cache test";
        let file_info = upload_bytes(&session, &endpoint, data, "cache.bin").await.unwrap();
        let dest = temp.path().join("cache.out");
        let group = session
            .new_file_download_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let handle = group.download_file_to_path(file_info, dest).await.unwrap();
        let first = handle.finish().await.unwrap();
        let second = handle.finish().await.unwrap();
        assert_eq!(first.file_info.hash, second.file_info.hash);
        assert_eq!(first.path, second.path);
        group.finish().await.unwrap();
    }

    // ── Non-tokio executor (Owned-mode bridge) ────────────────────────────────

    #[test]
    // `build()` falls back to Owned mode from a futures executor, and the
    // bridge correctly routes download through the owned thread pool while the
    // future is driven by the caller's executor (futures::block_on).
    fn test_async_bridge_works_from_futures_executor() {
        let temp = tempdir().unwrap();

        futures::executor::block_on(async {
            let session = XetSessionBuilder::new().build().unwrap();
            let endpoint = format!("local://{}", temp.path().join("cas").display());
            assert_eq!(session.inner.runtime.mode(), RuntimeMode::Owned);

            let data = b"hello from futures executor";
            let file_info = {
                let commit = session
                    .new_upload_commit()
                    .unwrap()
                    .with_endpoint(&endpoint)
                    .build()
                    .await
                    .unwrap();
                let _handle = commit
                    .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
                    .await
                    .unwrap();
                let results = commit.commit().await.unwrap();
                results.uploads.into_values().next().expect("one uploaded file").xet_info
            };

            let dest = temp.path().join("out_futures.bin");
            let group = session
                .new_file_download_group()
                .unwrap()
                .with_endpoint(&endpoint)
                .build()
                .await
                .unwrap();
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
            let session = XetSessionBuilder::new().build().unwrap();
            let endpoint = format!("local://{}", temp.path().join("cas").display());
            assert_eq!(session.inner.runtime.mode(), RuntimeMode::Owned);

            let data = b"hello from smol executor";
            let file_info = {
                let commit = session
                    .new_upload_commit()
                    .unwrap()
                    .with_endpoint(&endpoint)
                    .build()
                    .await
                    .unwrap();
                let _handle = commit
                    .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
                    .await
                    .unwrap();
                let results = commit.commit().await.unwrap();
                results.uploads.into_values().next().expect("one uploaded file").xet_info
            };

            let dest = temp.path().join("out_smol.bin");
            let group = session
                .new_file_download_group()
                .unwrap()
                .with_endpoint(&endpoint)
                .build()
                .await
                .unwrap();
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
            let session = XetSessionBuilder::new().build().unwrap();
            let endpoint = format!("local://{}", temp.path().join("cas").display());
            assert_eq!(session.inner.runtime.mode(), RuntimeMode::Owned);

            let data = b"hello from async-std executor";
            let file_info = {
                let commit = session
                    .new_upload_commit()
                    .unwrap()
                    .with_endpoint(&endpoint)
                    .build()
                    .await
                    .unwrap();
                let _handle = commit
                    .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("test.bin".into()))
                    .await
                    .unwrap();
                let results = commit.commit().await.unwrap();
                results.uploads.into_values().next().expect("one uploaded file").xet_info
            };

            let dest = temp.path().join("out_async_std.bin");
            let group = session
                .new_file_download_group()
                .unwrap()
                .with_endpoint(&endpoint)
                .build()
                .await
                .unwrap();
            group.download_file_to_path(file_info, dest.clone()).await.unwrap();
            group.finish().await.unwrap();
            assert_eq!(std::fs::read(&dest).unwrap(), data);
        });
    }

    // ── Blocking API tests ────────────────────────────────────────────────────

    fn upload_bytes_blocking(session: &XetSession, endpoint: &str, data: &[u8], name: &str) -> Result<XetFileInfo> {
        let commit = session.new_upload_commit()?.with_endpoint(endpoint).build_blocking()?;
        let _handle = commit.upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some(name.into()))?;
        let results = commit.commit_blocking()?;
        let meta = results.uploads.into_values().next().expect("one uploaded file");
        Ok(meta.xet_info)
    }

    #[test]
    fn test_blocking_download_file_round_trip() -> Result<()> {
        let temp = tempdir()?;
        let session = XetSessionBuilder::new().build()?;
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let original = b"Hello, download round-trip!";
        let file_info = upload_bytes_blocking(&session, &endpoint, original, "payload.bin")?;

        let dest = temp.path().join("downloaded.bin");
        let group = session.new_file_download_group()?.with_endpoint(&endpoint).build_blocking()?;
        group.download_file_to_path_blocking(file_info, dest.clone())?;
        group.finish_blocking()?;

        assert_eq!(std::fs::read(&dest)?, original);
        Ok(())
    }

    #[test]
    fn test_blocking_download_multiple_files() -> Result<()> {
        let temp = tempdir()?;
        let session = XetSessionBuilder::new().build()?;
        let endpoint = format!("local://{}", temp.path().join("cas").display());

        let data_a = b"First file content";
        let data_b = b"Second file content - different";

        let (file_a_info, file_b_info) = {
            let commit = session.new_upload_commit()?.with_endpoint(&endpoint).build_blocking()?;
            let _handle_a =
                commit.upload_bytes_blocking(data_a.to_vec(), Sha256Policy::Compute, Some("a.bin".into()))?;
            let _handle_b =
                commit.upload_bytes_blocking(data_b.to_vec(), Sha256Policy::Compute, Some("b.bin".into()))?;
            let results = commit.commit_blocking()?;
            assert_eq!(results.uploads.len(), 2);
            let mut metas: Vec<_> = results.uploads.into_values().collect();
            metas.sort_by(|a, b| a.tracking_name.cmp(&b.tracking_name));
            (metas[0].xet_info.clone(), metas[1].xet_info.clone())
        };

        let dest_a = temp.path().join("a_out.bin");
        let dest_b = temp.path().join("b_out.bin");
        let group = session.new_file_download_group()?.with_endpoint(&endpoint).build_blocking()?;
        group.download_file_to_path_blocking(file_a_info, dest_a.clone())?;
        group.download_file_to_path_blocking(file_b_info, dest_b.clone())?;
        group.finish_blocking()?;

        assert_eq!(std::fs::read(&dest_a)?, data_a);
        assert_eq!(std::fs::read(&dest_b)?, data_b);
        Ok(())
    }

    #[test]
    fn test_blocking_download_progress_reflects_bytes_after_finish() -> Result<()> {
        let temp = tempdir()?;
        let session = XetSessionBuilder::new().build()?;
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let original = b"download progress tracking data";
        let file_info = upload_bytes_blocking(&session, &endpoint, original, "prog.bin")?;

        let dest = temp.path().join("out.bin");
        let group = session.new_file_download_group()?.with_endpoint(&endpoint).build_blocking()?;
        let progress_observer = group.clone();
        group.download_file_to_path_blocking(file_info, dest)?;
        group.finish_blocking()?;

        std::thread::sleep(
            session
                .inner
                .runtime
                .config()
                .data
                .progress_update_interval
                .saturating_add(Duration::from_secs(1)),
        );
        let snapshot = progress_observer.progress();
        assert_eq!(snapshot.total_bytes, original.len() as u64);
        assert_eq!(snapshot.total_bytes_completed, original.len() as u64);
        assert_eq!(snapshot.total_transfer_bytes, snapshot.total_transfer_bytes_completed);
        assert!(snapshot.total_transfer_bytes_completed > 0);
        Ok(())
    }

    #[test]
    fn test_blocking_download_result_access_patterns() -> Result<()> {
        let temp = tempdir()?;
        let session = XetSessionBuilder::new().build()?;
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let data = b"download result access patterns";
        let file_info = upload_bytes_blocking(&session, &endpoint, data, "file.bin")?;
        let dest = temp.path().join("out.bin");
        let group = session.new_file_download_group()?.with_endpoint(&endpoint).build_blocking()?;
        let handle = group.download_file_to_path_blocking(file_info.clone(), dest)?;

        // Before finish, per-task result is not available yet.
        assert!(handle.result().is_none());

        let report = group.finish_blocking()?;

        // Result should be available in the finish map by task id.
        let map_result = report
            .downloads
            .get(&handle.task_id())
            .expect("task_id must be present in results");
        assert_eq!(map_result.file_info.file_size, Some(data.len() as u64));

        // Result should also be available via the task handle.
        let result = handle.result().expect("result must be set after finish");
        let dl = result.unwrap();
        assert_eq!(dl.file_info.file_size, Some(data.len() as u64));
        assert_eq!(dl.file_info.hash, file_info.hash);
        Ok(())
    }

    fn assert_blocking_download_round_trip<R>(run: R)
    where
        R: FnOnce(std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>),
    {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());

        run(Box::pin(async move {
            let data = b"download from smol executor";
            let file_info = upload_bytes_blocking(&session, &endpoint, data, "test.bin").unwrap();
            let dest = temp.path().join("out_smol.bin");
            let group = session
                .new_file_download_group()
                .unwrap()
                .with_endpoint(&endpoint)
                .build_blocking()
                .unwrap();
            group.download_file_to_path_blocking(file_info, dest.clone()).unwrap();
            group.finish_blocking().unwrap();
            assert_eq!(std::fs::read(&dest).unwrap(), data);
        }));
    }

    #[test]
    fn test_blocking_download_round_trip_in_smol() {
        assert_blocking_download_round_trip(smol::block_on);
    }

    #[test]
    fn test_blocking_download_round_trip_in_futures_executor() {
        assert_blocking_download_round_trip(futures::executor::block_on);
    }

    #[test]
    fn test_blocking_download_round_trip_in_async_std() {
        assert_blocking_download_round_trip(async_std::task::block_on);
    }

    // ── RuntimeMode checks ────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // download_file_to_path_blocking returns WrongRuntimeMode on an External-mode session.
    async fn test_download_file_to_path_blocking_errors_in_external_mode() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.inner.runtime.mode(), RuntimeMode::External);
        let group = session.new_file_download_group().unwrap().build().await.unwrap();
        let file_info = XetFileInfo {
            hash: String::new(),
            file_size: Some(0),
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
    // Owned-mode session: bridge_sync uses handle.block_on(), which panics
    // because tokio sets a thread-local runtime context that it detects and rejects.
    fn test_download_file_to_path_blocking_panics_in_async_context() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.inner.runtime.mode(), RuntimeMode::Owned);
        let group = session.new_file_download_group().unwrap().build_blocking().unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let file_info = XetFileInfo {
            hash: String::new(),
            file_size: Some(0),
            sha256: None,
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async { group.download_file_to_path_blocking(file_info, PathBuf::from("/nonexistent")) })
        }));
        assert!(result.is_err(), "download_file_to_path_blocking() must panic when called from async");
    }
}
