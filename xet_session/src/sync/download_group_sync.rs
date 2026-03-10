//! Sync-context download group wrapper.
//!
//! [`DownloadGroupSync`] is obtained from [`XetSession::new_download_group_blocking`] and
//! provides a fully blocking API suitable for sync Rust or Python (PyO3) callers.
//! For async Rust use [`DownloadGroup`] instead.

use std::collections::HashMap;
use std::path::PathBuf;

use data::XetFileInfo;
use ulid::Ulid;

use crate::download_group::{DownloadGroup, DownloadResult};
use crate::errors::SessionError;
use crate::progress::{DownloadTaskHandle, ProgressSnapshot};
use crate::session::XetSession;

/// Sync-context handle for grouping related file downloads.
///
/// Obtained from [`XetSession::new_download_group_blocking`]. All methods block
/// the calling thread — **do not use from within an async runtime** (it will panic).
/// For async Rust code use [`DownloadGroup`] from [`XetSession::new_download_group`].
///
/// # Cloning
///
/// Cloning is cheap — it simply increments an atomic reference count.
/// All clones share the same background worker and task state.
#[derive(Clone)]
pub struct DownloadGroupSync {
    pub(crate) inner: DownloadGroup,
}

impl DownloadGroupSync {
    /// Create a new download group from a **sync** (non-async) context.
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime — use
    /// [`XetSession::new_download_group`] instead.
    pub(crate) fn new(session: XetSession) -> Result<Self, SessionError> {
        let runtime = session.runtime.clone();
        let group = runtime.external_run_async_task(DownloadGroup::init(session.clone()))??;
        Ok(Self { inner: group })
    }

    /// Queue a file for download. See [`DownloadGroup::download_file_to_path`] for full documentation.
    pub fn download_file_to_path(
        &self,
        file_info: XetFileInfo,
        dest_path: PathBuf,
    ) -> Result<DownloadTaskHandle, SessionError> {
        self.inner.download_file_to_path(file_info, dest_path)
    }

    /// Return a snapshot of progress for every queued download.
    pub fn get_progress(&self) -> Result<ProgressSnapshot, SessionError> {
        self.inner.get_progress()
    }

    /// Wait for all downloads to complete and return their results.
    ///
    /// See [`DownloadGroup::finish`] for full documentation.
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime. Use [`DownloadGroup::finish`] instead.
    pub fn finish(self) -> Result<HashMap<Ulid, DownloadResult>, SessionError> {
        let group = self.inner.clone();
        self.inner
            .runtime()
            .external_run_async_task(async move { group.handle_finish().await })?
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::{TempDir, tempdir};

    use super::*;
    use crate::progress::UploadTaskHandle;
    use crate::session::{XetSession, XetSessionBuilder};

    fn local_session(temp: &TempDir) -> Result<XetSession, Box<dyn std::error::Error>> {
        let cas_path = temp.path().join("cas");
        Ok(XetSession::new(Some(format!("local://{}", cas_path.display())), None, None, None)?)
    }

    fn upload_bytes(session: &XetSession, data: &[u8], name: &str) -> Result<XetFileInfo, Box<dyn std::error::Error>> {
        let commit = session.new_upload_commit_blocking()?;
        let handle = commit.upload_bytes(data.to_vec(), Some(name.into()))?;
        let results = commit.commit()?;
        let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
        Ok(XetFileInfo {
            hash: meta.hash.clone(),
            file_size: meta.file_size,
        })
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
        let group = session.new_download_group_blocking()?;
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

        let commit = session.new_upload_commit_blocking()?;
        let handle_a = commit.upload_bytes(data_a.to_vec(), Some("a.bin".into()))?;
        let handle_b = commit.upload_bytes(data_b.to_vec(), Some("b.bin".into()))?;
        let results = commit.commit()?;

        let to_file_info = |handle: &UploadTaskHandle| -> XetFileInfo {
            let meta = results.get(&handle.task_id).unwrap().as_ref().as_ref().unwrap();
            XetFileInfo {
                hash: meta.hash.clone(),
                file_size: meta.file_size,
            }
        };

        let dest_a = temp.path().join("a_out.bin");
        let dest_b = temp.path().join("b_out.bin");
        let group = session.new_download_group_blocking()?;
        group.download_file_to_path(to_file_info(&handle_a), dest_a.clone())?;
        group.download_file_to_path(to_file_info(&handle_b), dest_b.clone())?;
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
        let group = session.new_download_group_blocking()?;
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

    // ── Per-task result access patterns ──────────────────────────────────────

    #[test]
    // Pattern 1: per-task result is accessible via task_id in the finish() HashMap.
    fn test_download_result_accessible_via_task_id_in_finish_map() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let data = b"result via task_id in finish map";
        let file_info = upload_bytes(&session, data, "file.bin")?;
        let dest = temp.path().join("out.bin");
        let group = session.new_download_group_blocking()?;
        let handle = group.download_file_to_path(file_info, dest)?;
        let results = group.finish()?;
        let result = results.get(&handle.task_id).expect("task_id must be present in results");
        assert_eq!(result.as_ref().as_ref().unwrap().file_info.file_size, data.len() as u64);
        Ok(())
    }

    #[test]
    // DownloadTaskHandle::result() returns None before finish() is called.
    fn test_download_result_none_before_finish() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let file_info = upload_bytes(&session, b"some data", "file.bin")?;
        let dest = temp.path().join("out.bin");
        let group = session.new_download_group_blocking()?;
        let handle = group.download_file_to_path(file_info, dest)?;
        assert!(handle.result().is_none(), "result must be None before finish()");
        group.finish()?;
        Ok(())
    }

    #[test]
    // DownloadTaskHandle::result() returns Some after finish() completes.
    fn test_download_result_some_after_finish() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let data = b"download result test data";
        let file_info = upload_bytes(&session, data, "file.bin")?;
        let dest = temp.path().join("out.bin");
        let group = session.new_download_group_blocking()?;
        let handle = group.download_file_to_path(file_info.clone(), dest)?;
        group.finish()?;
        let result = handle.result().expect("result must be set after finish()");
        let dl = result.as_ref().as_ref().unwrap();
        assert_eq!(dl.file_info.file_size, data.len() as u64);
        assert_eq!(dl.file_info.hash, file_info.hash);
        Ok(())
    }

    // ── Async-context panic guard ─────────────────────────────────────────────

    #[tokio::test]
    // new_download_group_blocking panics when called from inside a tokio runtime.
    async fn test_new_download_group_blocking_panics_in_async_context() {
        let session = XetSessionBuilder::new().build().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = session.new_download_group_blocking();
        }));
        assert!(result.is_err(), "expected panic from _blocking inside async");
    }
}
