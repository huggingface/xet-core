//! Sync-context upload commit wrapper.
//!
//! [`UploadCommitSync`] is obtained from [`XetSession::new_upload_commit_blocking`] and
//! provides a fully blocking API suitable for sync Rust or Python (PyO3) callers.
//! For async Rust use [`UploadCommit`] instead.

use std::collections::HashMap;
use std::path::PathBuf;

use ulid::Ulid;
use xet_data::processing::SingleFileCleaner;

use super::super::errors::SessionError;
use super::super::progress::{ProgressSnapshot, TaskHandle, UploadTaskHandle};
use super::super::session::XetSession;
use super::super::upload_commit::{UploadCommit, UploadResult};

/// Sync-context handle for grouping related file uploads.
///
/// Obtained from [`XetSession::new_upload_commit_blocking`]. All methods block
/// the calling thread — **do not use from within an async runtime** (it will panic).
/// For async Rust code use [`UploadCommit`] from [`XetSession::new_upload_commit`].
///
/// # Cloning
///
/// Cloning is cheap — it simply increments an atomic reference count.
/// All clones share the same upload session and task state.
#[derive(Clone)]
pub struct UploadCommitSync {
    pub(crate) inner: UploadCommit,
}

impl UploadCommitSync {
    /// Create a new upload commit from a **sync** (non-async) context.
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime — use
    /// [`XetSession::new_upload_commit`] instead.
    pub(crate) fn new(session: XetSession) -> Result<Self, SessionError> {
        let runtime = session.runtime.clone();
        let commit = runtime.external_run_async_task(UploadCommit::new(session.clone()))??;
        Ok(Self { inner: commit })
    }

    /// Queue a file for upload. See [`UploadCommit::upload_from_path`] for full documentation.
    pub fn upload_from_path(&self, file_path: PathBuf) -> Result<UploadTaskHandle, SessionError> {
        let commit = self.inner.clone();
        self.inner
            .runtime()
            .external_run_async_task(async move { commit.upload_from_path(file_path).await })?
    }

    /// Queue raw bytes for upload. See [`UploadCommit::upload_bytes`] for full documentation.
    pub fn upload_bytes(
        &self,
        bytes: Vec<u8>,
        tracking_name: Option<String>,
    ) -> Result<UploadTaskHandle, SessionError> {
        let commit = self.inner.clone();
        self.inner
            .runtime()
            .external_run_async_task(async move { commit.upload_bytes(bytes, tracking_name).await })?
    }

    /// Begin an incremental file upload, returning a [`SingleFileCleaner`] to stream bytes.
    ///
    /// This is the sync-context equivalent of [`UploadCommit::upload_file`].
    ///
    /// # Parameters
    ///
    /// - `file_name`: optional name used for progress/telemetry reporting.
    /// - `file_size`: expected size in bytes (used for progress tracking; `0` is valid if unknown).
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime. Use [`UploadCommit::upload_file`] instead.
    pub fn upload_file(
        &self,
        file_name: Option<String>,
        file_size: u64,
    ) -> Result<(TaskHandle, SingleFileCleaner), SessionError> {
        let commit = self.inner.clone();
        self.inner
            .runtime()
            .external_run_async_task(async move { commit.upload_file(file_name, file_size).await })?
    }

    /// Return a snapshot of progress for every queued upload.
    pub fn get_progress(&self) -> Result<ProgressSnapshot, SessionError> {
        self.inner.get_progress()
    }

    /// Wait for all uploads to complete and push metadata to the CAS server.
    ///
    /// Returns a `HashMap` keyed by task ID. See [`UploadCommit::commit`] for full documentation.
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime. Use [`UploadCommit::commit`] instead.
    pub fn commit(self) -> Result<HashMap<Ulid, UploadResult>, SessionError> {
        let commit = self.inner.clone();
        self.inner
            .runtime()
            .external_run_async_task(async move { commit.handle_commit().await })?
    }
}

#[cfg(test)]
mod tests {
    use tempfile::{TempDir, tempdir};

    use crate::xet_session::session::{XetSession, XetSessionBuilder};

    fn local_session(temp: &TempDir) -> Result<XetSession, Box<dyn std::error::Error>> {
        let cas_path = temp.path().join("cas");
        Ok(XetSession::new(Some(format!("local://{}", cas_path.display())), None, None, None)?)
    }

    // ── Round-trip tests ─────────────────────────────────────────────────────

    #[test]
    // Uploading raw bytes and committing returns a non-empty hash and the correct file size.
    fn test_upload_bytes_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let data = b"Hello, upload commit round-trip!";
        let commit = session.new_upload_commit_blocking()?;
        let task_handle = commit.upload_bytes(data.to_vec(), Some("hello.bin".into()))?;
        let results = commit.commit()?;
        assert_eq!(results.len(), 1);
        let meta = results.get(&task_handle.task_id).unwrap().as_ref().as_ref().unwrap();
        assert_eq!(meta.file_size, data.len() as u64);
        assert!(!meta.hash.is_empty());
        Ok(())
    }

    #[test]
    // Uploading a file from disk and committing returns the correct file size.
    fn test_upload_from_path_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let src = temp.path().join("data.bin");
        let data = b"file path upload content";
        std::fs::write(&src, data)?;
        let commit = session.new_upload_commit_blocking()?;
        let handle = commit.upload_from_path(src)?;
        commit.commit()?;
        let meta = handle.result().unwrap();
        let meta = meta.as_ref().as_ref().unwrap();
        assert_eq!(meta.file_size, data.len() as u64);
        assert!(!meta.hash.is_empty());
        Ok(())
    }

    // ── Per-task result access patterns ──────────────────────────────────────

    #[test]
    // UploadTaskHandle::result() returns None before commit() is called.
    fn test_upload_result_none_before_commit() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let src = temp.path().join("data.bin");
        std::fs::write(&src, b"content")?;
        let commit = session.new_upload_commit_blocking()?;
        let handle = commit.upload_from_path(src)?;
        assert!(handle.result().is_none(), "result must be None before commit()");
        commit.commit()?;
        Ok(())
    }

    #[test]
    // Pattern 1: per-task result is accessible via task_id in the commit() HashMap.
    fn test_upload_result_accessible_via_task_id_in_commit_map() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let data = b"result via task_id";
        let src = temp.path().join("data.bin");
        std::fs::write(&src, data)?;
        let commit = session.new_upload_commit_blocking()?;
        let handle = commit.upload_from_path(src)?;
        let results = commit.commit()?;
        let result = results.get(&handle.task_id).expect("task_id must be present in results");
        assert_eq!(result.as_ref().as_ref().unwrap().file_size, data.len() as u64);
        Ok(())
    }

    #[test]
    // Pattern 2: per-task result is accessible directly from the UploadTaskHandle after commit().
    fn test_upload_result_accessible_via_handle_after_commit() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let data = b"result via handle";
        let src = temp.path().join("data.bin");
        std::fs::write(&src, data)?;
        let commit = session.new_upload_commit_blocking()?;
        let handle = commit.upload_from_path(src)?;
        commit.commit()?;
        let result = handle.result().expect("result must be set after commit");
        assert_eq!(result.as_ref().as_ref().unwrap().file_size, data.len() as u64);
        Ok(())
    }

    #[test]
    // Streaming upload via upload_file + SingleFileCleaner.
    fn test_upload_streaming_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let data = b"streamed upload bytes";
        let runtime = session.runtime.clone();
        let commit = session.new_upload_commit_blocking()?;
        let (_handle, mut cleaner) = commit.upload_file(Some("stream.bin".into()), data.len() as u64)?;
        let (hash, file_size) = runtime.external_run_async_task(async move {
            cleaner.add_data(data).await.unwrap();
            let (xfi, _) = cleaner.finish().await.unwrap();
            (xfi.hash, xfi.file_size)
        })?;
        let results = commit.commit()?;
        assert!(results.is_empty());
        assert_eq!(file_size, data.len() as u64);
        assert!(!hash.is_empty());
        Ok(())
    }

    #[test]
    // Uploading multiple blobs in one commit returns one result per upload.
    fn test_upload_multiple_files_in_one_commit() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let commit = session.new_upload_commit_blocking()?;
        commit.upload_bytes(b"file one".to_vec(), Some("a.bin".into()))?;
        commit.upload_bytes(b"file two".to_vec(), Some("b.bin".into()))?;
        commit.upload_bytes(b"file three".to_vec(), Some("c.bin".into()))?;
        let results = commit.commit()?;
        assert_eq!(results.len(), 3);
        Ok(())
    }

    #[test]
    // After a successful commit the aggregate progress reflects bytes processed.
    fn test_upload_progress_reflects_bytes_after_commit() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempdir()?;
        let session = local_session(&temp)?;
        let data = b"progress tracking upload data";
        let commit = session.new_upload_commit_blocking()?;
        let progress_observer = commit.clone();
        commit.upload_bytes(data.to_vec(), Some("prog.bin".into()))?;
        commit.commit()?;
        let snapshot = progress_observer.get_progress()?;
        assert!(snapshot.total().total_bytes_completed > 0);
        Ok(())
    }

    // ── Async-context panic guard ─────────────────────────────────────────────

    #[tokio::test]
    // new_upload_commit_blocking panics when called from inside a tokio runtime.
    async fn test_new_upload_commit_blocking_panics_in_async_context() {
        let session = XetSessionBuilder::new().build().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = session.new_upload_commit_blocking();
        }));
        assert!(result.is_err(), "expected panic from _blocking inside async");
    }
}
