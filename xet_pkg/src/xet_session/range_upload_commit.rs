//! XetRangeUploadCommit — group of edits to an existing file (dirty upload).
//!
//! This is the "dirty upload" layer: instead of uploading an entire file, you
//! specify which byte ranges have changed (dirty ranges) and provide the new
//! data.  The untouched regions are pulled from the original file in CAS.
//!
//! ```text
//! with session.new_range_upload(original_hash="abc...", original_size=10000) as commit:
//!     commit.edit(1000, 2000).write(b"new data")
//!     commit.append(500)
//! report = commit.commit()
//! ```

use std::ops::Range;
use std::sync::{Arc, Mutex};

use xet_core_structures::merklehash::MerkleHash;
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::{DirtyInput, FileUploadSession, XetFileInfo, create_remote_client};
use xet_data::progress_tracking::{GroupProgressReport, ItemProgressReport};
use xet_runtime::utils::UniqueId;

use super::auth_group_builder::{AuthGroupBuilder, AuthOptions};
use super::common::create_translator_config;
use super::range_upload_edit::XetRangeUploadEdit;
use super::session::XetSession;
use super::task_runtime::{TaskRuntime, XetTaskState};
use crate::error::XetError;

// ── Report ───────────────────────────────────────────────────────────────────

/// Report returned by [`XetRangeUploadCommit::commit`].
#[derive(Clone, Debug)]
#[cfg_attr(feature = "python", pyo3::pyclass(get_all, from_py_object))]
pub struct XetRangeUploadReport {
    /// Xet file information for the composed file: hash, size, and optional SHA-256.
    pub file_info: XetFileInfo,
}

// ── Builder ──────────────────────────────────────────────────────────────────

pub type XetRangeUploadCommitBuilder = AuthGroupBuilder<XetRangeUploadCommit>;

impl AuthGroupBuilder<XetRangeUploadCommit> {
    /// Create the [`XetRangeUploadCommit`] from an async context.
    pub async fn build(self, original_hash: String, original_size: u64) -> Result<XetRangeUploadCommit, XetError> {
        let AuthGroupBuilder {
            session, auth_options, ..
        } = self;
        let parent_runtime = session.inner.task_runtime.clone();
        let child_parent = parent_runtime.clone();
        let commit = parent_runtime
            .bridge_async("new_range_upload", async move {
                let commit_runtime = child_parent.child()?;
                XetRangeUploadCommit::new(session, commit_runtime, auth_options, original_hash, original_size).await
            })
            .await?;
        Ok(commit)
    }

    /// Create the [`XetRangeUploadCommit`] from a sync context.
    ///
    /// # Errors
    ///
    /// Returns [`XetError::WrongRuntimeMode`] if the session wraps an external
    /// tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime on an Owned-mode session.
    #[cfg(not(target_family = "wasm"))]
    pub fn build_blocking(self, original_hash: String, original_size: u64) -> Result<XetRangeUploadCommit, XetError> {
        let AuthGroupBuilder {
            session, auth_options, ..
        } = self;
        let parent_runtime = session.inner.task_runtime.clone();
        let child_parent = parent_runtime.clone();
        let commit = parent_runtime.bridge_sync("new_range_upload_blocking", async move {
            let commit_runtime = child_parent.child()?;
            XetRangeUploadCommit::new(session, commit_runtime, auth_options, original_hash, original_size).await
        })?;
        Ok(commit)
    }
}

// ── XetRangeUploadCommit (public wrapper) ────────────────────────────────────

/// API for editing an existing file by uploading only changed byte ranges.
///
/// Obtain via [`XetSession::new_range_upload`] — configure auth on the returned
/// [`AuthGroupBuilder`], then call [`build`](AuthGroupBuilder::build) (async) or
/// [`build_blocking`](AuthGroupBuilder::build_blocking) (sync).
///
/// Queue edits with [`edit`](Self::edit), [`insert`](Self::insert), [`delete`](Self::delete),
/// or [`append`](Self::append), then call
/// [`commit`](Self::commit) (async) or [`commit_blocking`](Self::commit_blocking) (sync).
///
/// This type is cheaply clonable; all clones share the same underlying state.
///
/// # Errors
///
/// Returns [`XetError::UserCancelled`] if the parent session has been aborted.
#[derive(Clone)]
pub struct XetRangeUploadCommit {
    pub(super) inner: Arc<XetRangeUploadCommitInner>,
    pub(super) task_runtime: Arc<TaskRuntime>,
}

impl XetRangeUploadCommit {
    pub(super) async fn new(
        session: XetSession,
        task_runtime: Arc<TaskRuntime>,
        auth_options: AuthOptions,
        original_hash: String,
        original_size: u64,
    ) -> Result<Self, XetError> {
        // Validate auth by creating the translator config (this resolves the endpoint
        // and token early, failing fast if auth is invalid).
        let config = Arc::new(create_translator_config(&session, auth_options).await?);
        let client = create_remote_client(&config, &session.inner.id.to_string(), false).await?;

        // Create upload session for progress tracking
        let upload_session = FileUploadSession::new(Arc::clone(&config)).await?;

        let commit_id = UniqueId::new();
        let inner = Arc::new(XetRangeUploadCommitInner {
            commit_id,
            config,
            client,
            original_hash,
            original_size,
            pending_edits: Mutex::new(Vec::new()),
            upload_session: Arc::new(std::sync::Mutex::new(Some(upload_session))),
        });

        Ok(Self { inner, task_runtime })
    }

    /// Unique identifier for this commit.
    pub fn id(&self) -> UniqueId {
        self.inner.commit_id
    }

    /// Status of this commit.
    pub fn status(&self) -> Result<XetTaskState, XetError> {
        self.task_runtime.status()
    }

    /// Start a new edit: replace `original_range` with `new_length` bytes.
    ///
    /// Returns an [`XetRangeUploadEdit`] handle.  Feed data incrementally with
    /// [`write`](XetRangeUploadEdit::write), then call
    /// [`finish`](XetRangeUploadEdit::finish) **before** calling [`commit`].
    pub fn edit(&self, original_range: Range<u64>, new_length: u64) -> XetRangeUploadEdit {
        let edit = XetRangeUploadEdit::new(original_range, new_length);
        self.inner.pending_edits.lock().unwrap().push(edit.clone());
        edit
    }

    /// Convenience: insert `new_length` bytes at position `pos`.
    ///
    /// Equivalent to `edit(pos..pos, new_length)`.
    pub fn insert(&self, pos: u64, new_length: u64) -> XetRangeUploadEdit {
        self.edit(pos..pos, new_length)
    }

    /// Convenience: delete bytes at `start..end`.
    ///
    /// Equivalent to `edit(start..end, 0)`.
    pub fn delete(&self, start: u64, end: u64) -> XetRangeUploadEdit {
        self.edit(start..end, 0)
    }

    /// Convenience: append `new_length` bytes at the end of the file.
    ///
    /// Equivalent to `edit(original_size..original_size, new_length)`.
    pub fn append(&self, new_length: u64) -> XetRangeUploadEdit {
        let original_size = self.inner.original_size;
        self.edit(original_size..original_size, new_length)
    }

    /// Wait for all edits to be committed and return the result.
    pub async fn commit(&self) -> Result<XetRangeUploadReport, XetError> {
        let inner = Arc::clone(&self.inner);
        self.task_runtime
            .bridge_async_finalizing("range_upload_commit", false, async move { inner.commit().await })
            .await
    }

    /// Blocking version of [`commit`](Self::commit).
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    #[cfg(not(target_family = "wasm"))]
    pub fn commit_blocking(&self) -> Result<XetRangeUploadReport, XetError> {
        let inner = Arc::clone(&self.inner);
        self.task_runtime.bridge_sync_finalizing(
            "range_upload_commit_blocking",
            false,
            async move { inner.commit().await },
        )
    }

    /// Cancel all pending edits.
    pub fn abort(&self) -> Result<(), XetError> {
        let mut pending = self.inner.pending_edits.lock().unwrap();
        pending.clear();
        self.task_runtime.cancel_subtree()?;
        Ok(())
    }

    /// Aggregate progress for this commit.
    pub fn progress(&self) -> GroupProgressReport {
        self.inner
            .upload_session
            .lock()
            .unwrap()
            .as_ref()
            .map(|s| s.report())
            .unwrap_or_default()
    }

    /// Get item reports from the upload session.
    pub fn item_reports_from_upload_session(&self) -> std::collections::HashMap<UniqueId, ItemProgressReport> {
        self.inner
            .upload_session
            .lock()
            .unwrap()
            .as_ref()
            .map(|s| s.item_reports())
            .unwrap_or_default()
    }
}

// ── XetRangeUploadCommitInner ───────────────────────────────────────────────

pub(crate) struct XetRangeUploadCommitInner {
    commit_id: UniqueId,
    /// Translator config with endpoint, auth, etc. (wrapped in Arc for sharing).
    config: Arc<TranslatorConfig>,
    /// CAS client for fetching original file segments.
    client: Arc<dyn xet_client::cas_client::Client>,
    original_hash: String,
    original_size: u64,
    /// Pending edit handles that will be consumed by commit.
    pending_edits: Mutex<Vec<XetRangeUploadEdit>>,
    /// Upload session for progress tracking (created lazily on first edit).
    upload_session: Arc<std::sync::Mutex<Option<Arc<FileUploadSession>>>>,
}

impl XetRangeUploadCommitInner {
    /// Finalise all pending edits and execute the range upload.
    async fn commit(self: &Arc<Self>) -> Result<XetRangeUploadReport, XetError> {
        // Finalise each edit and collect DirtyInputs.  All edits use **original-file**
        // coordinates and must be non-overlapping.  The caller is responsible for
        // merging any overlapping operations before calling commit().
        let dirty_inputs: Vec<DirtyInput> = {
            let mut pending = self.pending_edits.lock().unwrap();
            let mut inputs = Vec::new();
            for edit in pending.drain(..) {
                let edit_arc = Arc::new(edit);
                let dirty = edit_arc
                    .finish()
                    .map_err(|_| XetError::other("edit was already finished before commit"))?;
                inputs.push(dirty);
            }
            inputs
        };

        tracing::debug!("Committing range upload with {} edits", dirty_inputs.len());

        // Convert the original hash string to MerkleHash.
        let original_hash = MerkleHash::from_hex(&self.original_hash)
            .map_err(|e| XetError::other(format!("invalid original_hash: {e}")))?;

        // Run upload_ranges with the config and client.
        let upload_session_for_ranges = {
            let guard = self.upload_session.lock().unwrap();
            guard.as_ref().cloned()
        };

        let file_info = xet_data::processing::upload_ranges(
            Arc::clone(&self.config),
            Arc::clone(&self.client),
            original_hash,
            self.original_size,
            dirty_inputs,
            upload_session_for_ranges,
        )
        .await?;

        Ok(XetRangeUploadReport { file_info })
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use http::HeaderMap;
    use tempfile::tempdir;

    use super::*;
    use crate::xet_session::Sha256Policy;
    use crate::xet_session::session::XetSessionBuilder;

    // Helper: read the HF Hub token from the default cache path.
    fn read_hf_token() -> String {
        let token_path = dirs::home_dir()
            .map(|d| d.join(".cache/huggingface/token"))
            .expect("could not resolve home dir");
        std::fs::read_to_string(token_path)
            .expect("failed to read HF token")
            .trim()
            .to_string()
    }

    fn upload_file(session: &XetSession, endpoint: &str, data: &[u8], name: &str) -> XetFileInfo {
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(endpoint)
            .build_blocking()
            .unwrap();
        let _handle = commit
            .upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some(name.into()))
            .unwrap();
        let results = commit.commit_blocking().unwrap();
        let meta = results.uploads.into_values().next().expect("one uploaded file");
        meta.xet_info.clone()
    }

    #[test]
    fn test_range_upload_edit_basic() {
        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();

        // Upload an original file (13 bytes: "Hello, World!")
        let original_data = b"Hello, World!";
        let original_info = upload_file(&session, &endpoint, original_data, "original.bin");

        // Create a range upload commit
        let commit = session
            .new_range_upload()
            .unwrap()
            .with_endpoint(&endpoint)
            .build_blocking(original_info.hash, original_info.file_size.unwrap())
            .unwrap();

        // Edit: replace bytes 7..12 (5 bytes: ", Wor") with new data (8 bytes: "Universe")
        // Expected file size: 13 - 5 + 8 = 16
        let edit = commit.edit(7..12, 8);
        edit.write(b"Universe");

        // Commit
        let report = commit.commit_blocking().unwrap();
        assert_eq!(report.file_info.file_size, Some(16));
    }

    #[test]
    fn test_range_upload_insert() {
        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();

        let original_data = b"Hello World!";
        let original_info = upload_file(&session, &endpoint, original_data, "original.bin");

        let commit = session
            .new_range_upload()
            .unwrap()
            .with_endpoint(&endpoint)
            .build_blocking(original_info.hash, original_info.file_size.unwrap())
            .unwrap();

        // Insert 7 bytes at position 5 (empty original range, so new_length = 7)
        let edit = commit.insert(5, 7);
        edit.write(b" Beautiful");

        let report = commit.commit_blocking().unwrap();
        // Original 12 bytes + 7 inserted = 19 bytes
        assert_eq!(report.file_info.file_size, Some(19));
    }

    #[test]
    fn test_range_upload_delete() {
        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();

        let original_data = b"Hello, World!";
        let original_info = upload_file(&session, &endpoint, original_data, "original.bin");

        let commit = session
            .new_range_upload()
            .unwrap()
            .with_endpoint(&endpoint)
            .build_blocking(original_info.hash, original_info.file_size.unwrap())
            .unwrap();

        // Delete bytes 7..12 (5 bytes: ", Wor")
        // 13 - 5 = 8 bytes
        let _edit = commit.delete(7, 12);

        let report = commit.commit_blocking().unwrap();
        assert_eq!(report.file_info.file_size, Some(8));
    }

    #[test]
    fn test_range_upload_append() {
        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();

        let original_data = b"Hello";
        let original_info = upload_file(&session, &endpoint, original_data, "original.bin");

        let commit = session
            .new_range_upload()
            .unwrap()
            .with_endpoint(&endpoint)
            .build_blocking(original_info.hash, original_info.file_size.unwrap())
            .unwrap();

        // Append 6 bytes
        let edit = commit.append(6);
        edit.write(b" World");

        let report = commit.commit_blocking().unwrap();
        assert_eq!(report.file_info.file_size, Some(11));
    }

    #[test]
    fn test_range_upload_multiple_edits() {
        let temp = tempdir().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let session = XetSessionBuilder::new().build().unwrap();

        let original_data = b"Hello, World!";
        let original_info = upload_file(&session, &endpoint, original_data, "original.bin");

        let commit = session
            .new_range_upload()
            .unwrap()
            .with_endpoint(&endpoint)
            .build_blocking(original_info.hash, original_info.file_size.unwrap())
            .unwrap();

        // Multiple edits
        // 7..12 (5 bytes) -> 8 bytes (Universe) => +3
        // 12..12 (0 bytes) -> 1 byte (!) => +1
        // Total: 13 + 3 + 1 = 17
        commit.edit(7..12, 8).write(b"Universe");
        commit.edit(12..12, 1).write(b"!");

        let report = commit.commit_blocking().unwrap();
        assert_eq!(report.file_info.file_size, Some(17));
    }

    // ── E2E tests against HuggingFace Hub ─────────────────────────────────────

    /// Helper: upload a single file to the HF Hub repo.
    fn upload_to_hub(session: &XetSession, data: &[u8], name: &str) -> XetFileInfo {
        let token = read_hf_token();
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            http::header::HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
        );
        let refresh_url = "https://huggingface.co/api/buckets/lhoestq/range-upload-test/xet-write-token".to_string();

        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_token_refresh_url(refresh_url, headers)
            .build_blocking()
            .unwrap();

        let _handle = commit
            .upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some(name.into()))
            .unwrap();
        let results = commit.commit_blocking().unwrap();
        let meta = results.uploads.into_values().next().expect("one uploaded file");
        meta.xet_info.clone()
    }

    fn make_auth_headers() -> HeaderMap {
        let token = read_hf_token();
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            http::header::HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
        );
        headers
    }

    fn make_write_refresh_url() -> String {
        "https://huggingface.co/api/buckets/lhoestq/range-upload-test/xet-write-token".to_string()
    }

    fn make_read_refresh_url() -> String {
        "https://huggingface.co/api/buckets/lhoestq/range-upload-test/xet-read-token".to_string()
    }

    #[test]
    fn test_e2e_range_upload_hub() {
        futures::executor::block_on(async {
            let session = XetSessionBuilder::new().build().unwrap();

            // ── Step 1: Create and upload an original file to HF Hub ──────────────
            let original_data = b"Hello, World! This is a test file for range upload.";
            let original_info = upload_to_hub(&session, original_data, "original.txt");
            println!("Original: hash={}, size={}", original_info.hash, original_info.file_size.unwrap());

            // Verify the hash matches
            use sha2::{Digest, Sha256};
            let hash_bytes: Vec<u8> = Sha256::digest(original_data).to_vec();
            let expected_sha: String = hash_bytes.iter().map(|b| format!("{:02x}", b)).collect();
            assert_eq!(original_info.sha256.as_deref(), Some(expected_sha.as_str()));

            // ── Step 2: Download the original file to verify contents ──────────────
            let dl_session = XetSessionBuilder::new().build().unwrap();

            let group = dl_session
                .new_file_download_group()
                .unwrap()
                .with_token_refresh_url(make_read_refresh_url(), make_auth_headers())
                .build_blocking()
                .unwrap();

            let dest_path = tempfile::tempdir().unwrap().path().join("downloaded.txt");
            let _handle = group
                .download_file_to_path_blocking(original_info.clone(), dest_path.clone())
                .unwrap();

            let report = group.finish_blocking().unwrap();
            assert_eq!(report.downloads.len(), 1);

            let downloaded_data = std::fs::read(&dest_path).unwrap();
            assert_eq!(downloaded_data, original_data);

            // ── Step 3: Perform a range upload (edit) ─────────────────────────────
            let edit_data = b"Universe! ";
            let write_headers = make_auth_headers();

            let commit = session
                .new_range_upload()
                .unwrap()
                .with_token_refresh_url(make_write_refresh_url(), write_headers)
                .build_blocking(original_info.hash, original_info.file_size.unwrap())
                .unwrap();

            // Edit: replace bytes 0..13 ("Hello, World!") with "Universe! " (10 bytes)
            // Original: 51 bytes ("Hello, World! This is a test file for range upload.")
            // After: 51 - 13 + 10 = 48 bytes

            let edit = commit.edit(0..13, 10);
            edit.write(edit_data);

            let report = commit.commit_blocking().unwrap();
            assert_eq!(report.file_info.file_size, Some(48));

            // ── Step 4: Download the modified file and verify ──────────────────────
            let dl_session2 = XetSessionBuilder::new().build().unwrap();
            let dl_headers2 = make_auth_headers();

            let group2 = dl_session2
                .new_file_download_group()
                .unwrap()
                .with_token_refresh_url(make_read_refresh_url(), dl_headers2)
                .build_blocking()
                .unwrap();

            let dest_path2 = tempfile::tempdir().unwrap().path().join("downloaded2.txt");
            let _handle2 = group2
                .download_file_to_path_blocking(report.file_info.clone(), dest_path2.clone())
                .unwrap();

            let report2 = group2.finish_blocking().unwrap();
            assert_eq!(report2.downloads.len(), 1);

            let modified_data = std::fs::read(&dest_path2).unwrap();
            let expected_modified = b"Universe!  This is a test file for range upload.";
            assert_eq!(expected_modified.len(), 48);
            assert_eq!(modified_data, expected_modified);
        });
    }

    #[test]
    fn test_e2e_range_upload_insert_hub() {
        futures::executor::block_on(async {
            let session = XetSessionBuilder::new().build().unwrap();

            // Upload original
            let original_data = b"ABCDEF";
            let original_info = upload_to_hub(&session, original_data, "insert_test.txt");

            // Insert 3 bytes at position 2: "XYZ"
            // Result: "ABXYZCDEF" (9 bytes)
            let write_headers = make_auth_headers();

            let commit = session
                .new_range_upload()
                .unwrap()
                .with_token_refresh_url(make_write_refresh_url(), write_headers)
                .build_blocking(original_info.hash, original_info.file_size.unwrap())
                .unwrap();

            commit.insert(2, 3).write(b"XYZ");

            let report = commit.commit_blocking().unwrap();
            assert_eq!(report.file_info.file_size, Some(9));

            // Verify by downloading
            let dl_session = XetSessionBuilder::new().build().unwrap();
            let dl_headers = make_auth_headers();

            let group = dl_session
                .new_file_download_group()
                .unwrap()
                .with_token_refresh_url(make_read_refresh_url(), dl_headers)
                .build_blocking()
                .unwrap();

            let dest = tempfile::tempdir().unwrap().path().join("inserted.txt");
            group
                .download_file_to_path_blocking(report.file_info.clone(), dest.clone())
                .unwrap();

            group.finish_blocking().unwrap();

            let data = std::fs::read(&dest).unwrap();
            assert_eq!(data, b"ABXYZCDEF");
        });
    }

    #[test]
    fn test_e2e_range_upload_delete_hub() {
        futures::executor::block_on(async {
            let session = XetSessionBuilder::new().build().unwrap();

            let original_data = b"Hello, World!";
            let original_info = upload_to_hub(&session, original_data, "delete_test.txt");

            // Delete bytes 5..12 (", World") => 7 bytes removed
            // Result: "Hello!" (6 bytes)
            let write_headers = make_auth_headers();

            let commit = session
                .new_range_upload()
                .unwrap()
                .with_token_refresh_url(make_write_refresh_url(), write_headers)
                .build_blocking(original_info.hash, original_info.file_size.unwrap())
                .unwrap();

            commit.delete(5, 12);

            let report = commit.commit_blocking().unwrap();
            assert_eq!(report.file_info.file_size, Some(6));

            // Verify
            let dl_session = XetSessionBuilder::new().build().unwrap();
            let dl_headers = make_auth_headers();

            let group = dl_session
                .new_file_download_group()
                .unwrap()
                .with_token_refresh_url(make_read_refresh_url(), dl_headers)
                .build_blocking()
                .unwrap();

            let dest = tempfile::tempdir().unwrap().path().join("deleted.txt");
            group
                .download_file_to_path_blocking(report.file_info.clone(), dest.clone())
                .unwrap();

            group.finish_blocking().unwrap();

            let data = std::fs::read(&dest).unwrap();
            assert_eq!(data, b"Hello!");
        });
    }
}
