//! XetSession - manages runtime and configuration

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use http::HeaderMap;
use tracing::info;
use ulid::Ulid;
use xet_data::progress_tracking::UniqueID;
use xet_runtime::config::XetConfig;
use xet_runtime::core::XetRuntime;

use super::download_stream_group::{DownloadStreamGroup, DownloadStreamGroupBuilder, DownloadStreamGroupInner};
use super::errors::SessionError;
use super::file_download_group::{FileDownloadGroup, FileDownloadGroupBuilder};
use super::upload_commit::{UploadCommit, UploadCommitBuilder};

/// Session state
enum SessionState {
    Alive,
    Aborted,
}

/// All shared state for a session.
/// Lives behind `Arc<XetSessionInner>` — do not use this type directly.
#[doc(hidden)]
pub struct XetSessionInner {
    // Independently cloned by background tasks, so needs its own Arc.
    pub(super) runtime: Arc<XetRuntime>,

    // Only accessed through &self; no independent cloning needed.
    pub(super) config: XetConfig,

    // CAS endpoint and shared HTTP settings (auth lives at the commit/group level)
    pub(super) endpoint: Option<String>,
    pub(super) custom_headers: Option<Arc<HeaderMap>>,

    // Track active upload commits and download groups.
    pub(super) active_upload_commits: Mutex<HashMap<UniqueID, UploadCommit>>,
    pub(super) active_file_download_groups: Mutex<HashMap<UniqueID, FileDownloadGroup>>,
    // Weak references so that dropping all user-held DownloadStreamGroup clones frees the group
    // immediately, without needing an explicit finalization call (unlike UploadCommit/FileDownloadGroup
    // which deregister on commit()/finish()). abort() upgrades live weak refs to cancel active streams.
    pub(super) active_download_stream_groups: Mutex<HashMap<UniqueID, Weak<DownloadStreamGroupInner>>>,

    // Session state
    state: Mutex<SessionState>,
    // "id" is used to identity a group of activities on our server, and so need to be globally unique
    pub(super) id: Ulid,
}

/// Builder for [`XetSession`].
///
/// All fields are optional; call [`build`](XetSessionBuilder::build) when done.
///
/// [`build`](Self::build) auto-detects a suitable current tokio handle when present,
/// or creates an owned runtime (see [`XetSessionBuilder::with_tokio_handle`]).
///
/// ## Authentication
///
/// Auth tokens are configured per-operation on [`UploadCommitBuilder`] and
/// [`FileDownloadGroupBuilder`], not on the session itself.  This lets uploads
/// and downloads use different access-level tokens from the same session:
///
/// ```rust,no_run
/// # use http::HeaderMap;
/// # use xet::xet_session::XetSessionBuilder;
/// let session = XetSessionBuilder::new().with_endpoint("https://cas.example.com").build()?;
///
/// // Upload token (write access)
/// let mut upload_headers = HeaderMap::new();
/// upload_headers.insert("Authorization", "Bearer write-token".parse().unwrap());
/// let commit = session
///     .new_upload_commit()?
///     .with_token_refresh_url("https://huggingface.co/api/repos/token/write", upload_headers)
///     .build_blocking()?;
///
/// // File download token (read access)
/// let mut dl_headers = HeaderMap::new();
/// dl_headers.insert("Authorization", "Bearer read-token".parse().unwrap());
/// let group = session
///     .new_file_download_group()?
///     .with_token_refresh_url("https://huggingface.co/api/repos/token/read", dl_headers)
///     .build_blocking()?;
///
/// // Streaming download token (read access, different group/pool)
/// let mut stream_headers = HeaderMap::new();
/// stream_headers.insert("Authorization", "Bearer read-token".parse().unwrap());
/// let stream_group = session
///     .new_download_stream_group()?
///     .with_token_refresh_url("https://huggingface.co/api/repos/token/read", stream_headers)
///     .build_blocking()?;
/// # Ok::<(), xet::xet_session::SessionError>(())
/// ```
pub struct XetSessionBuilder {
    config: XetConfig,
    endpoint: Option<String>,
    custom_headers: Option<Arc<HeaderMap>>,
    tokio_handle: Option<tokio::runtime::Handle>,
}

impl Default for XetSessionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl XetSessionBuilder {
    /// Create a builder with default [`XetConfig`].
    pub fn new() -> Self {
        Self {
            config: XetConfig::new(),
            endpoint: None,
            custom_headers: None,
            tokio_handle: None,
        }
    }

    /// Create a builder pre-populated with the given [`XetConfig`].
    pub fn new_with_config(config: XetConfig) -> Self {
        Self {
            config,
            endpoint: None,
            custom_headers: None,
            tokio_handle: None,
        }
    }

    /// Set the Xet CAS server endpoint URL (e.g. `"https://cas.example.com"`).
    pub fn with_endpoint(self, endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: Some(endpoint.into()),
            ..self
        }
    }

    /// Attach custom HTTP headers that are forwarded with every CAS request.
    pub fn with_custom_headers(self, headers: Arc<HeaderMap>) -> Self {
        Self {
            custom_headers: Some(headers),
            ..self
        }
    }

    /// Attach to an existing tokio runtime handle.
    ///
    /// If the handle meets runtime requirements (multi-thread flavor, time driver, IO driver),
    /// the session will wrap it — no second thread pool is created.
    /// [`new_upload_commit`](XetSession::new_upload_commit),
    /// [`new_file_download_group`](XetSession::new_file_download_group), and
    /// [`new_download_stream_group`](XetSession::new_download_stream_group) are always
    /// available (they are synchronous factory methods), but calling
    /// [`build_blocking`](crate::xet_session::UploadCommitBuilder::build_blocking),
    /// [`build_blocking`](crate::xet_session::FileDownloadGroupBuilder::build_blocking), or
    /// [`build_blocking`](crate::xet_session::DownloadStreamGroupBuilder::build_blocking) on
    /// the returned builder will return [`SessionError::WrongRuntimeMode`].
    ///
    /// If the handle does **not** meet requirements (e.g. `current_thread` flavor or missing
    /// drivers), it is silently ignored and [`build`](Self::build) will fall back to creating
    /// an owned thread pool instead.
    pub fn with_tokio_handle(self, handle: tokio::runtime::Handle) -> Self {
        let accept = XetRuntime::handle_meets_requirements(&handle);
        if !accept {
            info!("supplied tokio handle rejected (missing drivers or wrong flavor); falling back to Owned mode");
        }
        Self {
            tokio_handle: accept.then_some(handle),
            ..self
        }
    }

    /// Consume the builder and create a [`XetSession`].
    ///
    /// If a tokio runtime handle is available (either from
    /// [`with_tokio_handle`](Self::with_tokio_handle) or auto-detected via
    /// `Handle::try_current()`), and it meets requirements, the session wraps
    /// it — no second thread pool is created. Otherwise, an owned multi-thread
    /// runtime is created; async methods use an internal bridge and work from
    /// any executor, and `_blocking` methods are available.
    pub fn build(self) -> Result<XetSession, SessionError> {
        let handle = self.tokio_handle.or_else(|| {
            tokio::runtime::Handle::try_current().ok().filter(|h| {
                let ok = XetRuntime::handle_meets_requirements(h);
                if !ok {
                    info!(
                        "auto-detected tokio handle rejected (missing drivers or wrong flavor); creating Owned runtime"
                    );
                }
                ok
            })
        });

        let runtime = match handle {
            Some(h) => {
                info!("XetSession using External runtime (wrapping caller's tokio handle)");
                XetRuntime::from_external_with_config(h, self.config.clone())
            },
            None => {
                info!("XetSession creating Owned runtime (new thread pool)");
                XetRuntime::new_with_config(self.config.clone())?
            },
        };

        Ok(XetSession::new(self.config, self.endpoint, self.custom_headers, runtime))
    }
}

/// Handle for managing file uploads and downloads.
///
/// `XetSession` is the top-level entry point for the xet-session API.  It
/// owns a `XetRuntime` (tokio thread pool) and shared HTTP settings (endpoint,
/// custom headers).  Auth tokens are configured per-operation on the builder
/// types returned by each factory method, not on the session itself, so uploads,
/// file downloads, and streaming downloads can each carry a different access-level
/// token from the same session.
///
/// # Cloning
///
/// Cloning is cheap — it simply increments an atomic reference count.
/// All clones share the same underlying runtime and configuration.
///
/// # Lifecycle
///
/// 1. Create a session with [`XetSessionBuilder`].
/// 2. Create operations:
///    - uploads via [`new_upload_commit`](Self::new_upload_commit) → [`UploadCommitBuilder`] → [`UploadCommit`]
///    - file downloads via [`new_file_download_group`](Self::new_file_download_group) → [`FileDownloadGroupBuilder`] →
///      [`FileDownloadGroup`]
///    - streaming downloads via [`new_download_stream_group`](Self::new_download_stream_group) →
///      [`DownloadStreamGroupBuilder`] → [`DownloadStreamGroup`]
/// 3. For an emergency stop, call [`XetSession::abort`].
#[derive(Clone)]
pub struct XetSession {
    inner: Arc<XetSessionInner>,
}

impl std::ops::Deref for XetSession {
    type Target = XetSessionInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl XetSession {
    /// Low-level constructor used by [`XetSessionBuilder::build`].
    fn new(
        config: XetConfig,
        endpoint: Option<String>,
        custom_headers: Option<Arc<HeaderMap>>,
        runtime: Arc<XetRuntime>,
    ) -> Self {
        Self {
            inner: Arc::new(XetSessionInner {
                runtime,
                config,
                endpoint,
                custom_headers,
                active_upload_commits: Mutex::new(HashMap::new()),
                active_file_download_groups: Mutex::new(HashMap::new()),
                active_download_stream_groups: Mutex::new(HashMap::new()),
                state: Mutex::new(SessionState::Alive),
                id: Ulid::new(),
            }),
        }
    }

    /// Create an [`UploadCommitBuilder`] for configuring and constructing an upload commit.
    ///
    /// Configure per-commit auth with [`with_token_info`](UploadCommitBuilder::with_token_info)
    /// and [`with_token_refresh_url`](UploadCommitBuilder::with_token_refresh_url), then call
    /// [`build`](UploadCommitBuilder::build) (async) or
    /// [`build_blocking`](UploadCommitBuilder::build_blocking) (sync).
    ///
    /// Returns `Err(SessionError::Aborted)` if the session has been aborted.
    pub fn new_upload_commit(&self) -> Result<UploadCommitBuilder, SessionError> {
        if matches!(*self.state.lock()?, SessionState::Aborted) {
            return Err(SessionError::Aborted);
        }
        Ok(UploadCommitBuilder::new(self.clone()))
    }

    /// Create a [`FileDownloadGroupBuilder`] for configuring and constructing a download group.
    ///
    /// Configure per-group auth with [`with_token_info`](FileDownloadGroupBuilder::with_token_info)
    /// and [`with_token_refresh_url`](FileDownloadGroupBuilder::with_token_refresh_url), then call
    /// [`build`](FileDownloadGroupBuilder::build) (async) or
    /// [`build_blocking`](FileDownloadGroupBuilder::build_blocking) (sync).
    ///
    /// Returns `Err(SessionError::Aborted)` if the session has been aborted.
    pub fn new_file_download_group(&self) -> Result<FileDownloadGroupBuilder, SessionError> {
        if matches!(*self.state.lock()?, SessionState::Aborted) {
            return Err(SessionError::Aborted);
        }
        Ok(FileDownloadGroupBuilder::new(self.clone()))
    }

    /// Create a [`DownloadStreamGroupBuilder`] for configuring and constructing a download stream group.
    ///
    /// Configure per-group auth with [`with_token_info`](DownloadStreamGroupBuilder::with_token_info)
    /// and [`with_token_refresh_url`](DownloadStreamGroupBuilder::with_token_refresh_url), then call
    /// [`build`](DownloadStreamGroupBuilder::build) (async) or
    /// [`build_blocking`](DownloadStreamGroupBuilder::build_blocking) (sync).
    ///
    /// Use the resulting [`DownloadStreamGroup`] to create individual streams via
    /// [`download_stream`](DownloadStreamGroup::download_stream) and
    /// [`download_unordered_stream`](DownloadStreamGroup::download_unordered_stream).
    ///
    /// Returns `Err(SessionError::Aborted)` if the session has been aborted.
    pub fn new_download_stream_group(&self) -> Result<DownloadStreamGroupBuilder, SessionError> {
        if matches!(*self.state.lock()?, SessionState::Aborted) {
            return Err(SessionError::Aborted);
        }
        Ok(DownloadStreamGroupBuilder::new(self.clone()))
    }

    /// Abort the session - cancel all currently running tasks
    ///
    /// This performs a SIGINT-style shutdown, aborting all active upload and download tasks.
    /// Use this when a Ctrl+C signal is detected or when you need to immediately stop all operations.
    pub fn abort(&self) -> Result<(), SessionError> {
        // Mark as not accepting new work, hold the lock so no new task can be created when aborting
        let mut state = self.state.lock()?;
        *state = SessionState::Aborted;

        // Perform SIGINT shutdown on the runtime
        // This will cancel all active tasks (uploads, downloads, etc.)
        self.runtime.perform_sigint_shutdown();

        // Propagate states to registered tasks and clear registered work
        let active_upload_commits = std::mem::take(&mut *self.active_upload_commits.lock()?);
        for (_id, task) in active_upload_commits {
            task.abort()?;
        }
        let active_file_download_groups = std::mem::take(&mut *self.active_file_download_groups.lock()?);
        for (_id, task) in active_file_download_groups {
            task.abort()?;
        }
        let active_download_stream_groups = std::mem::take(&mut *self.active_download_stream_groups.lock()?);
        for (_id, weak_group) in active_download_stream_groups {
            if let Some(inner) = weak_group.upgrade() {
                DownloadStreamGroup { inner }.abort()?;
            }
        }
        Ok(())
    }

    pub(super) fn check_alive(&self) -> Result<(), SessionError> {
        if matches!(*self.state.lock()?, SessionState::Aborted) {
            return Err(SessionError::Aborted);
        }
        Ok(())
    }

    pub(super) fn finish_upload_commit(&self, commit_id: UniqueID) -> Result<(), SessionError> {
        self.active_upload_commits.lock()?.remove(&commit_id);
        Ok(())
    }

    pub(super) fn finish_file_download_group(&self, group_id: UniqueID) -> Result<(), SessionError> {
        self.active_file_download_groups.lock()?.remove(&group_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use xet_data::processing::XetFileInfo;
    use xet_runtime::core::{RuntimeMode, XetRuntime};

    use super::*;

    // ── Identity ─────────────────────────────────────────────────────────────

    #[test]
    // A clone refers to the same inner Arc, so their session IDs must match.
    fn test_session_clone_shares_state() {
        let s1 = XetSessionBuilder::new().build().unwrap();
        let s2 = s1.clone();
        assert_eq!(s1.id, s2.id);
    }

    #[test]
    // Two independently created sessions have distinct IDs.
    fn test_two_sessions_have_distinct_ids() {
        let s1 = XetSessionBuilder::new().build().unwrap();
        let s2 = XetSessionBuilder::new().build().unwrap();
        assert_ne!(s1.id, s2.id);
    }

    #[test]
    // Session ID is a Ulid, to guard future regressions.
    fn test_session_id_is_ulid() {
        let s = XetSessionBuilder::new().build().unwrap();
        assert!(Ulid::from_string(&s.id.to_string()).is_ok())
    }

    // ── Abort behavior ───────────────────────────────────────────────────────

    #[test]
    // After abort, check_alive returns Aborted.
    fn test_check_alive_after_abort() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.check_alive().unwrap_err();
        assert!(matches!(err, SessionError::Aborted));
    }

    #[test]
    // new_upload_commit on an aborted session returns Aborted.
    fn test_new_upload_commit_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.new_upload_commit().err().unwrap();
        assert!(matches!(err, SessionError::Aborted));
    }

    #[test]
    // new_file_download_group on an aborted session returns Aborted.
    fn test_new_file_download_group_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.new_file_download_group().err().unwrap();
        assert!(matches!(err, SessionError::Aborted));
    }

    #[test]
    // Aborting a session clears all registered upload commits.
    fn test_abort_clears_active_upload_commits() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _c1 = session.new_upload_commit().unwrap().build_blocking().unwrap();
        let _c2 = session.new_upload_commit().unwrap().build_blocking().unwrap();
        session.abort().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 0);
    }

    #[test]
    // Aborting a session clears all registered download groups.
    fn test_abort_clears_active_file_download_groups() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _g1 = session.new_file_download_group().unwrap().build_blocking().unwrap();
        session.abort().unwrap();
        assert_eq!(session.active_file_download_groups.lock().unwrap().len(), 0);
    }

    // ── Registration ─────────────────────────────────────────────────────────

    #[test]
    // A new upload commit is registered in the session's active set.
    fn test_new_upload_commit_registers_in_session() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _commit = session.new_upload_commit().unwrap().build_blocking().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
    }

    #[test]
    // A new download group is registered in the session's active set.
    fn test_new_file_download_group_registers_in_session() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _group = session.new_file_download_group().unwrap().build_blocking().unwrap();
        assert_eq!(session.active_file_download_groups.lock().unwrap().len(), 1);
    }

    // ── Deregistration ───────────────────────────────────────────────────────

    #[test]
    // finish_upload_commit removes only the specified commit, leaving others intact.
    fn test_finish_upload_commit_removes_only_that_commit() {
        let session = XetSessionBuilder::new().build().unwrap();
        let c1 = session.new_upload_commit().unwrap().build_blocking().unwrap();
        let _c2 = session.new_upload_commit().unwrap().build_blocking().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 2);
        session.finish_upload_commit(c1.id()).unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
    }

    #[test]
    // finish_file_download_group removes only the specified group, leaving others intact.
    fn test_finish_file_download_group_removes_only_that_group() {
        let session = XetSessionBuilder::new().build().unwrap();
        let g1 = session.new_file_download_group().unwrap().build_blocking().unwrap();
        let _g2 = session.new_file_download_group().unwrap().build_blocking().unwrap();
        assert_eq!(session.active_file_download_groups.lock().unwrap().len(), 2);
        session.finish_file_download_group(g1.id()).unwrap();
        assert_eq!(session.active_file_download_groups.lock().unwrap().len(), 1);
    }

    #[test]
    // finish_upload_commit on an unknown ID is a no-op (no error, no change).
    fn test_finish_upload_commit_with_unknown_id_is_noop() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _c1 = session.new_upload_commit().unwrap().build_blocking().unwrap();
        let unknown_id = UniqueID::new();
        assert!(session.finish_upload_commit(unknown_id).is_ok());
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
    }

    // ── Async abort behavior ──────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // new_upload_commit / new_file_download_group on an aborted session both return Aborted.
    async fn test_async_new_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let commit_err = session.new_upload_commit().err().unwrap();
        let group_err = session.new_file_download_group().err().unwrap();
        assert!(matches!(commit_err, SessionError::Aborted));
        assert!(matches!(group_err, SessionError::Aborted));
    }

    #[tokio::test(flavor = "multi_thread")]
    // Aborting a session clears all active upload commits and download groups.
    async fn test_async_abort_clears_active_commits_and_groups() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _c1 = session.new_upload_commit().unwrap().build().await.unwrap();
        let _c2 = session.new_upload_commit().unwrap().build().await.unwrap();
        let _g1 = session.new_file_download_group().unwrap().build().await.unwrap();
        session.abort().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 0);
        assert_eq!(session.active_file_download_groups.lock().unwrap().len(), 0);
    }

    // ── Async registration ────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // A new upload commit and a new download group are each registered in the
    // session's active set, and concurrent creation registers both.
    async fn test_async_new_registers_in_session() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _commit = session.new_upload_commit().unwrap().build().await.unwrap();
        let _group = session.new_file_download_group().unwrap().build().await.unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
        assert_eq!(session.active_file_download_groups.lock().unwrap().len(), 1);
    }

    // ── Async deregistration ──────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // Finishing one upload commit / download group removes only that one,
    // leaving the other still registered.
    async fn test_async_finish_removes_only_that_item() {
        let session = XetSessionBuilder::new().build().unwrap();
        let c1 = session.new_upload_commit().unwrap().build().await.unwrap();
        let _c2 = session.new_upload_commit().unwrap().build().await.unwrap();
        let g1 = session.new_file_download_group().unwrap().build().await.unwrap();
        let _g2 = session.new_file_download_group().unwrap().build().await.unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 2);
        assert_eq!(session.active_file_download_groups.lock().unwrap().len(), 2);
        session.finish_upload_commit(c1.id()).unwrap();
        session.finish_file_download_group(g1.id()).unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
        assert_eq!(session.active_file_download_groups.lock().unwrap().len(), 1);
    }

    // ── XetRuntime::handle_meets_requirements ────────────────────────────────

    #[test]
    // A multi-thread runtime with enable_all() meets all requirements.
    fn test_handle_multi_thread_all_features_returns_true() {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        assert!(XetRuntime::handle_meets_requirements(rt.handle()));
    }

    #[test]
    #[cfg(not(target_family = "wasm"))]
    // A current_thread runtime is rejected even when enable_all() is set.
    fn test_handle_current_thread_returns_false() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        assert!(!XetRuntime::handle_meets_requirements(rt.handle()));
    }

    #[test]
    // A multi-thread runtime with no drivers enabled returns false.
    fn test_handle_without_any_driver_returns_false() {
        let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        assert!(!XetRuntime::handle_meets_requirements(rt.handle()));
    }

    #[test]
    // A multi-thread runtime with only enable_time() is missing the IO driver.
    fn test_handle_without_io_driver_returns_false() {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_time().build().unwrap();
        assert!(!XetRuntime::handle_meets_requirements(rt.handle()));
    }

    #[test]
    // A multi-thread runtime with only enable_io() is missing the time driver.
    fn test_handle_without_time_driver_returns_false() {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_io().build().unwrap();
        assert!(!XetRuntime::handle_meets_requirements(rt.handle()));
    }

    // ── External-mode _blocking guard ────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // build_blocking on an External-mode session returns WrongRuntimeMode.
    async fn test_new_upload_commit_blocking_errors_in_external_mode() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime.mode(), RuntimeMode::External);
        let err = session.new_upload_commit().unwrap().build_blocking().err().unwrap();
        assert!(matches!(err, SessionError::WrongRuntimeMode(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    // build_blocking on an External-mode session returns WrongRuntimeMode.
    async fn test_new_file_download_group_blocking_errors_in_external_mode() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime.mode(), RuntimeMode::External);
        let err = session.new_file_download_group().unwrap().build_blocking().err().unwrap();
        assert!(matches!(err, SessionError::WrongRuntimeMode(_)));
    }

    // ── Owned-mode _blocking panic guard ─────────────────────────────────────

    #[test]
    // build_blocking panics when called from within a tokio runtime on an Owned-mode session.
    fn test_new_upload_commit_blocking_panics_in_async_context() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime.mode(), RuntimeMode::Owned);
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async { session.new_upload_commit().unwrap().build_blocking() })
        }));
        assert!(result.is_err(), "build_blocking() must panic when called from async");
    }

    #[test]
    // build_blocking panics when called from within a tokio runtime on an Owned-mode session.
    fn test_new_file_download_group_blocking_panics_in_async_context() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime.mode(), RuntimeMode::Owned);
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async { session.new_file_download_group().unwrap().build_blocking() })
        }));
        assert!(result.is_err(), "build_blocking() must panic when called from async");
    }

    // ── Streaming download ──────────────────────────────────────────────────

    #[test]
    // new_download_stream_group on an aborted session returns Aborted.
    fn test_download_stream_on_aborted_session_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.new_download_stream_group().err().unwrap();
        assert!(matches!(err, SessionError::Aborted));
    }

    #[test]
    // new_download_stream_group on an aborted session returns Aborted (blocking variant).
    fn test_download_stream_blocking_on_aborted_session_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.new_download_stream_group().err().unwrap();
        assert!(matches!(err, SessionError::Aborted));
    }

    #[tokio::test(flavor = "multi_thread")]
    // download_stream_blocking on a group returns WrongRuntimeMode on an External-mode session.
    async fn test_download_stream_blocking_errors_in_external_mode() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime.mode(), RuntimeMode::External);
        let group = session.new_download_stream_group().unwrap().build().await.unwrap();
        let result = group.download_stream_blocking(
            XetFileInfo {
                hash: "abc123".to_string(),
                file_size: Some(1024),
                sha256: None,
            },
            None,
        );
        assert!(matches!(result, Err(SessionError::WrongRuntimeMode(_))));
    }

    #[test]
    // Aborting a session clears all registered download stream groups.
    fn test_abort_clears_active_download_stream_groups() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _g1 = session.new_download_stream_group().unwrap().build_blocking().unwrap();
        session.abort().unwrap();
        assert_eq!(session.active_download_stream_groups.lock().unwrap().len(), 0);
    }

    #[test]
    // new_download_stream_group after abort returns Aborted.
    fn test_new_download_stream_group_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.new_download_stream_group().err().unwrap();
        assert!(matches!(err, SessionError::Aborted));
    }
}
