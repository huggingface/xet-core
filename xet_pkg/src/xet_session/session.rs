//! XetSession - manages runtime and configuration

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use http::HeaderMap;
use tracing::info;
use ulid::Ulid;
use xet_client::cas_client::auth::TokenRefresher;
use xet_data::progress_tracking::UniqueID;
use xet_runtime::config::XetConfig;
use xet_runtime::core::XetRuntime;

use super::download_group::DownloadGroup;
use super::upload_commit::UploadCommit;
use crate::error::XetError;

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

    // CAS endpoint and auth (shared by all upload commits/download groups)
    pub(super) endpoint: Option<String>,
    pub(super) token_info: Option<(String, u64)>,
    pub(super) token_refresher: Option<Arc<dyn TokenRefresher>>,
    pub(super) custom_headers: Option<Arc<HeaderMap>>,

    // Track active upload commits and download groups.
    pub(super) active_upload_commits: Mutex<HashMap<UniqueID, UploadCommit>>,
    pub(super) active_download_groups: Mutex<HashMap<UniqueID, DownloadGroup>>,

    // Session state
    state: Mutex<SessionState>,
    // "id" is used to identity a group of activities on our server, and so need to be globally unique
    pub(super) id: Ulid,
}

/// Builder for [`XetSession`].
///
/// All fields are optional; call [`build`](XetSessionBuilder::build) when done.
///
/// ```rust,no_run
/// # use xet::xet_session::XetSessionBuilder;
/// let session = XetSessionBuilder::new()
///     .with_endpoint("https://cas.example.com".into())
///     .with_token_info("my-token".into(), 1_700_000_000)
///     .build()?;
/// # Ok::<(), xet::XetError>(())
/// ```
pub struct XetSessionBuilder {
    config: XetConfig,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
    custom_headers: Option<Arc<HeaderMap>>,
    tokio_handle: Option<tokio::runtime::Handle>,
}

impl Default for XetSessionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl XetSessionBuilder {
    /// Create a builder with default [`XetConfig`] and no authentication.
    pub fn new() -> Self {
        Self {
            config: XetConfig::new(),
            endpoint: None,
            token_info: None,
            token_refresher: None,
            custom_headers: None,
            tokio_handle: None,
        }
    }

    /// Create a builder pre-populated with the given [`XetConfig`].
    pub fn new_with_config(config: XetConfig) -> Self {
        Self {
            config,
            endpoint: None,
            token_info: None,
            token_refresher: None,
            custom_headers: None,
            tokio_handle: None,
        }
    }

    /// Set the Xet CAS server endpoint URL (e.g. `"https://cas.example.com"`).
    pub fn with_endpoint(self, endpoint: String) -> Self {
        Self {
            endpoint: Some(endpoint),
            ..self
        }
    }

    /// Set a static Xet CAS server access token and its expiry as a Unix timestamp (seconds).
    pub fn with_token_info(self, token: String, expiry: u64) -> Self {
        Self {
            token_info: Some((token, expiry)),
            ..self
        }
    }

    /// Set a callback that is invoked to refresh the Xet CAS server access token when it expires.
    pub fn with_token_refresher(self, refresher: Arc<dyn TokenRefresher>) -> Self {
        Self {
            token_refresher: Some(refresher),
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
    /// the session will wrap it — no second thread pool is created. Only async
    /// methods (`new_upload_commit`, `new_download_group`) may be called; `_blocking` variants
    /// will return [`XetError::WrongRuntimeMode`].
    ///
    /// If the handle does **not** meet requirements (e.g. `current_thread` flavor or missing
    /// drivers), it is silently ignored and [`build`](Self::build) will fall back to creating
    /// an owned thread pool instead.
    pub fn with_tokio_handle(self, handle: tokio::runtime::Handle) -> Self {
        #[cfg(not(target_family = "wasm"))]
        let accept = XetRuntime::handle_meets_requirements(&handle);
        #[cfg(target_family = "wasm")]
        let accept = true;

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
    pub fn build(self) -> Result<XetSession, XetError> {
        let handle = self.tokio_handle.or_else(|| {
            tokio::runtime::Handle::try_current().ok().filter(|h| {
                #[cfg(not(target_family = "wasm"))]
                {
                    XetRuntime::handle_meets_requirements(h)
                }
                #[cfg(target_family = "wasm")]
                {
                    let _ = h;
                    true
                }
            })
        });

        let runtime = match handle {
            Some(h) => XetRuntime::from_external_with_config(h, self.config.clone()),
            None => XetRuntime::new_with_config(self.config.clone())?,
        };

        Ok(XetSession::new(
            self.config,
            self.endpoint,
            self.token_info,
            self.token_refresher,
            self.custom_headers,
            runtime,
        ))
    }
}

/// Handle for managing file uploads and downloads.
///
/// `XetSession` is the top-level entry point for the xet-session API.  It
/// owns a `XetRuntime` (tokio thread pool) and holds authentication
/// credentials that are shared by all [`UploadCommit`]s and
/// [`DownloadGroup`]s created from it.
///
/// # Cloning
///
/// Cloning is cheap — it simply increments an atomic reference count.
/// All clones share the same runtime and credentials.
///
/// # Lifecycle
///
/// 1. Create a session with [`XetSessionBuilder`].
/// 2. Create one or more [`UploadCommit`]s / [`DownloadGroup`]s.
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
        token_info: Option<(String, u64)>,
        token_refresher: Option<Arc<dyn TokenRefresher>>,
        custom_headers: Option<Arc<HeaderMap>>,
        runtime: Arc<XetRuntime>,
    ) -> Self {
        Self {
            inner: Arc::new(XetSessionInner {
                runtime,
                config,
                endpoint,
                token_info,
                token_refresher,
                custom_headers,
                active_upload_commits: Mutex::new(HashMap::new()),
                active_download_groups: Mutex::new(HashMap::new()),
                state: Mutex::new(SessionState::Alive),
                id: Ulid::new(),
            }),
        }
    }

    /// Create a new [`UploadCommit`] that groups related file uploads.
    ///
    /// Returns `Err(XetError::Aborted)` if the session has been aborted.
    ///
    /// # Note
    ///
    /// This is an `async fn` and must be `.await`ed. For sync Rust or Python (PyO3) callers,
    /// use [`new_upload_commit_blocking`](Self::new_upload_commit_blocking).
    pub async fn new_upload_commit(&self) -> Result<UploadCommit, XetError> {
        {
            let state = self.state.lock()?;
            if matches!(*state, SessionState::Aborted) {
                return Err(XetError::Aborted);
            }
        }

        let session = self.clone();
        let commit = self
            .runtime
            .bridge_async("new_upload_commit", async move { UploadCommit::new(session).await })
            .await??;

        self.active_upload_commits.lock()?.insert(commit.id(), commit.clone());

        Ok(commit)
    }

    /// Create a new [`UploadCommit`] from a **sync** (non-async) context.
    ///
    /// Returns `Err(XetError::Aborted)` if the session has been aborted.
    /// Returns `Err(XetError::WrongRuntimeMode)` if the session uses an external
    /// tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if called from within a **tokio** async runtime (tokio sets a thread-local
    /// context that `Handle::block_on` detects and panics on). Non-tokio executors (smol,
    /// async-std, `futures::executor`) do not set this context, so calling from those is
    /// safe. Use [`new_upload_commit`](Self::new_upload_commit) from async contexts instead.
    pub fn new_upload_commit_blocking(&self) -> Result<UploadCommit, XetError> {
        {
            let state = self.state.lock()?;
            if matches!(*state, SessionState::Aborted) {
                return Err(XetError::Aborted);
            }
        }

        let commit = self.runtime.bridge_sync(UploadCommit::new(self.clone()))??;
        self.active_upload_commits.lock()?.insert(commit.id(), commit.clone());
        Ok(commit)
    }

    /// Create a new [`DownloadGroup`] that groups related file downloads.
    ///
    /// Returns `Err(XetError::Aborted)` if the session has been aborted.
    ///
    /// # Note
    ///
    /// This is an `async fn` and must be `.await`ed. For sync Rust or Python (PyO3) callers,
    /// use [`new_download_group_blocking`](Self::new_download_group_blocking).
    pub async fn new_download_group(&self) -> Result<DownloadGroup, XetError> {
        {
            let state = self.state.lock()?;
            if matches!(*state, SessionState::Aborted) {
                return Err(XetError::Aborted);
            }
        }

        let session = self.clone();
        let group = self
            .runtime
            .bridge_async("new_download_group", async move { DownloadGroup::new(session).await })
            .await??;

        self.active_download_groups.lock()?.insert(group.id(), group.clone());

        Ok(group)
    }

    /// Create a new [`DownloadGroup`] from a **sync** (non-async) context.
    ///
    /// Returns `Err(XetError::Aborted)` if the session has been aborted.
    /// Returns `Err(XetError::WrongRuntimeMode)` if the session uses an external
    /// tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if called from within a **tokio** async runtime (tokio sets a thread-local
    /// context that `Handle::block_on` detects and panics on). Non-tokio executors (smol,
    /// async-std, `futures::executor`) do not set this context, so calling from those is
    /// safe. Use [`new_download_group`](Self::new_download_group) from async contexts instead.
    pub fn new_download_group_blocking(&self) -> Result<DownloadGroup, XetError> {
        {
            let state = self.state.lock()?;
            if matches!(*state, SessionState::Aborted) {
                return Err(XetError::Aborted);
            }
        }

        let group = self.runtime.bridge_sync(DownloadGroup::new(self.clone()))??;
        self.active_download_groups.lock()?.insert(group.id(), group.clone());
        Ok(group)
    }

    /// Abort the session - cancel all currently running tasks
    ///
    /// This performs a SIGINT-style shutdown, aborting all active upload and download tasks.
    /// Use this when a Ctrl+C signal is detected or when you need to immediately stop all operations.
    pub fn abort(&self) -> Result<(), XetError> {
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
        let active_download_groups = std::mem::take(&mut *self.active_download_groups.lock()?);
        for (_id, task) in active_download_groups {
            task.abort()?;
        }
        Ok(())
    }

    pub(super) fn check_alive(&self) -> Result<(), XetError> {
        if matches!(*self.state.lock()?, SessionState::Aborted) {
            return Err(XetError::Aborted);
        }
        Ok(())
    }

    pub(super) fn finish_upload_commit(&self, commit_id: UniqueID) -> Result<(), XetError> {
        self.active_upload_commits.lock()?.remove(&commit_id);
        Ok(())
    }

    pub(super) fn finish_download_group(&self, group_id: UniqueID) -> Result<(), XetError> {
        self.active_download_groups.lock()?.remove(&group_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use xet_runtime::core::RuntimeMode;

    use super::*;

    // ── Identity ─────────────────────────────────────────────────────────────

    #[test]
    fn test_session_clone_shares_state() {
        let s1 = XetSessionBuilder::new().build().unwrap();
        let s2 = s1.clone();
        assert_eq!(s1.id, s2.id);
    }

    #[test]
    fn test_two_sessions_have_distinct_ids() {
        let s1 = XetSessionBuilder::new().build().unwrap();
        let s2 = XetSessionBuilder::new().build().unwrap();
        assert_ne!(s1.id, s2.id);
    }

    #[test]
    fn test_session_id_is_ulid() {
        let s = XetSessionBuilder::new().build().unwrap();
        assert!(Ulid::from_string(&s.id.to_string()).is_ok())
    }

    // ── Abort behavior ───────────────────────────────────────────────────────

    #[test]
    fn test_check_alive_after_abort() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.check_alive().unwrap_err();
        assert!(matches!(err, XetError::Aborted));
    }

    #[test]
    fn test_new_upload_commit_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.new_upload_commit_blocking().err().unwrap();
        assert!(matches!(err, XetError::Aborted));
    }

    #[test]
    fn test_new_download_group_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.new_download_group_blocking().err().unwrap();
        assert!(matches!(err, XetError::Aborted));
    }

    #[test]
    fn test_abort_clears_active_upload_commits() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _c1 = session.new_upload_commit_blocking().unwrap();
        let _c2 = session.new_upload_commit_blocking().unwrap();
        session.abort().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 0);
    }

    #[test]
    fn test_abort_clears_active_download_groups() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _g1 = session.new_download_group_blocking().unwrap();
        session.abort().unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 0);
    }

    // ── Registration ─────────────────────────────────────────────────────────

    #[test]
    fn test_new_upload_commit_registers_in_session() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _commit = session.new_upload_commit_blocking().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_new_download_group_registers_in_session() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _group = session.new_download_group_blocking().unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
    }

    // ── Deregistration ───────────────────────────────────────────────────────

    #[test]
    fn test_finish_upload_commit_removes_only_that_commit() {
        let session = XetSessionBuilder::new().build().unwrap();
        let c1 = session.new_upload_commit_blocking().unwrap();
        let _c2 = session.new_upload_commit_blocking().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 2);
        session.finish_upload_commit(c1.id()).unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_finish_download_group_removes_only_that_group() {
        let session = XetSessionBuilder::new().build().unwrap();
        let g1 = session.new_download_group_blocking().unwrap();
        let _g2 = session.new_download_group_blocking().unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 2);
        session.finish_download_group(g1.id()).unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_finish_upload_commit_with_unknown_id_is_noop() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _c1 = session.new_upload_commit_blocking().unwrap();
        let unknown_id = UniqueID::new();
        assert!(session.finish_upload_commit(unknown_id).is_ok());
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
    }

    // ── Async abort behavior ──────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_new_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let commit_err = session.new_upload_commit().await.err().unwrap();
        let group_err = session.new_download_group().await.err().unwrap();
        assert!(matches!(commit_err, XetError::Aborted));
        assert!(matches!(group_err, XetError::Aborted));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_abort_clears_active_commits_and_groups() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _c1 = session.new_upload_commit().await.unwrap();
        let _c2 = session.new_upload_commit().await.unwrap();
        let _g1 = session.new_download_group().await.unwrap();
        session.abort().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 0);
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 0);
    }

    // ── Async registration ────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_new_registers_in_session() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _commit = session.new_upload_commit().await.unwrap();
        let _group = session.new_download_group().await.unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
    }

    // ── Async deregistration ──────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_finish_removes_only_that_item() {
        let session = XetSessionBuilder::new().build().unwrap();
        let c1 = session.new_upload_commit().await.unwrap();
        let _c2 = session.new_upload_commit().await.unwrap();
        let g1 = session.new_download_group().await.unwrap();
        let _g2 = session.new_download_group().await.unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 2);
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 2);
        session.finish_upload_commit(c1.id()).unwrap();
        session.finish_download_group(g1.id()).unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
    }

    // ── External-mode _blocking guard ────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_upload_commit_blocking_errors_in_external_mode() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime.mode(), RuntimeMode::External);
        let err = session.new_upload_commit_blocking().err().unwrap();
        assert!(matches!(err, XetError::WrongRuntimeMode(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_download_group_blocking_errors_in_external_mode() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime.mode(), RuntimeMode::External);
        let err = session.new_download_group_blocking().err().unwrap();
        assert!(matches!(err, XetError::WrongRuntimeMode(_)));
    }

    // ── Owned-mode _blocking panic guard ─────────────────────────────────────

    #[test]
    fn test_new_upload_commit_blocking_panics_in_async_context() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime.mode(), RuntimeMode::Owned);
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async { session.new_upload_commit_blocking() })
        }));
        assert!(result.is_err(), "new_upload_commit_blocking() must panic when called from async");
    }

    #[test]
    fn test_new_download_group_blocking_panics_in_async_context() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime.mode(), RuntimeMode::Owned);
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async { session.new_download_group_blocking() })
        }));
        assert!(result.is_err(), "new_download_group_blocking() must panic when called from async");
    }
}
