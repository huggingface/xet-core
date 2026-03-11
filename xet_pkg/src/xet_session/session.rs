//! XetSession - manages runtime and configuration

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use http::HeaderMap;
use ulid::Ulid;
use xet_client::cas_client::auth::TokenRefresher;
use xet_runtime::config::XetConfig;
use xet_runtime::core::XetRuntime;

use super::download_group::DownloadGroup;
use super::errors::SessionError;
use super::sync::{DownloadGroupSync, UploadCommitSync};
use super::upload_commit::UploadCommit;

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
    pub(crate) runtime: Arc<XetRuntime>,

    // Only accessed through &self; no independent cloning needed.
    pub(crate) config: XetConfig,

    // CAS endpoint and auth (shared by all upload commits/download groups)
    pub(crate) endpoint: Option<String>,
    pub(crate) token_info: Option<(String, u64)>,
    pub(crate) token_refresher: Option<Arc<dyn TokenRefresher>>,
    pub(crate) custom_headers: Option<Arc<HeaderMap>>,

    // Track active upload commits and download groups.
    pub(crate) active_upload_commits: Mutex<HashMap<Ulid, UploadCommit>>,
    pub(crate) active_download_groups: Mutex<HashMap<Ulid, DownloadGroup>>,

    // Session state
    state: Mutex<SessionState>,
    pub(crate) id: Ulid,
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
/// # Ok::<(), xet::xet_session::SessionError>(())
/// ```
pub struct XetSessionBuilder {
    config: XetConfig,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
    custom_headers: Option<Arc<HeaderMap>>,
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

    /// Consume the builder and create a [`XetSession`].
    pub fn build(self) -> Result<XetSession, SessionError> {
        XetSession::new_with_config(
            self.config,
            self.endpoint,
            self.token_info,
            self.token_refresher,
            self.custom_headers,
        )
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
    /// Create a session with default [`XetConfig`] — used by tests only.
    /// In production code, use [`XetSessionBuilder`] instead.
    #[cfg(test)]
    pub(crate) fn new(
        endpoint: Option<String>,
        token_info: Option<(String, u64)>,
        token_refresher: Option<Arc<dyn TokenRefresher>>,
        custom_headers: Option<Arc<HeaderMap>>,
    ) -> Result<Self, SessionError> {
        Self::new_with_config(XetConfig::new(), endpoint, token_info, token_refresher, custom_headers)
    }

    /// Internal constructor called by [`XetSessionBuilder::build`].
    pub(crate) fn new_with_config(
        config: XetConfig,
        endpoint: Option<String>,
        token_info: Option<(String, u64)>,
        token_refresher: Option<Arc<dyn TokenRefresher>>,
        custom_headers: Option<Arc<HeaderMap>>,
    ) -> Result<Self, SessionError> {
        let runtime = XetRuntime::new_or_attach_with_config(config.clone())?;

        let session_id = Ulid::new();

        Ok(Self {
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
                id: session_id,
            }),
        })
    }

    /// Create a new [`UploadCommit`] that groups related file uploads.
    ///
    /// Returns `Err(SessionError::Aborted)` if the session has been aborted.
    ///
    /// # Note
    ///
    /// This is an `async fn` and must be `.await`ed. For sync Rust or Python (PyO3) callers,
    /// use [`new_upload_commit_blocking`](Self::new_upload_commit_blocking).
    pub async fn new_upload_commit(&self) -> Result<UploadCommit, SessionError> {
        // Check state before the async init; drop the guard so it is not held across .await.
        {
            let state = self.state.lock()?;
            if matches!(*state, SessionState::Aborted) {
                return Err(SessionError::Aborted);
            }
        }

        let commit = UploadCommit::new(self.clone()).await?;

        // Register the commit
        self.active_upload_commits.lock()?.insert(commit.id(), commit.clone());

        Ok(commit)
    }

    /// Create a new [`UploadCommit`] from a **sync** (non-async) context.
    ///
    /// Returns `Err(SessionError::Aborted)` if the session has been aborted.
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime. Use
    /// [`new_upload_commit`](Self::new_upload_commit) instead.
    pub fn new_upload_commit_blocking(&self) -> Result<UploadCommitSync, SessionError> {
        {
            let state = self.state.lock()?;
            if matches!(*state, SessionState::Aborted) {
                return Err(SessionError::Aborted);
            }
        }

        let sync_commit = UploadCommitSync::new(self.clone())?;
        self.active_upload_commits
            .lock()?
            .insert(sync_commit.inner.id(), sync_commit.inner.clone());
        Ok(sync_commit)
    }

    /// Create a new [`DownloadGroup`] that groups related file downloads.
    ///
    /// Returns `Err(SessionError::Aborted)` if the session has been aborted.
    ///
    /// # Note
    ///
    /// This is an `async fn` and must be `.await`ed. For sync Rust or Python (PyO3) callers,
    /// use [`new_download_group_blocking`](Self::new_download_group_blocking).
    pub async fn new_download_group(&self) -> Result<DownloadGroup, SessionError> {
        // Check state before the async init; drop the guard so it is not held across .await.
        {
            let state = self.state.lock()?;
            if matches!(*state, SessionState::Aborted) {
                return Err(SessionError::Aborted);
            }
        }

        let group = DownloadGroup::new(self.clone()).await?;

        // Register the group
        self.active_download_groups.lock()?.insert(group.id(), group.clone());

        Ok(group)
    }

    /// Create a new [`DownloadGroup`] from a **sync** (non-async) context.
    ///
    /// Returns `Err(SessionError::Aborted)` if the session has been aborted.
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime. Use
    /// [`new_download_group`](Self::new_download_group) instead.
    pub fn new_download_group_blocking(&self) -> Result<DownloadGroupSync, SessionError> {
        {
            let state = self.state.lock()?;
            if matches!(*state, SessionState::Aborted) {
                return Err(SessionError::Aborted);
            }
        }

        let sync_group = DownloadGroupSync::new(self.clone())?;
        self.active_download_groups
            .lock()?
            .insert(sync_group.inner.id(), sync_group.inner.clone());
        Ok(sync_group)
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
        let active_download_groups = std::mem::take(&mut *self.active_download_groups.lock()?);
        for (_id, task) in active_download_groups {
            task.abort()?;
        }
        Ok(())
    }

    pub(crate) fn check_alive(&self) -> Result<(), SessionError> {
        if matches!(*self.state.lock()?, SessionState::Aborted) {
            return Err(SessionError::Aborted);
        }
        Ok(())
    }

    pub(crate) fn finish_upload_commit(&self, commit_id: Ulid) -> Result<(), SessionError> {
        self.active_upload_commits.lock()?.remove(&commit_id);
        Ok(())
    }

    pub(crate) fn finish_download_group(&self, group_id: Ulid) -> Result<(), SessionError> {
        self.active_download_groups.lock()?.remove(&group_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_session() -> XetSession {
        XetSession::new(None, None, None, None).expect("Failed to create session")
    }

    // ── Identity ─────────────────────────────────────────────────────────────

    #[test]
    // A clone refers to the same inner Arc, so their session IDs must match.
    fn test_session_clone_shares_state() {
        let s1 = make_session();
        let s2 = s1.clone();
        assert_eq!(s1.id, s2.id);
    }

    #[test]
    // Two independently created sessions have distinct IDs.
    fn test_two_sessions_have_distinct_ids() {
        let s1 = make_session();
        let s2 = make_session();
        assert_ne!(s1.id, s2.id);
    }

    // ── Abort behavior ───────────────────────────────────────────────────────

    #[test]
    // After abort, check_alive returns Aborted.
    fn test_check_alive_after_abort() {
        let session = make_session();
        session.abort().unwrap();
        let err = session.check_alive().unwrap_err();
        assert!(matches!(err, SessionError::Aborted));
    }

    #[test]
    // new_upload_commit_blocking on an aborted session returns Aborted.
    fn test_new_upload_commit_after_abort_returns_aborted() {
        let session = make_session();
        session.abort().unwrap();
        let err = session.new_upload_commit_blocking().err().unwrap();
        assert!(matches!(err, SessionError::Aborted));
    }

    #[test]
    // new_download_group_blocking on an aborted session returns Aborted.
    fn test_new_download_group_after_abort_returns_aborted() {
        let session = make_session();
        session.abort().unwrap();
        let err = session.new_download_group_blocking().err().unwrap();
        assert!(matches!(err, SessionError::Aborted));
    }

    #[test]
    // Aborting a session clears all registered upload commits.
    fn test_abort_clears_active_upload_commits() {
        let session = make_session();
        let _c1 = session.new_upload_commit_blocking().unwrap();
        let _c2 = session.new_upload_commit_blocking().unwrap();
        session.abort().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 0);
    }

    #[test]
    // Aborting a session clears all registered download groups.
    fn test_abort_clears_active_download_groups() {
        let session = make_session();
        let _g1 = session.new_download_group_blocking().unwrap();
        session.abort().unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 0);
    }

    // ── Registration ─────────────────────────────────────────────────────────

    #[test]
    // A new upload commit is registered in the session's active set.
    fn test_new_upload_commit_registers_in_session() {
        let session = make_session();
        let _commit = session.new_upload_commit_blocking().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
    }

    #[test]
    // A new download group is registered in the session's active set.
    fn test_new_download_group_registers_in_session() {
        let session = make_session();
        let _group = session.new_download_group_blocking().unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
    }

    // ── Deregistration ───────────────────────────────────────────────────────

    #[test]
    // finish_upload_commit removes only the specified commit, leaving others intact.
    fn test_finish_upload_commit_removes_only_that_commit() {
        let session = make_session();
        let c1 = session.new_upload_commit_blocking().unwrap();
        let _c2 = session.new_upload_commit_blocking().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 2);
        session.finish_upload_commit(c1.inner.id()).unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
    }

    #[test]
    // finish_download_group removes only the specified group, leaving others intact.
    fn test_finish_download_group_removes_only_that_group() {
        let session = make_session();
        let g1 = session.new_download_group_blocking().unwrap();
        let _g2 = session.new_download_group_blocking().unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 2);
        session.finish_download_group(g1.inner.id()).unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
    }

    #[test]
    // finish_upload_commit on an unknown ID is a no-op (no error, no change).
    fn test_finish_upload_commit_with_unknown_id_is_noop() {
        let session = make_session();
        let _c1 = session.new_upload_commit_blocking().unwrap();
        let unknown_id = ulid::Ulid::new();
        assert!(session.finish_upload_commit(unknown_id).is_ok());
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
    }

    // ── Async abort behavior ──────────────────────────────────────────────────

    #[tokio::test]
    // new_upload_commit / new_download_group on an aborted session both return Aborted.
    async fn test_async_new_after_abort_returns_aborted() {
        let session = make_session();
        session.abort().unwrap();
        let commit_err = session.new_upload_commit().await.err().unwrap();
        let group_err = session.new_download_group().await.err().unwrap();
        assert!(matches!(commit_err, SessionError::Aborted));
        assert!(matches!(group_err, SessionError::Aborted));
    }

    #[tokio::test]
    // Aborting a session clears all active upload commits and download groups.
    async fn test_async_abort_clears_active_commits_and_groups() {
        let session = make_session();
        let (_c1, _c2, _g1) =
            tokio::join!(session.new_upload_commit(), session.new_upload_commit(), session.new_download_group(),);
        session.abort().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 0);
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 0);
    }

    // ── Async registration ────────────────────────────────────────────────────

    #[tokio::test]
    // A new upload commit and a new download group are each registered in the
    // session's active set, and concurrent creation registers both.
    async fn test_async_new_registers_in_session() {
        let session = make_session();
        let (commit_res, group_res) = tokio::join!(session.new_upload_commit(), session.new_download_group());
        let _commit = commit_res.unwrap();
        let _group = group_res.unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
    }

    // ── Async deregistration ──────────────────────────────────────────────────

    #[tokio::test]
    // Finishing one upload commit / download group removes only that one,
    // leaving the other still registered.
    async fn test_async_finish_removes_only_that_item() {
        let session = make_session();
        let (c1_res, c2_res, g1_res, g2_res) = tokio::join!(
            session.new_upload_commit(),
            session.new_upload_commit(),
            session.new_download_group(),
            session.new_download_group(),
        );
        let c1 = c1_res.unwrap();
        let _c2 = c2_res.unwrap();
        let g1 = g1_res.unwrap();
        let _g2 = g2_res.unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 2);
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 2);
        session.finish_upload_commit(c1.id()).unwrap();
        session.finish_download_group(g1.id()).unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
    }

    // ── Sync-inside-async panic guards ───────────────────────────────────────

    #[tokio::test]
    // new_upload_commit_blocking panics when called from inside a tokio runtime.
    async fn test_new_upload_commit_blocking_panics_in_async_context() {
        let session = make_session();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = session.new_upload_commit_blocking();
        }));
        assert!(result.is_err(), "expected panic from _blocking inside async");
    }

    #[tokio::test]
    // new_download_group_blocking panics when called from inside a tokio runtime.
    async fn test_new_download_group_blocking_panics_in_async_context() {
        let session = make_session();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = session.new_download_group_blocking();
        }));
        assert!(result.is_err(), "expected panic from _blocking inside async");
    }
}
