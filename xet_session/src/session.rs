//! XetSession - manages runtime and configuration

use http::HeaderMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use ulid::Ulid;
use utils::auth::TokenRefresher;
use xet_config::XetConfig;
use xet_runtime::XetRuntime;

use crate::download_group::DownloadGroup;
use crate::errors::SessionError;
use crate::upload_commit::UploadCommit;

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
    pub runtime: Arc<XetRuntime>,

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
/// 1. Create a session with [`XetSession::new`] or [`XetSession::new_with_config`].
/// 2. Create one or more [`UploadCommit`]s / [`DownloadGroup`]s.
/// 3. When a graceful shutdown is needed, drop the session (or all clones of
///    it).  For an emergency stop, call [`XetSession::abort`].
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
    /// Create a new session with default [`XetConfig`].
    ///
    /// # Parameters
    ///
    /// * `endpoint` – Specify the CAS server endpoint URL.  Pass `None` to
    ///   use the default (local CAS).
    /// * `token_info` – `(token, expiry_unix_timestamp)` pair for
    ///   authentication.  Pass `None` for unauthenticated access.
    /// * `token_refresher` – Optional callback used to obtain a fresh token
    ///   when the current one expires.
    /// * `user_agent` – User-agent string sent with every request (e.g.
    ///   `"my-app/1.2.3"`).
    pub fn new(
        endpoint: Option<String>,
        token_info: Option<(String, u64)>,
        token_refresher: Option<Arc<dyn TokenRefresher>>,
        custom_headers: Option<Arc<HeaderMap>>,
    ) -> Result<Self, SessionError> {
        Self::new_with_config(XetConfig::new(), endpoint, token_info, token_refresher, custom_headers)
    }

    /// Create a new session with a custom [`XetConfig`].
    ///
    /// Like [`XetSession::new`] but also accepts a fully-populated
    /// [`XetConfig`].
    pub fn new_with_config(
        config: XetConfig,
        endpoint: Option<String>,
        token_info: Option<(String, u64)>,
        token_refresher: Option<Arc<dyn TokenRefresher>>,
        custom_headers: Option<Arc<HeaderMap>>,
    ) -> Result<Self, SessionError> {
        let runtime = XetRuntime::new_with_config(config.clone())?;

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
    pub fn new_upload_commit(&self) -> Result<UploadCommit, SessionError> {
        let state = self.state.lock()?;
        if matches!(*state, SessionState::Aborted) {
            return Err(SessionError::Aborted);
        }

        let commit = UploadCommit::new(self.clone())?;

        // Register the commit
        self.active_upload_commits.lock()?.insert(commit.id(), commit.clone());

        Ok(commit)
    }

    /// Create a new [`DownloadGroup`] that groups related file downloads.
    ///
    /// Returns `Err(SessionError::Aborted)` if the session has been aborted.
    pub fn new_download_group(&self) -> Result<DownloadGroup, SessionError> {
        let state = self.state.lock()?;
        if matches!(*state, SessionState::Aborted) {
            return Err(SessionError::Aborted);
        }

        let group = DownloadGroup::new(self.clone())?;

        // Register the group
        self.active_download_groups.lock()?.insert(group.id(), group.clone());

        Ok(group)
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
        let mut active_upload_commits = self.active_upload_commits.lock()?;
        for (_id, task) in active_upload_commits.drain() {
            task.abort()?;
        }
        let mut active_download_groups = self.active_download_groups.lock()?;
        for (_id, task) in active_download_groups.drain() {
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

    #[test]
    fn test_session_new_succeeds() {
        let _session = make_session();
    }

    #[test]
    fn test_session_clone_shares_state() {
        let s1 = make_session();
        let s2 = s1.clone();
        // Both refer to the same inner, so their session IDs must match.
        assert_eq!(s1.id, s2.id);
    }

    #[test]
    fn test_check_alive_before_abort() {
        let session = make_session();
        assert!(session.check_alive().is_ok());
    }

    #[test]
    fn test_abort_returns_ok() {
        let session = make_session();
        assert!(session.abort().is_ok());
    }

    #[test]
    fn test_check_alive_after_abort() {
        let session = make_session();
        session.abort().unwrap();
        let err = session.check_alive().unwrap_err();
        assert!(matches!(err, SessionError::Aborted));
    }

    #[test]
    fn test_new_upload_commit_after_abort_returns_aborted() {
        let session = make_session();
        session.abort().unwrap();
        let err = session.new_upload_commit().err().unwrap();
        assert!(matches!(err, SessionError::Aborted));
    }

    #[test]
    fn test_new_download_group_after_abort_returns_aborted() {
        let session = make_session();
        session.abort().unwrap();
        let err = session.new_download_group().err().unwrap();
        assert!(matches!(err, SessionError::Aborted));
    }

    #[test]
    fn test_new_upload_commit_registers_in_session() {
        let session = make_session();
        let _commit = session.new_upload_commit().expect("Failed to create upload commit");
        // Session tracked it — active_upload_commits should have 1 entry.
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_new_download_group_registers_in_session() {
        let session = make_session();
        let _group = session.new_download_group().expect("Failed to create download group");
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_multiple_commits_registered() {
        let session = make_session();
        let _c1 = session.new_upload_commit().unwrap();
        let _c2 = session.new_upload_commit().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 2);
    }
}
