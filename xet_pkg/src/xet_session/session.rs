//! XetSession - manages runtime and configuration

use std::collections::HashMap;
use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use http::HeaderMap;
use tracing::info;
use ulid::Ulid;
use xet_client::cas_client::auth::TokenRefresher;
use xet_data::progress_tracking::UniqueID;
use xet_runtime::RuntimeError;
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

/// Whether the session owns its tokio runtime or inherits an external one.
///
/// - **`Owned`**: session created its own thread pool via [`XetSessionBuilder::build`] or
///   [`XetSessionBuilder::build_async`] (outside tokio). Both `_blocking` and async methods are supported. Async
///   methods use an internal `bridge_to_owned` bridge that routes futures onto the owned thread pool, so they work from
///   any executor (tokio, smol, async-std).
///
/// - **`External`**: session wraps a caller-provided tokio handle via [`XetSessionBuilder::with_tokio_handle`] or
///   [`XetSessionBuilder::build_async`] (tokio context). Only async methods may be called; `_blocking` methods return
///   [`XetError::WrongRuntimeMode`]. No second thread pool is created.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum RuntimeMode {
    Owned,
    External,
}

/// All shared state for a session.
/// Lives behind `Arc<XetSessionInner>` — do not use this type directly.
#[doc(hidden)]
pub struct XetSessionInner {
    // Independently cloned by background tasks, so needs its own Arc.
    pub(super) runtime: Arc<XetRuntime>,

    /// Whether the session owns its runtime or wraps an external tokio handle.
    pub(super) runtime_mode: RuntimeMode,

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

/// Probe whether a tokio runtime handle meets the requirements for External mode.
///
/// Checks three things:
/// 1. **Multi-threaded flavor** (non-WASM only).
/// 2. **Time driver** — required for timeouts, retry backoff, and progress intervals.
/// 3. **IO driver** — required for all network I/O via `reqwest`/`hyper`.
///
/// Driver availability is probed by entering the handle's context and polling a
/// driver-dependent future once inside `catch_unwind`.  Tokio panics synchronously
/// on the first poll when a driver is absent, so the result is immediate — no
/// spawning or blocking required.
///
/// **Fragility note:** this probing technique relies on tokio panicking
/// synchronously on the first poll of `tokio::time::sleep` /
/// `tokio::net::TcpListener::bind` when the corresponding driver (time / IO)
/// is absent.  This is undocumented internal behavior validated against
/// tokio 1.x.  If a future tokio version returns an error instead of
/// panicking, this function will incorrectly accept a runtime missing drivers.
///
/// Returns `true` if all requirements are met, `false` otherwise.
fn handle_meets_session_requirements(handle: &tokio::runtime::Handle) -> bool {
    // Non-WASM: require a multi-threaded runtime.
    #[cfg(not(target_family = "wasm"))]
    if matches!(handle.runtime_flavor(), tokio::runtime::RuntimeFlavor::CurrentThread) {
        return false;
    }

    let _guard = handle.enter();
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    let has_time = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut sleep = pin!(tokio::time::sleep(std::time::Duration::ZERO));
        let _ = sleep.as_mut().poll(&mut cx);
    }))
    .is_ok();

    let has_io = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut bind = pin!(tokio::net::TcpListener::bind("127.0.0.1:0"));
        let _ = bind.as_mut().poll(&mut cx);
    }))
    .is_ok();

    has_time && has_io
}

/// Builder for [`XetSession`].
///
/// All fields are optional; call [`build`](XetSessionBuilder::build) (sync) or
/// [`build_async`](XetSessionBuilder::build_async) (async) when done.
///
/// ```rust,no_run
/// # use xet::xet_session::XetSessionBuilder;
/// // Sync context — session owns its runtime:
/// let session = XetSessionBuilder::new()
///     .with_endpoint("https://cas.example.com".into())
///     .with_token_info("my-token".into(), 1_700_000_000)
///     .build()?;
/// # Ok::<(), xet::XetError>(())
/// ```
///
/// ```rust,no_run
/// # use xet::xet_session::XetSessionBuilder;
/// # async fn example() -> Result<(), xet::XetError> {
/// // Async context — wraps the caller's tokio handle (External mode) if inside tokio,
/// // or creates an owned runtime (Owned mode) if called from a non-tokio executor:
/// let session = XetSessionBuilder::new()
///     .with_endpoint("https://cas.example.com".into())
///     .with_token_info("my-token".into(), 1_700_000_000)
///     .build_async()
///     .await?;
/// # Ok(())
/// # }
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
    /// If the handle meets session requirements (multi-thread flavor, time driver, IO driver),
    /// the session will wrap it — no second thread pool is created (External mode). Only async
    /// methods (`new_upload_commit`, `new_download_group`) may be called; `_blocking` variants
    /// will return [`XetError::WrongRuntimeMode`].
    ///
    /// If the handle does **not** meet requirements (e.g. `current_thread` flavor or missing
    /// drivers), it is silently ignored and [`build`](Self::build) will fall back to creating
    /// an owned thread pool (Owned mode) instead.
    ///
    /// Use [`build_async`](Self::build_async) as a convenient alternative when building from
    /// within a tokio async context.
    pub fn with_tokio_handle(self, handle: tokio::runtime::Handle) -> Self {
        let accept = handle_meets_session_requirements(&handle);
        if !accept {
            info!("supplied tokio handle rejected (missing drivers or wrong flavor); falling back to Owned mode");
        }
        Self {
            tokio_handle: accept.then_some(handle),
            ..self
        }
    }

    /// Build and automatically attach to the current runtime.
    ///
    /// Despite being `async`, this method resolves synchronously (no internal
    /// `.await` points).  It is declared `async` so callers in an async context
    /// can use it naturally alongside `tokio::runtime::Handle::try_current()`
    /// detection.
    ///
    /// - **Tokio context** with a suitable runtime (multi-thread, time + IO drivers): wraps the caller's handle via
    ///   [`with_tokio_handle`](Self::with_tokio_handle) — External mode.
    /// - **Tokio context** with an unsuitable runtime (e.g. `current_thread`): handle is discarded by
    ///   `with_tokio_handle`; falls back to an owned thread pool — Owned mode.
    /// - **Non-tokio context** (smol, async-std, etc.): creates an owned thread pool — Owned mode; async methods use an
    ///   internal bridge compatible with any executor.
    pub async fn build_async(self) -> Result<XetSession, XetError> {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => self.with_tokio_handle(handle).build(),
            Err(_) => self.build(),
        }
    }

    /// Consume the builder and create a [`XetSession`].
    ///
    /// - If a valid tokio handle was previously set via [`with_tokio_handle`](Self::with_tokio_handle), the session
    ///   wraps that handle (External mode) — no second thread pool is created.
    /// - Otherwise, creates an owned thread pool (Owned mode); async methods use an internal bridge and work from any
    ///   executor, and `_blocking` methods are available.
    ///
    /// For async contexts, prefer [`build_async`](Self::build_async).
    pub fn build(self) -> Result<XetSession, XetError> {
        let (runtime, mode) = match self.tokio_handle {
            Some(handle) => (XetRuntime::from_external_with_config(handle, self.config.clone()), RuntimeMode::External),
            None => (XetRuntime::new_with_config(self.config.clone())?, RuntimeMode::Owned),
        };
        Ok(XetSession::new(
            self.config,
            self.endpoint,
            self.token_info,
            self.token_refresher,
            self.custom_headers,
            runtime,
            mode,
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
        runtime_mode: RuntimeMode,
    ) -> Self {
        Self {
            inner: Arc::new(XetSessionInner {
                runtime,
                runtime_mode,
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

    /// Run a future on the appropriate runtime for this session.
    ///
    /// In External mode the future is awaited directly on the caller's executor.
    /// In Owned mode the future is bridged onto the owned thread pool via
    /// [`XetRuntime::bridge_to_owned`].
    pub(super) async fn dispatch<T, F>(&self, task_name: &'static str, fut: F) -> Result<T, RuntimeError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        match self.runtime_mode {
            RuntimeMode::External => Ok(fut.await),
            RuntimeMode::Owned => self.runtime.bridge_to_owned(task_name, fut).await,
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
        // Check state before the async init; drop the guard so it is not held across .await.
        {
            let state = self.state.lock()?;
            if matches!(*state, SessionState::Aborted) {
                return Err(XetError::Aborted);
            }
        }

        let session = self.clone();
        let commit = self
            .dispatch("new_upload_commit", async move { UploadCommit::new(session).await })
            .await??;

        // Register the commit (sync insertion, safe in any executor context)
        self.active_upload_commits.lock()?.insert(commit.id(), commit.clone());

        Ok(commit)
    }

    /// Create a new [`UploadCommit`] from a **sync** (non-async) context.
    ///
    /// The returned [`UploadCommit`] supports both async methods (`upload_from_path`,
    /// `commit`) and blocking methods (`upload_from_path_blocking`, `commit_blocking`).
    ///
    /// Returns `Err(XetError::Aborted)` if the session has been aborted.
    /// Returns `Err(XetError::WrongRuntimeMode)` if the session uses an external
    /// tokio runtime (from [`XetSessionBuilder::with_tokio_handle`] or tokio-detected
    /// [`XetSessionBuilder::build_async`]).
    ///
    /// # Panics
    ///
    /// Panics if called from within a **tokio** async runtime (tokio sets a thread-local
    /// context that `Handle::block_on` detects and panics on). Non-tokio executors (smol,
    /// async-std, `futures::executor`) do not set this context, so calling from those is
    /// safe — it blocks the executor thread until the task completes. Use
    /// [`new_upload_commit`](Self::new_upload_commit) from async contexts instead.
    pub fn new_upload_commit_blocking(&self) -> Result<UploadCommit, XetError> {
        if matches!(self.runtime_mode, RuntimeMode::External) {
            return Err(XetError::wrong_mode(
                "new_upload_commit_blocking() cannot be called on a session using an \
                 external tokio runtime (with_tokio_handle() or tokio build_async()); \
                 use new_upload_commit().await instead",
            ));
        }
        {
            let state = self.state.lock()?;
            if matches!(*state, SessionState::Aborted) {
                return Err(XetError::Aborted);
            }
        }

        let commit = self.runtime.external_run_async_task(UploadCommit::new(self.clone()))??;
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
        // Check state before the async init; drop the guard so it is not held across .await.
        {
            let state = self.state.lock()?;
            if matches!(*state, SessionState::Aborted) {
                return Err(XetError::Aborted);
            }
        }

        let session = self.clone();
        let group = self
            .dispatch("new_download_group", async move { DownloadGroup::new(session).await })
            .await??;

        // Register the group (sync insertion, safe in any executor context)
        self.active_download_groups.lock()?.insert(group.id(), group.clone());

        Ok(group)
    }

    /// Create a new [`DownloadGroup`] from a **sync** (non-async) context.
    ///
    /// The returned [`DownloadGroup`] supports both the async [`finish`](DownloadGroup::finish)
    /// and blocking [`finish_blocking`](DownloadGroup::finish_blocking) methods.
    ///
    /// Returns `Err(XetError::Aborted)` if the session has been aborted.
    /// Returns `Err(XetError::WrongRuntimeMode)` if the session uses an external
    /// tokio runtime (from [`XetSessionBuilder::with_tokio_handle`] or tokio-detected
    /// [`XetSessionBuilder::build_async`]).
    ///
    /// # Panics
    ///
    /// Panics if called from within a **tokio** async runtime (tokio sets a thread-local
    /// context that `Handle::block_on` detects and panics on). Non-tokio executors (smol,
    /// async-std, `futures::executor`) do not set this context, so calling from those is
    /// safe — it blocks the executor thread until the task completes. Use
    /// [`new_download_group`](Self::new_download_group) from async contexts instead.
    pub fn new_download_group_blocking(&self) -> Result<DownloadGroup, XetError> {
        if matches!(self.runtime_mode, RuntimeMode::External) {
            return Err(XetError::wrong_mode(
                "new_download_group_blocking() cannot be called on a session using an \
                 external tokio runtime (with_tokio_handle() or tokio build_async()); \
                 use new_download_group().await instead",
            ));
        }
        {
            let state = self.state.lock()?;
            if matches!(*state, SessionState::Aborted) {
                return Err(XetError::Aborted);
            }
        }

        let group = self.runtime.external_run_async_task(DownloadGroup::new(self.clone()))??;
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
        assert!(matches!(err, XetError::Aborted));
    }

    #[test]
    // new_upload_commit_blocking on an aborted session returns Aborted.
    fn test_new_upload_commit_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.new_upload_commit_blocking().err().unwrap();
        assert!(matches!(err, XetError::Aborted));
    }

    #[test]
    // new_download_group_blocking on an aborted session returns Aborted.
    fn test_new_download_group_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.new_download_group_blocking().err().unwrap();
        assert!(matches!(err, XetError::Aborted));
    }

    #[test]
    // Aborting a session clears all registered upload commits.
    fn test_abort_clears_active_upload_commits() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _c1 = session.new_upload_commit_blocking().unwrap();
        let _c2 = session.new_upload_commit_blocking().unwrap();
        session.abort().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 0);
    }

    #[test]
    // Aborting a session clears all registered download groups.
    fn test_abort_clears_active_download_groups() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _g1 = session.new_download_group_blocking().unwrap();
        session.abort().unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 0);
    }

    // ── Registration ─────────────────────────────────────────────────────────

    #[test]
    // A new upload commit is registered in the session's active set.
    fn test_new_upload_commit_registers_in_session() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _commit = session.new_upload_commit_blocking().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
    }

    #[test]
    // A new download group is registered in the session's active set.
    fn test_new_download_group_registers_in_session() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _group = session.new_download_group_blocking().unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
    }

    // ── Deregistration ───────────────────────────────────────────────────────

    #[test]
    // finish_upload_commit removes only the specified commit, leaving others intact.
    fn test_finish_upload_commit_removes_only_that_commit() {
        let session = XetSessionBuilder::new().build().unwrap();
        let c1 = session.new_upload_commit_blocking().unwrap();
        let _c2 = session.new_upload_commit_blocking().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 2);
        session.finish_upload_commit(c1.id()).unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
    }

    #[test]
    // finish_download_group removes only the specified group, leaving others intact.
    fn test_finish_download_group_removes_only_that_group() {
        let session = XetSessionBuilder::new().build().unwrap();
        let g1 = session.new_download_group_blocking().unwrap();
        let _g2 = session.new_download_group_blocking().unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 2);
        session.finish_download_group(g1.id()).unwrap();
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
    }

    #[test]
    // finish_upload_commit on an unknown ID is a no-op (no error, no change).
    fn test_finish_upload_commit_with_unknown_id_is_noop() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _c1 = session.new_upload_commit_blocking().unwrap();
        let unknown_id = UniqueID::new();
        assert!(session.finish_upload_commit(unknown_id).is_ok());
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
    }

    // ── Async abort behavior ──────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // new_upload_commit / new_download_group on an aborted session both return Aborted.
    async fn test_async_new_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        session.abort().unwrap();
        let commit_err = session.new_upload_commit().await.err().unwrap();
        let group_err = session.new_download_group().await.err().unwrap();
        assert!(matches!(commit_err, XetError::Aborted));
        assert!(matches!(group_err, XetError::Aborted));
    }

    #[tokio::test(flavor = "multi_thread")]
    // Aborting a session clears all active upload commits and download groups.
    async fn test_async_abort_clears_active_commits_and_groups() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let _c1 = session.new_upload_commit().await.unwrap();
        let _c2 = session.new_upload_commit().await.unwrap();
        let _g1 = session.new_download_group().await.unwrap();
        session.abort().unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 0);
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 0);
    }

    // ── Async registration ────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // A new upload commit and a new download group are each registered in the
    // session's active set, and concurrent creation registers both.
    async fn test_async_new_registers_in_session() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        let _commit = session.new_upload_commit().await.unwrap();
        let _group = session.new_download_group().await.unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
        assert_eq!(session.active_download_groups.lock().unwrap().len(), 1);
    }

    // ── Async deregistration ──────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // Finishing one upload commit / download group removes only that one,
    // leaving the other still registered.
    async fn test_async_finish_removes_only_that_item() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
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

    // ── handle_meets_session_requirements ────────────────────────────────────

    #[test]
    // A multi-thread runtime with enable_all() meets all requirements.
    fn test_handle_multi_thread_all_features_returns_true() {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        assert!(handle_meets_session_requirements(rt.handle()));
    }

    #[test]
    #[cfg(not(target_family = "wasm"))]
    // A current_thread runtime is rejected even when enable_all() is set.
    fn test_handle_current_thread_returns_false() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        assert!(!handle_meets_session_requirements(rt.handle()));
    }

    #[test]
    // A multi-thread runtime with no drivers enabled returns false.
    fn test_handle_without_any_driver_returns_false() {
        let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        assert!(!handle_meets_session_requirements(rt.handle()));
    }

    #[test]
    // A multi-thread runtime with only enable_time() is missing the IO driver.
    fn test_handle_without_io_driver_returns_false() {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_time().build().unwrap();
        assert!(!handle_meets_session_requirements(rt.handle()));
    }

    #[test]
    // A multi-thread runtime with only enable_io() is missing the time driver.
    fn test_handle_without_time_driver_returns_false() {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_io().build().unwrap();
        assert!(!handle_meets_session_requirements(rt.handle()));
    }

    // ── External-mode _blocking guard ────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // new_upload_commit_blocking returns WrongRuntimeMode on an External-mode session.
    async fn test_new_upload_commit_blocking_errors_in_external_mode() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        assert_eq!(session.runtime_mode, RuntimeMode::External);
        let err = session.new_upload_commit_blocking().err().unwrap();
        assert!(matches!(err, XetError::WrongRuntimeMode(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    // new_download_group_blocking returns WrongRuntimeMode on an External-mode session.
    async fn test_new_download_group_blocking_errors_in_external_mode() {
        let session = XetSessionBuilder::new().build_async().await.unwrap();
        assert_eq!(session.runtime_mode, RuntimeMode::External);
        let err = session.new_download_group_blocking().err().unwrap();
        assert!(matches!(err, XetError::WrongRuntimeMode(_)));
    }

    // ── Owned-mode _blocking panic guard ─────────────────────────────────────

    #[test]
    // new_upload_commit_blocking panics when called from within a tokio runtime on an
    // Owned-mode session: external_run_async_task calls handle.block_on(), which panics
    // because tokio sets a thread-local runtime context that it detects and rejects.
    fn test_new_upload_commit_blocking_panics_in_async_context() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime_mode, RuntimeMode::Owned);
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async { session.new_upload_commit_blocking() })
        }));
        assert!(result.is_err(), "new_upload_commit_blocking() must panic when called from async");
    }

    #[test]
    // new_download_group_blocking panics when called from within a tokio runtime on an
    // Owned-mode session: same mechanism as the upload variant above.
    fn test_new_download_group_blocking_panics_in_async_context() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime_mode, RuntimeMode::Owned);
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async { session.new_download_group_blocking() })
        }));
        assert!(result.is_err(), "new_download_group_blocking() must panic when called from async");
    }
}
