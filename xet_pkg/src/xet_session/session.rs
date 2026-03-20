//! XetSession - manages runtime and configuration

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use http::HeaderMap;
use tracing::info;
use xet_client::cas_client::auth::TokenRefresher;
use xet_data::processing::{FileDownloadSession, XetFileInfo};
use xet_data::progress_tracking::UniqueID;
use xet_runtime::config::XetConfig;
use xet_runtime::core::XetRuntime;

use super::common::create_translator_config;
use super::download_streams::{XetDownloadStream, XetUnorderedDownloadStream};
use super::errors::SessionError;
use super::file_download_group::FileDownloadGroup;
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
    pub(super) active_file_download_groups: Mutex<HashMap<UniqueID, FileDownloadGroup>>,

    // Lazily-initialized download session for streaming downloads (no group-level progress).
    streaming_download_session: tokio::sync::OnceCell<Arc<FileDownloadSession>>,

    // Session state
    state: Mutex<SessionState>,
    pub(super) id: UniqueID,
}

/// Builder for [`XetSession`].
///
/// All fields are optional; call [`build`](XetSessionBuilder::build) when done.
///
/// [`build`](Self::build) auto-detects a suitable current tokio handle when present,
/// or creates an owned runtime (see [`XetSessionBuilder::with_tokio_handle`]).
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
    /// methods (`new_upload_commit`, `new_file_download_group`) may be called; `_blocking` variants
    /// will return [`SessionError::WrongRuntimeMode`].
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
    pub fn build(self) -> Result<XetSession, SessionError> {
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
/// [`FileDownloadGroup`]s created from it.
///
/// # Cloning
///
/// Cloning is cheap — it simply increments an atomic reference count.
/// All clones share the same runtime and credentials.
///
/// # Lifecycle
///
/// 1. Create a session with [`XetSessionBuilder`].
/// 2. Create one or more [`UploadCommit`]s / [`FileDownloadGroup`]s.
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
                active_file_download_groups: Mutex::new(HashMap::new()),
                streaming_download_session: tokio::sync::OnceCell::new(),
                state: Mutex::new(SessionState::Alive),
                id: UniqueID::new(),
            }),
        }
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

        let session = self.clone();
        let commit = self
            .runtime
            .bridge_async("new_upload_commit", async move { UploadCommit::new(session).await })
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
    /// Returns `Err(SessionError::Aborted)` if the session has been aborted.
    /// Returns `Err(SessionError::WrongRuntimeMode)` if the session uses an external
    /// tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if called from within a **tokio** async runtime (tokio sets a thread-local
    /// context that `Handle::block_on` detects and panics on). Non-tokio executors (smol,
    /// async-std, `futures::executor`) do not set this context, so calling from those is
    /// safe — it blocks the executor thread until the task completes. Use
    /// [`new_upload_commit`](Self::new_upload_commit) from async contexts instead.
    pub fn new_upload_commit_blocking(&self) -> Result<UploadCommit, SessionError> {
        {
            let state = self.state.lock()?;
            if matches!(*state, SessionState::Aborted) {
                return Err(SessionError::Aborted);
            }
        }

        let commit = self.runtime.bridge_sync(UploadCommit::new(self.clone()))??;
        self.active_upload_commits.lock()?.insert(commit.id(), commit.clone());
        Ok(commit)
    }

    /// Create a new [`FileDownloadGroup`] that groups related file downloads.
    ///
    /// Returns `Err(SessionError::Aborted)` if the session has been aborted.
    ///
    /// # Note
    ///
    /// This is an `async fn` and must be `.await`ed. For sync Rust or Python (PyO3) callers,
    /// use [`new_file_download_group_blocking`](Self::new_file_download_group_blocking).
    pub async fn new_file_download_group(&self) -> Result<FileDownloadGroup, SessionError> {
        // Check state before the async init; drop the guard so it is not held across .await.
        {
            let state = self.state.lock()?;
            if matches!(*state, SessionState::Aborted) {
                return Err(SessionError::Aborted);
            }
        }

        let session = self.clone();
        let group = self
            .runtime
            .bridge_async("new_file_download_group", async move { FileDownloadGroup::new(session).await })
            .await??;

        // Register the group (sync insertion, safe in any executor context)
        self.active_file_download_groups.lock()?.insert(group.id(), group.clone());

        Ok(group)
    }

    /// Create a new [`FileDownloadGroup`] from a **sync** (non-async) context.
    ///
    /// The returned [`FileDownloadGroup`] supports both the async [`finish`](FileDownloadGroup::finish)
    /// and blocking [`finish_blocking`](FileDownloadGroup::finish_blocking) methods.
    ///
    /// Returns `Err(SessionError::Aborted)` if the session has been aborted.
    /// Returns `Err(SessionError::WrongRuntimeMode)` if the session uses an external
    /// tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if called from within a **tokio** async runtime (tokio sets a thread-local
    /// context that `Handle::block_on` detects and panics on). Non-tokio executors (smol,
    /// async-std, `futures::executor`) do not set this context, so calling from those is
    /// safe — it blocks the executor thread until the task completes. Use
    /// [`new_file_download_group`](Self::new_file_download_group) from async contexts instead.
    pub fn new_file_download_group_blocking(&self) -> Result<FileDownloadGroup, SessionError> {
        {
            let state = self.state.lock()?;
            if matches!(*state, SessionState::Aborted) {
                return Err(SessionError::Aborted);
            }
        }

        let group = self.runtime.bridge_sync(FileDownloadGroup::new(self.clone()))??;
        self.active_file_download_groups.lock()?.insert(group.id(), group.clone());
        Ok(group)
    }

    /// Initialise (or return the cached) [`FileDownloadSession`] used for
    /// streaming downloads. The session is created lazily on the first call
    /// with no group-level progress tracking.
    async fn get_or_init_streaming_session(&self) -> Result<Arc<FileDownloadSession>, SessionError> {
        self.streaming_download_session
            .get_or_try_init(|| async {
                let config = create_translator_config(self)?;
                let session = FileDownloadSession::new(Arc::new(config)).await?;
                Ok::<_, SessionError>(session)
            })
            .await
            .cloned()
    }

    /// Create a [`XetDownloadStream`] for the given file, optionally
    /// restricted to a byte range.
    ///
    /// The returned stream yields data chunks as they are reconstructed,
    /// with built-in progress tracking via
    /// [`get_progress`](XetDownloadStream::get_progress).
    /// The reconstruction task is spawned on the session's runtime but
    /// paused until [`start`](XetDownloadStream::start) is called (or the
    /// first [`next`](XetDownloadStream::next) /
    /// [`blocking_next`](XetDownloadStream::blocking_next)).  Because the
    /// spawn happens during creation, `start()` and `next()` work from any
    /// executor (tokio, smol, async-std, futures).
    ///
    /// If `range` is `Some`, only the specified byte range of the file is
    /// reconstructed.
    ///
    /// The stream is independent of any [`FileDownloadGroup`] and is not
    /// tracked by the session — the caller is responsible for consuming
    /// or cancelling it.
    ///
    /// Returns `Err(SessionError::Aborted)` if the session has been aborted.
    pub async fn download_stream(
        &self,
        file_info: XetFileInfo,
        range: Option<Range<u64>>,
    ) -> Result<XetDownloadStream, SessionError> {
        self.check_alive()?;

        let session = self.clone();
        self.runtime
            .bridge_async("download_stream", async move {
                let dl_session = session.get_or_init_streaming_session().await?;
                let (id, stream) = dl_session.download_stream(&file_info, range).await?;
                Ok(XetDownloadStream::new(stream, dl_session, id))
            })
            .await?
    }

    /// Blocking version of [`download_stream`](Self::download_stream).
    ///
    /// The reconstruction task is spawned on the session's runtime but
    /// paused until [`start`](XetDownloadStream::start) is called (or the
    /// first [`blocking_next`](XetDownloadStream::blocking_next)).  No
    /// tokio runtime context is required on the calling thread after this
    /// method returns.
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn download_stream_blocking(
        &self,
        file_info: XetFileInfo,
        range: Option<Range<u64>>,
    ) -> Result<XetDownloadStream, SessionError> {
        self.check_alive()?;

        let session = self.clone();
        self.runtime.bridge_sync(async move {
            let dl_session = session.get_or_init_streaming_session().await?;
            let (id, stream) = dl_session.download_stream(&file_info, range).await?;
            Ok(XetDownloadStream::new(stream, dl_session, id))
        })?
    }

    /// Create an [`XetUnorderedDownloadStream`] for the given file,
    /// optionally restricted to a byte range.
    ///
    /// The returned stream yields `(offset, Bytes)` chunks in whatever
    /// order they complete, with built-in progress tracking via
    /// [`get_progress`](XetUnorderedDownloadStream::get_progress).
    ///
    /// If `range` is `Some`, only the specified byte range of the file is
    /// reconstructed.
    ///
    /// Can be awaited from any async executor (tokio, smol, async-std,
    /// futures).
    ///
    /// The stream is independent of any [`FileDownloadGroup`] and is not
    /// tracked by the session — the caller is responsible for consuming
    /// or cancelling it.
    ///
    /// Returns `Err(SessionError::Aborted)` if the session has been aborted.
    pub async fn download_unordered_stream(
        &self,
        file_info: XetFileInfo,
        range: Option<Range<u64>>,
    ) -> Result<XetUnorderedDownloadStream, SessionError> {
        self.check_alive()?;

        let session = self.clone();
        self.runtime
            .bridge_async("download_unordered_stream", async move {
                let dl_session = session.get_or_init_streaming_session().await?;
                let (id, stream) = dl_session.download_unordered_stream(&file_info, range).await?;
                Ok(XetUnorderedDownloadStream::new(stream, dl_session, id))
            })
            .await?
    }

    /// Blocking version of [`download_unordered_stream`](Self::download_unordered_stream).
    ///
    /// The reconstruction task is spawned on the session's runtime but
    /// paused until [`start`](XetUnorderedDownloadStream::start) is called
    /// (or the first [`blocking_next`](XetUnorderedDownloadStream::blocking_next)).
    /// No tokio runtime context is required on the calling thread after
    /// this method returns.
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn download_unordered_stream_blocking(
        &self,
        file_info: XetFileInfo,
        range: Option<Range<u64>>,
    ) -> Result<XetUnorderedDownloadStream, SessionError> {
        self.check_alive()?;

        let session = self.clone();
        self.runtime.bridge_sync(async move {
            let dl_session = session.get_or_init_streaming_session().await?;
            let (id, stream) = dl_session.download_unordered_stream(&file_info, range).await?;
            Ok(XetUnorderedDownloadStream::new(stream, dl_session, id))
        })?
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
        if let Some(streaming_download_session) = self.streaming_download_session.get() {
            streaming_download_session.abort_active_streams();
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
    // new_upload_commit_blocking on an aborted session returns Aborted.
    fn test_new_upload_commit_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.new_upload_commit_blocking().err().unwrap();
        assert!(matches!(err, SessionError::Aborted));
    }

    #[test]
    // new_file_download_group_blocking on an aborted session returns Aborted.
    fn test_new_file_download_group_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.new_file_download_group_blocking().err().unwrap();
        assert!(matches!(err, SessionError::Aborted));
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
    fn test_abort_clears_active_file_download_groups() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _g1 = session.new_file_download_group_blocking().unwrap();
        session.abort().unwrap();
        assert_eq!(session.active_file_download_groups.lock().unwrap().len(), 0);
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
    fn test_new_file_download_group_registers_in_session() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _group = session.new_file_download_group_blocking().unwrap();
        assert_eq!(session.active_file_download_groups.lock().unwrap().len(), 1);
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
    // finish_file_download_group removes only the specified group, leaving others intact.
    fn test_finish_file_download_group_removes_only_that_group() {
        let session = XetSessionBuilder::new().build().unwrap();
        let g1 = session.new_file_download_group_blocking().unwrap();
        let _g2 = session.new_file_download_group_blocking().unwrap();
        assert_eq!(session.active_file_download_groups.lock().unwrap().len(), 2);
        session.finish_file_download_group(g1.id()).unwrap();
        assert_eq!(session.active_file_download_groups.lock().unwrap().len(), 1);
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
    // new_upload_commit / new_file_download_group on an aborted session both return Aborted.
    async fn test_async_new_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let commit_err = session.new_upload_commit().await.err().unwrap();
        let group_err = session.new_file_download_group().await.err().unwrap();
        assert!(matches!(commit_err, SessionError::Aborted));
        assert!(matches!(group_err, SessionError::Aborted));
    }

    #[tokio::test(flavor = "multi_thread")]
    // Aborting a session clears all active upload commits and download groups.
    async fn test_async_abort_clears_active_commits_and_groups() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _c1 = session.new_upload_commit().await.unwrap();
        let _c2 = session.new_upload_commit().await.unwrap();
        let _g1 = session.new_file_download_group().await.unwrap();
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
        let _commit = session.new_upload_commit().await.unwrap();
        let _group = session.new_file_download_group().await.unwrap();
        assert_eq!(session.active_upload_commits.lock().unwrap().len(), 1);
        assert_eq!(session.active_file_download_groups.lock().unwrap().len(), 1);
    }

    // ── Async deregistration ──────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // Finishing one upload commit / download group removes only that one,
    // leaving the other still registered.
    async fn test_async_finish_removes_only_that_item() {
        let session = XetSessionBuilder::new().build().unwrap();
        let c1 = session.new_upload_commit().await.unwrap();
        let _c2 = session.new_upload_commit().await.unwrap();
        let g1 = session.new_file_download_group().await.unwrap();
        let _g2 = session.new_file_download_group().await.unwrap();
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
    // new_upload_commit_blocking returns WrongRuntimeMode on an External-mode session.
    async fn test_new_upload_commit_blocking_errors_in_external_mode() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime.mode(), RuntimeMode::External);
        let err = session.new_upload_commit_blocking().err().unwrap();
        assert!(matches!(err, SessionError::WrongRuntimeMode(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    // new_file_download_group_blocking returns WrongRuntimeMode on an External-mode session.
    async fn test_new_file_download_group_blocking_errors_in_external_mode() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime.mode(), RuntimeMode::External);
        let err = session.new_file_download_group_blocking().err().unwrap();
        assert!(matches!(err, SessionError::WrongRuntimeMode(_)));
    }

    // ── Owned-mode _blocking panic guard ─────────────────────────────────────

    #[test]
    // new_upload_commit_blocking panics when called from within a tokio runtime on an
    // Owned-mode session: bridge_sync uses handle.block_on(), which panics because tokio
    // sets a thread-local runtime context that it detects and rejects.
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
    // new_file_download_group_blocking panics when called from within a tokio runtime on an
    // Owned-mode session: same mechanism as the upload variant above.
    fn test_new_file_download_group_blocking_panics_in_async_context() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime.mode(), RuntimeMode::Owned);
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async { session.new_file_download_group_blocking() })
        }));
        assert!(result.is_err(), "new_file_download_group_blocking() must panic when called from async");
    }

    // ── Streaming download ──────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // download_stream on an aborted session returns Aborted.
    async fn test_download_stream_on_aborted_session_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let result = session
            .download_stream(
                XetFileInfo {
                    hash: "abc123".to_string(),
                    file_size: Some(1024),
                    sha256: None,
                },
                None,
            )
            .await;
        assert!(matches!(result, Err(SessionError::Aborted)));
    }

    #[test]
    // download_stream_blocking on an aborted session returns Aborted.
    fn test_download_stream_blocking_on_aborted_session_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let result = session.download_stream_blocking(
            XetFileInfo {
                hash: "abc123".to_string(),
                file_size: Some(1024),
                sha256: None,
            },
            None,
        );
        assert!(matches!(result, Err(SessionError::Aborted)));
    }

    #[tokio::test(flavor = "multi_thread")]
    // download_stream_blocking returns WrongRuntimeMode on an External-mode session.
    async fn test_download_stream_blocking_errors_in_external_mode() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.runtime.mode(), RuntimeMode::External);
        let result = session.download_stream_blocking(
            XetFileInfo {
                hash: "abc123".to_string(),
                file_size: Some(1024),
                sha256: None,
            },
            None,
        );
        assert!(matches!(result, Err(SessionError::WrongRuntimeMode(_))));
    }

    // ── Streaming download round-trip tests ─────────────────────────────────

    use tempfile::{TempDir, tempdir};
    use xet_data::processing::Sha256Policy;

    async fn local_session(temp: &TempDir) -> Result<XetSession, Box<dyn std::error::Error>> {
        let cas_path = temp.path().join("cas");
        Ok(XetSessionBuilder::new()
            .with_endpoint(format!("local://{}", cas_path.display()))
            .build()?)
    }

    fn local_session_sync(temp: &TempDir) -> Result<XetSession, Box<dyn std::error::Error>> {
        let cas_path = temp.path().join("cas");
        Ok(XetSessionBuilder::new()
            .with_endpoint(format!("local://{}", cas_path.display()))
            .build()?)
    }

    async fn upload_bytes(
        session: &XetSession,
        data: &[u8],
        name: &str,
    ) -> Result<XetFileInfo, Box<dyn std::error::Error>> {
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

    fn upload_bytes_blocking(
        session: &XetSession,
        data: &[u8],
        name: &str,
    ) -> Result<XetFileInfo, Box<dyn std::error::Error>> {
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

    #[tokio::test(flavor = "multi_thread")]
    // Async streaming download round-trip: upload, stream, verify content.
    async fn test_download_stream_round_trip() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let original = b"Hello, streaming download!";
        let file_info = upload_bytes(&session, original, "stream.bin").await.unwrap();

        let mut stream = session.download_stream(file_info, None).await.unwrap();
        let mut collected = Vec::new();
        while let Some(chunk) = stream.next().await.unwrap() {
            collected.extend_from_slice(&chunk);
        }
        assert_eq!(collected, original);
    }

    #[test]
    // Blocking streaming download round-trip: upload, stream, verify content.
    fn test_download_stream_blocking_round_trip() {
        let temp = tempdir().unwrap();
        let session = local_session_sync(&temp).unwrap();
        let original = b"Hello, blocking streaming download!";
        let file_info = upload_bytes_blocking(&session, original, "stream.bin").unwrap();

        let mut stream = session.download_stream_blocking(file_info, None).unwrap();

        let mut collected = Vec::new();
        while let Some(chunk) = stream.blocking_next().unwrap() {
            collected.extend_from_slice(&chunk);
        }
        assert_eq!(collected, original);
    }

    #[tokio::test(flavor = "multi_thread")]
    // get_progress() reports correct totals after consuming the stream.
    async fn test_download_stream_progress_reports_completion() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let original = b"progress tracking test data for streaming";
        let file_info = upload_bytes(&session, original, "progress.bin").await.unwrap();

        let mut stream = session.download_stream(file_info, None).await.unwrap();
        let initial = stream.get_progress();
        assert_eq!(initial.total_bytes, original.len() as u64);
        assert_eq!(initial.bytes_completed, 0);

        let mut collected = Vec::new();
        while let Some(chunk) = stream.next().await.unwrap() {
            collected.extend_from_slice(&chunk);
        }
        assert_eq!(collected, original);

        let final_progress = stream.get_progress();
        assert_eq!(final_progress.total_bytes, original.len() as u64);
        assert_eq!(final_progress.bytes_completed, original.len() as u64);
    }

    #[test]
    // get_progress() works correctly in blocking mode.
    fn test_download_stream_blocking_progress_reports_completion() {
        let temp = tempdir().unwrap();
        let session = local_session_sync(&temp).unwrap();
        let original = b"blocking progress tracking test data";
        let file_info = upload_bytes_blocking(&session, original, "progress.bin").unwrap();

        let mut stream = session.download_stream_blocking(file_info, None).unwrap();

        let mut collected = Vec::new();
        while let Some(chunk) = stream.blocking_next().unwrap() {
            collected.extend_from_slice(&chunk);
        }
        assert_eq!(collected, original);

        let final_progress = stream.get_progress();
        assert_eq!(final_progress.total_bytes, original.len() as u64);
        assert_eq!(final_progress.bytes_completed, original.len() as u64);
    }

    #[tokio::test(flavor = "multi_thread")]
    // Multiple sequential streaming downloads reuse the lazy FileDownloadSession.
    async fn test_download_stream_multiple_sequential() {
        let temp = tempdir().unwrap();
        let session = local_session(&temp).await.unwrap();
        let data_a = b"first stream payload";
        let data_b = b"second stream payload";
        let info_a = upload_bytes(&session, data_a, "a.bin").await.unwrap();
        let info_b = upload_bytes(&session, data_b, "b.bin").await.unwrap();

        let mut stream_a = session.download_stream(info_a, None).await.unwrap();
        let mut collected_a = Vec::new();
        while let Some(chunk) = stream_a.next().await.unwrap() {
            collected_a.extend_from_slice(&chunk);
        }
        assert_eq!(collected_a, data_a);

        let mut stream_b = session.download_stream(info_b, None).await.unwrap();
        let mut collected_b = Vec::new();
        while let Some(chunk) = stream_b.next().await.unwrap() {
            collected_b.extend_from_slice(&chunk);
        }
        assert_eq!(collected_b, data_b);
    }
}
