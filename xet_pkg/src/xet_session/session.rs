//! XetSession - manages runtime and configuration

#[cfg(not(target_family = "wasm"))]
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(not(target_family = "wasm"))]
use std::sync::{Mutex, Weak};

use tracing::info;
use uuid::Uuid;
use xet_runtime::config::XetConfig;
use xet_runtime::core::XetContext;
#[cfg(feature = "fd-track")]
use xet_runtime::fd_diagnostics::{report_fd_count, track_fd_scope};
use xet_runtime::utils::UniqueId;

#[cfg(not(target_family = "wasm"))]
use super::download_stream_group::{
    XetDownloadStreamGroup, XetDownloadStreamGroupBuilder, XetDownloadStreamGroupInner,
};
use super::errors::SessionError;
#[cfg(not(target_family = "wasm"))]
use super::file_download_group::XetFileDownloadGroupBuilder;
use super::task_runtime::{TaskRuntime, XetTaskState};
#[cfg(not(target_family = "wasm"))]
use super::upload_commit::XetUploadCommitBuilder;

/// All shared state for a session.
/// Lives behind `Arc<XetSessionInner>` — do not use this type directly.
#[doc(hidden)]
pub struct XetSessionInner {
    pub(super) ctx: XetContext,

    // Root of the cancellation tree. Child commits/groups create child TaskRuntimes via
    // task_runtime.child(), which links their cancellation tokens to this root. Calling
    // cancel_subtree() here propagates cancellation to all descendants automatically.
    //
    // IMPORTANT: Do NOT add maps of active commits or download groups to this struct.
    // Storing strong (or even Weak) references to child objects here creates a circular
    // dependency (session → child → session) that prevents cleanup and leaks file handles.
    // All abort propagation is handled through the TaskRuntime cancellation token tree.
    pub(super) task_runtime: Arc<TaskRuntime>,

    // Weak references so that dropping all user-held XetDownloadStreamGroup clones frees the group
    // immediately, without needing an explicit finalization call. abort() upgrades live weak refs
    // to cancel active streams.
    #[cfg(not(target_family = "wasm"))]
    pub(super) active_download_stream_groups: Mutex<HashMap<UniqueId, Weak<XetDownloadStreamGroupInner>>>,

    // "id" is used to identify a group of activities on our server, and so needs to be globally unique
    pub(super) id: Uuid,
}

/// Builder for [`XetSession`].
///
/// All fields are optional; call [`build`](XetSessionBuilder::build) when done.
///
/// ## Runtime detection
///
/// [`build`](Self::build) auto-detects a suitable tokio runtime:
///
/// - **Inside `#[tokio::main]` or an existing tokio multi-thread runtime** and the runtime meets the requirements — the
///   session wraps the caller's handle; no second thread pool is created.  Both async and blocking methods work.
/// - **Outside any runtime** — an owned multi-thread runtime is created internally. Blocking methods (`_blocking`
///   suffix) work from any thread; async methods work via an internal bridge.
/// - **Explicit handle** — call [`with_tokio_handle`](Self::with_tokio_handle) to supply a handle directly.  If it
///   doesn't meet requirements (multi-thread, time + IO drivers), it is silently ignored and an owned runtime is
///   created instead.
///
/// ## Authentication
///
/// Auth tokens are configured per-operation on the builder returned by each factory method, not on the
/// session itself.  This lets uploads and downloads use different access-level
/// tokens from the same session:
///
/// ```rust,no_run
/// # use http::HeaderMap;
/// # use xet::xet_session::XetSessionBuilder;
/// let session = XetSessionBuilder::new().build()?;
///
/// // Upload token (write access)
/// let mut upload_headers = HeaderMap::new();
/// upload_headers.insert("Authorization", "Bearer hub-write-token".parse().unwrap());
/// let commit = session
///     .new_upload_commit()?
///     .with_endpoint("https://cas.example.com")
///     .with_token_info("CAS_WRITE_JWT", 900)
///     .with_token_refresh_url("https://huggingface.co/api/repos/token/write", upload_headers)
///     .build_blocking()?;
///
/// // File download token (read access)
/// let mut dl_headers = HeaderMap::new();
/// dl_headers.insert("Authorization", "Bearer hub-read-token".parse().unwrap());
/// let group = session
///     .new_file_download_group()?
///     .with_token_info("CAS_READ_JWT", 900)
///     .with_token_refresh_url("https://huggingface.co/api/repos/token/read", dl_headers)
///     .build_blocking()?;
///
/// // Streaming download token (read access, different group/pool)
/// let mut stream_headers = HeaderMap::new();
/// stream_headers.insert("Authorization", "Bearer hub-read-token".parse().unwrap());
/// let stream_group = session
///     .new_download_stream_group()?
///     .with_token_info("CAS_READ_JWT", 900)
///     .with_token_refresh_url("https://huggingface.co/api/repos/token/read", stream_headers)
///     .build_blocking()?;
/// # Ok::<(), xet::xet_session::SessionError>(())
/// ```
///
/// ## `XetConfig`
///
/// For most use cases, [`new`](Self::new) with the default [`XetConfig`] is
/// sufficient.  Use [`new_with_config`](Self::new_with_config) when you need to
/// override runtime settings such as cache directories or concurrency limits.
pub struct XetSessionBuilder {
    config: XetConfig,
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
            tokio_handle: None,
        }
    }

    /// Create a builder pre-populated with the given [`XetConfig`].
    pub fn new_with_config(config: XetConfig) -> Self {
        Self {
            config,
            tokio_handle: None,
        }
    }

    /// Attach to an existing tokio runtime handle.
    ///
    /// If the handle meets runtime requirements (multi-thread flavor, time driver, IO driver),
    /// the session will wrap it — no second thread pool is created. Only async
    /// methods (`new_upload_commit`, `new_file_download_group`) may be called; `_blocking` variants
    /// return [`SessionError::WrongRuntimeMode`] from `bridge_sync` (external runtime cannot run sync bridge).
    ///
    /// If the handle does **not** meet requirements (e.g. `current_thread` flavor or missing
    /// drivers), it is silently ignored and [`build`](Self::build) will fall back to creating
    /// an owned thread pool instead.
    ///
    /// Handles can be shared by multiple sessions. Each session gets its own
    /// [`XetContext`] (`config` + `common`), while the underlying runtime
    /// may be shared.
    #[cfg(not(target_family = "wasm"))]
    pub fn with_tokio_handle(self, handle: tokio::runtime::Handle) -> Self {
        let accept = XetContext::handle_meets_requirements(&handle);
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
    /// Threadpool selection order:
    /// 1. Reuse the current owned runtime from thread-local storage, when present.
    /// 2. Otherwise, use a provided tokio handle (or auto-detected current handle) if valid.
    /// 3. Otherwise, create a new owned thread pool.
    ///
    /// Each build creates a fresh [`XetContext`] around the selected runtime, so sessions
    /// can share the same execution backend while keeping independent config and common state.
    #[cfg(not(target_family = "wasm"))]
    pub fn build(self) -> Result<XetSession, SessionError> {
        #[cfg(feature = "fd-track")]
        let _fd_scope = track_fd_scope("XetSessionBuilder::build");

        let ctx = if let Some(h) = self.tokio_handle {
            info!("XetSession using explicitly provided tokio handle");
            XetContext::from_external(h, self.config)
        } else {
            XetContext::with_config(self.config)?
        };

        let session = XetSession::new(ctx);
        info!("Session created, session_id={}", session.inner.id);
        #[cfg(feature = "fd-track")]
        report_fd_count("XetSessionBuilder::build complete");
        Ok(session)
    }

    #[cfg(target_family = "wasm")]
    pub fn build(self) -> Result<XetSession, SessionError> {
        let ctx = XetContext::from_external(tokio::runtime::Handle::current(), self.config);
        let session = XetSession::new(ctx);
        info!("Session created, session_id={}", session.inner.id);
        Ok(session)
    }
}

/// Handle for managing file uploads and downloads.
///
/// `XetSession` is the top-level entry point for the xet-session API.  It
/// owns a [`XetContext`] (configuration and tokio thread pool). CAS endpoints,
/// custom headers, and auth tokens are configured per-operation on the builder
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
///    - uploads via [`new_upload_commit`](Self::new_upload_commit) → [`XetUploadCommitBuilder`] → [`XetUploadCommit`]
///    - file downloads via [`new_file_download_group`](Self::new_file_download_group) → [`XetFileDownloadGroupBuilder`]
///      → [`XetFileDownloadGroup`]
///    - streaming downloads via [`new_download_stream_group`](Self::new_download_stream_group) →
///      [`XetDownloadStreamGroupBuilder`] → [`XetDownloadStreamGroup`]
/// 3. For an emergency stop, call [`XetSession::abort`].
#[derive(Clone)]
pub struct XetSession {
    pub(super) inner: Arc<XetSessionInner>,
}

impl XetSession {
    /// Low-level constructor used by [`XetSessionBuilder::build`].
    fn new(ctx: XetContext) -> Self {
        let task_runtime = TaskRuntime::new_root(ctx.runtime.clone());
        Self {
            inner: Arc::new(XetSessionInner {
                ctx,
                task_runtime,
                #[cfg(not(target_family = "wasm"))]
                active_download_stream_groups: Mutex::new(HashMap::new()),
                id: Uuid::now_v7(),
            }),
        }
    }

    /// Create a [`XetUploadCommitBuilder`] for configuring and constructing an upload commit.
    ///
    /// Configure the builder with any combination of:
    /// - [`with_endpoint`](XetUploadCommitBuilder::with_endpoint) — CAS server URL (if omitted, resolved from the token
    ///   refresh response or the session default)
    /// - [`with_custom_headers`](XetUploadCommitBuilder::with_custom_headers) — extra HTTP headers forwarded with every
    ///   CAS request
    /// - [`with_token_info`](XetUploadCommitBuilder::with_token_info) — pre-seeded CAS token and expiry to skip the
    ///   initial refresh round-trip
    /// - [`with_token_refresh_url`](XetUploadCommitBuilder::with_token_refresh_url) — URL and auth headers for
    ///   refreshing the CAS token
    ///
    /// Then call [`build`](XetUploadCommitBuilder::build) (async) or
    /// [`build_blocking`](XetUploadCommitBuilder::build_blocking) (sync).
    ///
    /// Returns `Err(SessionError::UserCancelled)` if the session has been aborted.
    #[cfg(not(target_family = "wasm"))]
    pub fn new_upload_commit(&self) -> Result<XetUploadCommitBuilder, SessionError> {
        self.inner.task_runtime.check_state("new_upload_commit")?;
        #[cfg(feature = "fd-track")]
        report_fd_count("XetSession::new_upload_commit");
        Ok(XetUploadCommitBuilder::new(self.clone()))
    }

    /// Create a [`XetFileDownloadGroupBuilder`] for configuring and constructing a file download group.
    ///
    /// Configure the builder with any combination of:
    /// - [`with_endpoint`](XetFileDownloadGroupBuilder::with_endpoint) — CAS server URL (if omitted, resolved from the
    ///   token refresh response or the session default)
    /// - [`with_custom_headers`](XetFileDownloadGroupBuilder::with_custom_headers) — extra HTTP headers forwarded with
    ///   every CAS request
    /// - [`with_token_info`](XetFileDownloadGroupBuilder::with_token_info) — pre-seeded CAS token and expiry to skip
    ///   the initial refresh round-trip
    /// - [`with_token_refresh_url`](XetFileDownloadGroupBuilder::with_token_refresh_url) — URL and auth headers for
    ///   refreshing the CAS token
    ///
    /// Then call [`build`](XetFileDownloadGroupBuilder::build) (async) or
    /// [`build_blocking`](XetFileDownloadGroupBuilder::build_blocking) (sync).
    ///
    /// Returns `Err(SessionError::UserCancelled)` if the session has been aborted.
    #[cfg(not(target_family = "wasm"))]
    pub fn new_file_download_group(&self) -> Result<XetFileDownloadGroupBuilder, SessionError> {
        self.inner.task_runtime.check_state("new_file_download_group")?;
        #[cfg(feature = "fd-track")]
        report_fd_count("XetSession::new_file_download_group");
        Ok(XetFileDownloadGroupBuilder::new(self.clone()))
    }

    /// Create a [`XetDownloadStreamGroupBuilder`] for configuring and constructing a download stream group.
    ///
    /// Configure the builder with any combination of:
    /// - [`with_endpoint`](XetDownloadStreamGroupBuilder::with_endpoint) — CAS server URL (if omitted, resolved from
    ///   the token refresh response or the session default)
    /// - [`with_custom_headers`](XetDownloadStreamGroupBuilder::with_custom_headers) — extra HTTP headers forwarded
    ///   with every CAS request
    /// - [`with_token_info`](XetDownloadStreamGroupBuilder::with_token_info) — pre-seeded CAS token and expiry to skip
    ///   the initial refresh round-trip
    /// - [`with_token_refresh_url`](XetDownloadStreamGroupBuilder::with_token_refresh_url) — URL and auth headers for
    ///   refreshing the CAS token
    ///
    /// Then call [`build`](XetDownloadStreamGroupBuilder::build) (async) or
    /// [`build_blocking`](XetDownloadStreamGroupBuilder::build_blocking) (sync).
    ///
    /// Use the resulting [`XetDownloadStreamGroup`] to create individual streams via
    /// [`download_stream`](XetDownloadStreamGroup::download_stream) and
    /// [`download_unordered_stream`](XetDownloadStreamGroup::download_unordered_stream).
    ///
    /// Returns `Err(SessionError::UserCancelled)` if the session has been aborted.
    #[cfg(not(target_family = "wasm"))]
    pub fn new_download_stream_group(&self) -> Result<XetDownloadStreamGroupBuilder, SessionError> {
        self.inner.task_runtime.check_state("new_download_stream_group")?;
        #[cfg(feature = "fd-track")]
        report_fd_count("XetSession::new_download_stream_group");
        Ok(XetDownloadStreamGroupBuilder::new(self.clone()))
    }

    pub fn status(&self) -> Result<XetTaskState, SessionError> {
        self.inner.task_runtime.status()
    }

    /// Abort the session and cancel all currently running tasks.
    ///
    /// This does not shut down the underlying runtime. Use
    /// [`sigint_abort`](Self::sigint_abort) for SIGINT-style runtime teardown.
    pub fn abort(&self) -> Result<(), SessionError> {
        #[cfg(feature = "fd-track")]
        let _fd_scope = track_fd_scope(format!("XetSession::abort({})", self.inner.id));

        info!("Session abort, session_id={}", self.inner.id);
        self.inner.task_runtime.cancel_subtree()?;

        #[cfg(not(target_family = "wasm"))]
        {
            let active_download_stream_groups = std::mem::take(&mut *self.inner.active_download_stream_groups.lock()?);
            for (_id, weak_group) in active_download_stream_groups {
                if let Some(inner) = weak_group.upgrade() {
                    inner.abort();
                }
            }
        }
        #[cfg(feature = "fd-track")]
        report_fd_count("XetSession::abort complete");
        Ok(())
    }

    /// SIGINT-style abort.
    ///
    /// Performs runtime SIGINT shutdown and clears session registrations.
    /// This does not call per-commit/group local abort hooks.
    #[cfg(not(target_family = "wasm"))]
    pub fn sigint_abort(&self) -> Result<(), SessionError> {
        #[cfg(feature = "fd-track")]
        let _fd_scope = track_fd_scope(format!("XetSession::sigint_abort({})", self.inner.id));

        info!("Session SIGINT abort, session_id={}", self.inner.id);
        self.inner.ctx.runtime.perform_sigint_shutdown();

        let active_download_stream_groups = std::mem::take(&mut *self.inner.active_download_stream_groups.lock()?);
        for (_id, weak_group) in active_download_stream_groups {
            if let Some(inner) = weak_group.upgrade() {
                inner.abort();
            }
        }

        #[cfg(feature = "fd-track")]
        report_fd_count("XetSession::sigint_abort complete");
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn check_alive(&self) -> Result<(), SessionError> {
        if self.inner.ctx.runtime.in_sigint_shutdown() {
            return Err(SessionError::KeyboardInterrupt);
        }
        self.inner.task_runtime.check_state("session")
    }

    #[cfg(not(target_family = "wasm"))]
    pub(super) fn register_download_stream_group(&self, group: &XetDownloadStreamGroup) -> Result<(), SessionError> {
        self.inner
            .active_download_stream_groups
            .lock()?
            .insert(group.id(), Arc::downgrade(&group.inner));
        Ok(())
    }

    pub fn id(&self) -> &Uuid {
        &self.inner.id
    }

    pub fn config(&self) -> &XetConfig {
        &self.inner.ctx.config
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use xet_data::processing::{Sha256Policy, XetFileInfo};
    use xet_runtime::core::{RuntimeMode, XetContext};

    use super::*;

    // ── Identity ─────────────────────────────────────────────────────────────

    #[test]
    // A clone refers to the same inner Arc, so their session IDs must match.
    fn test_session_clone_shares_state() {
        let s1 = XetSessionBuilder::new().build().unwrap();
        let s2 = s1.clone();
        assert_eq!(s1.inner.id, s2.inner.id);
    }

    #[test]
    // Two independently created sessions have distinct IDs.
    fn test_two_sessions_have_distinct_ids() {
        let s1 = XetSessionBuilder::new().build().unwrap();
        let s2 = XetSessionBuilder::new().build().unwrap();
        assert_ne!(s1.inner.id, s2.inner.id);
    }

    #[test]
    // Session ID is a UUIDv7, to guard future regressions.
    fn test_session_id_is_uuid_v7() {
        let s = XetSessionBuilder::new().build().unwrap();
        let parsed: uuid::Uuid = s.inner.id.to_string().parse().expect("session id must parse as Uuid");
        assert_eq!(parsed.get_version(), Some(uuid::Version::SortRand));
    }

    // ── Abort behavior ───────────────────────────────────────────────────────

    #[test]
    // After abort, check_alive returns UserCancelled.
    fn test_check_alive_after_abort() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.check_alive().unwrap_err();
        assert!(matches!(err, SessionError::UserCancelled(_)));
    }

    #[test]
    // After sigint_abort, check_alive returns KeyboardInterrupt.
    fn test_check_alive_after_sigint_abort() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.sigint_abort().unwrap();
        let err = session.check_alive().unwrap_err();
        assert!(matches!(err, SessionError::KeyboardInterrupt));
    }

    #[test]
    // new_upload_commit on an aborted session returns UserCancelled.
    fn test_new_upload_commit_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.new_upload_commit().err().unwrap();
        assert!(matches!(err, SessionError::UserCancelled(_)));
    }

    #[test]
    // new_file_download_group on an aborted session returns UserCancelled.
    fn test_new_file_download_group_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.new_file_download_group().err().unwrap();
        assert!(matches!(err, SessionError::UserCancelled(_)));
    }

    // ── Async abort behavior ──────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // new_upload_commit / new_file_download_group on an aborted session both return UserCancelled.
    async fn test_async_new_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let commit_err = session.new_upload_commit().err().unwrap();
        let group_err = session.new_file_download_group().err().unwrap();
        assert!(matches!(commit_err, SessionError::UserCancelled(_)));
        assert!(matches!(group_err, SessionError::UserCancelled(_)));
    }

    // ── XetContext::handle_meets_requirements ────────────────────────────────

    #[test]
    // A multi-thread runtime with enable_all() meets all requirements.
    fn test_handle_multi_thread_all_features_returns_true() {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        assert!(XetContext::handle_meets_requirements(rt.handle()));
    }

    #[test]
    #[cfg(not(target_family = "wasm"))]
    // A current_thread runtime is rejected even when enable_all() is set.
    fn test_handle_current_thread_returns_false() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        assert!(!XetContext::handle_meets_requirements(rt.handle()));
    }

    #[test]
    // A multi-thread runtime with no drivers enabled returns false.
    fn test_handle_without_any_driver_returns_false() {
        let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        assert!(!XetContext::handle_meets_requirements(rt.handle()));
    }

    #[test]
    // A multi-thread runtime with only enable_time() is missing the IO driver.
    fn test_handle_without_io_driver_returns_false() {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_time().build().unwrap();
        assert!(!XetContext::handle_meets_requirements(rt.handle()));
    }

    #[test]
    // A multi-thread runtime with only enable_io() is missing the time driver.
    fn test_handle_without_time_driver_returns_false() {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_io().build().unwrap();
        assert!(!XetContext::handle_meets_requirements(rt.handle()));
    }

    // ── External-mode _blocking guard ────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    // build_blocking on an External-mode session returns WrongRuntimeMode.
    async fn test_new_upload_commit_blocking_errors_in_external_mode() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.inner.ctx.runtime.mode(), RuntimeMode::External);
        let err = session.new_upload_commit().unwrap().build_blocking().err().unwrap();
        assert!(matches!(err, SessionError::WrongRuntimeMode(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    // build_blocking on an External-mode session returns WrongRuntimeMode.
    async fn test_new_file_download_group_blocking_errors_in_external_mode() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.inner.ctx.runtime.mode(), RuntimeMode::External);
        let err = session.new_file_download_group().unwrap().build_blocking().err().unwrap();
        assert!(matches!(err, SessionError::WrongRuntimeMode(_)));
    }

    // ── Owned-mode _blocking panic guard ─────────────────────────────────────

    #[test]
    // build_blocking panics when called from within a tokio runtime on an Owned-mode session.
    fn test_new_upload_commit_blocking_panics_in_async_context() {
        let session = XetSessionBuilder::new().build().unwrap();
        assert_eq!(session.inner.ctx.runtime.mode(), RuntimeMode::Owned);
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
        assert_eq!(session.inner.ctx.runtime.mode(), RuntimeMode::Owned);
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async { session.new_file_download_group().unwrap().build_blocking() })
        }));
        assert!(result.is_err(), "build_blocking() must panic when called from async");
    }

    #[test]
    // new_download_stream_group after abort returns UserCancelled.
    fn test_new_download_stream_group_after_abort_returns_aborted() {
        let session = XetSessionBuilder::new().build().unwrap();
        session.abort().unwrap();
        let err = session.new_download_stream_group().err().unwrap();
        assert!(matches!(err, SessionError::UserCancelled(_)));
    }

    #[test]
    // Aborting a session clears all registered stream groups.
    fn test_abort_clears_active_download_stream_groups() {
        let session = XetSessionBuilder::new().build().unwrap();
        let _g1 = session.new_download_stream_group().unwrap().build_blocking().unwrap();
        session.abort().unwrap();
        assert_eq!(session.inner.active_download_stream_groups.lock().unwrap().len(), 0);
    }

    // ── Streaming download round-trip tests ─────────────────────────────────

    async fn upload_bytes(
        session: &XetSession,
        endpoint: &str,
        data: &[u8],
        name: &str,
    ) -> Result<XetFileInfo, Box<dyn std::error::Error>> {
        let commit = session.new_upload_commit()?.with_endpoint(endpoint).build().await?;
        let _handle = commit
            .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some(name.into()))
            .await?;
        let results = commit.commit().await?;
        let meta = results.uploads.into_values().next().expect("one uploaded file");
        Ok(meta.xet_info)
    }

    fn upload_bytes_blocking(
        session: &XetSession,
        endpoint: &str,
        data: &[u8],
        name: &str,
    ) -> Result<XetFileInfo, Box<dyn std::error::Error>> {
        let commit = session.new_upload_commit()?.with_endpoint(endpoint).build_blocking()?;
        let _handle = commit.upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some(name.into()))?;
        let results = commit.commit_blocking()?;
        let meta = results.uploads.into_values().next().expect("one uploaded file");
        Ok(meta.xet_info)
    }

    #[tokio::test(flavor = "multi_thread")]
    // Async streaming download round-trip: upload, stream, verify content.
    async fn test_download_stream_round_trip() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let original = b"Hello, streaming download!";
        let file_info = upload_bytes(&session, &endpoint, original, "stream.bin").await.unwrap();

        let mut stream = session
            .new_download_stream_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap()
            .download_stream(file_info, None)
            .await
            .unwrap();
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
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let original = b"Hello, blocking streaming download!";
        let file_info = upload_bytes_blocking(&session, &endpoint, original, "stream.bin").unwrap();

        let mut stream = session
            .new_download_stream_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build_blocking()
            .unwrap()
            .download_stream_blocking(file_info, None)
            .unwrap();

        let mut collected = Vec::new();
        while let Some(chunk) = stream.blocking_next().unwrap() {
            collected.extend_from_slice(&chunk);
        }
        assert_eq!(collected, original);
    }

    #[tokio::test(flavor = "multi_thread")]
    // progress() reports correct totals after consuming the stream.
    async fn test_download_stream_progress_reports_completion() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let original = b"progress tracking test data for streaming";
        let file_info = upload_bytes(&session, &endpoint, original, "progress.bin").await.unwrap();

        let mut stream = session
            .new_download_stream_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap()
            .download_stream(file_info, None)
            .await
            .unwrap();
        let initial = stream.progress().unwrap();
        assert_eq!(initial.total_bytes, original.len() as u64);
        assert_eq!(initial.bytes_completed, 0);

        let mut collected = Vec::new();
        while let Some(chunk) = stream.next().await.unwrap() {
            collected.extend_from_slice(&chunk);
        }
        assert_eq!(collected, original);

        let final_progress = stream.progress().unwrap();
        assert_eq!(final_progress.total_bytes, original.len() as u64);
        assert_eq!(final_progress.bytes_completed, original.len() as u64);
    }

    #[test]
    // progress() works correctly in blocking mode.
    fn test_download_stream_blocking_progress_reports_completion() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let original = b"blocking progress tracking test data";
        let file_info = upload_bytes_blocking(&session, &endpoint, original, "progress.bin").unwrap();

        let mut stream = session
            .new_download_stream_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build_blocking()
            .unwrap()
            .download_stream_blocking(file_info, None)
            .unwrap();

        let mut collected = Vec::new();
        while let Some(chunk) = stream.blocking_next().unwrap() {
            collected.extend_from_slice(&chunk);
        }
        assert_eq!(collected, original);

        let final_progress = stream.progress().unwrap();
        assert_eq!(final_progress.total_bytes, original.len() as u64);
        assert_eq!(final_progress.bytes_completed, original.len() as u64);
    }

    #[tokio::test(flavor = "multi_thread")]
    // Multiple sequential streaming downloads share a single group's connection pool.
    async fn test_download_stream_multiple_sequential() {
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new().build().unwrap();
        let endpoint = format!("local://{}", temp.path().join("cas").display());
        let data_a = b"first stream payload";
        let data_b = b"second stream payload";
        let info_a = upload_bytes(&session, &endpoint, data_a, "a.bin").await.unwrap();
        let info_b = upload_bytes(&session, &endpoint, data_b, "b.bin").await.unwrap();

        let group = session
            .new_download_stream_group()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();

        let mut stream_a = group.download_stream(info_a, None).await.unwrap();
        let mut collected_a = Vec::new();
        while let Some(chunk) = stream_a.next().await.unwrap() {
            collected_a.extend_from_slice(&chunk);
        }
        assert_eq!(collected_a, data_a);

        let mut stream_b = group.download_stream(info_b, None).await.unwrap();
        let mut collected_b = Vec::new();
        while let Some(chunk) = stream_b.next().await.unwrap() {
            collected_b.extend_from_slice(&chunk);
        }
        assert_eq!(collected_b, data_b);
    }

    // ── Shared tokio handle behavior ──────────────────────────────────────────

    #[test]
    // Building multiple sessions with the same tokio handle is allowed.
    fn test_build_with_same_handle_stays_external() {
        let tokio_rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let handle = tokio_rt.handle().clone();

        let first = XetSessionBuilder::new().with_tokio_handle(handle.clone()).build().unwrap();
        assert_eq!(first.inner.ctx.runtime.mode(), RuntimeMode::External, "first build must use External runtime");

        let second = XetSessionBuilder::new().with_tokio_handle(handle).build();
        assert!(second.is_ok(), "second build with the same tokio handle must still succeed");
        assert_eq!(
            second.unwrap().inner.ctx.runtime.mode(),
            RuntimeMode::External,
            "second build should remain External when sharing the same tokio handle"
        );
    }

    #[test]
    // Dropping one session must not affect creating another session with the same handle.
    fn test_build_with_same_handle_succeeds_after_first_is_dropped() {
        let tokio_rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let handle = tokio_rt.handle().clone();

        let first = XetSessionBuilder::new().with_tokio_handle(handle.clone()).build().unwrap();
        drop(first);

        let second = XetSessionBuilder::new().with_tokio_handle(handle).build();
        assert!(second.is_ok(), "build must succeed after the previous session holding the same handle is dropped");
    }
}
