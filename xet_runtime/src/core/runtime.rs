use std::sync::Arc;

use reqwest::Client;
use tokio::runtime::Handle as TokioRuntimeHandle;

use super::XetCommon;
use super::threadpool::XetThreadpool;
use crate::config::XetConfig;
use crate::error::RuntimeError;

/// Bundles the thread pool, configuration, and shared state into a single clonable handle.
///
/// Every major struct in the codebase should accept `&XetRuntime` in its constructor
/// and store a clone. This replaces the thread-local globals, allowing multiple
/// independent runtimes within the same process.
#[derive(Clone)]
pub struct XetRuntime {
    pub threadpool: Arc<XetThreadpool>,
    pub config: Arc<XetConfig>,
    pub common: Arc<XetCommon>,
}

impl XetRuntime {
    /// Creates a runtime from a pre-built thread pool and configuration.
    pub fn new(threadpool: Arc<XetThreadpool>, config: XetConfig) -> Self {
        let config = Arc::new(config);
        let common = Arc::new(XetCommon::new(&config));
        Self {
            threadpool,
            config,
            common,
        }
    }

    /// Creates a runtime with default configuration and an auto-detected thread pool.
    ///
    /// If called from within an existing tokio runtime, wraps that runtime.
    /// Otherwise, spins up a new owned tokio thread pool.
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Result<Self, RuntimeError> {
        Self::default_with_config(XetConfig::new())
    }

    /// Creates a runtime with the given configuration and an auto-detected thread pool.
    ///
    /// If called from within an existing tokio runtime, wraps that runtime.
    /// Otherwise, spins up a new owned tokio thread pool.
    pub fn default_with_config(config: XetConfig) -> Result<Self, RuntimeError> {
        let threadpool = if let Ok(handle) = TokioRuntimeHandle::try_current() {
            XetThreadpool::from_external(handle)
        } else {
            XetThreadpool::new(&config)?
        };
        Ok(Self::new(threadpool, config))
    }

    /// Wraps a caller-provided tokio handle with default configuration.
    pub fn from_external(rt_handle: TokioRuntimeHandle) -> Self {
        Self::new(XetThreadpool::from_external(rt_handle), XetConfig::new())
    }

    /// Wraps a caller-provided tokio handle with system monitoring enabled.
    pub fn from_external_with_config(rt_handle: TokioRuntimeHandle, config: XetConfig) -> Result<Self, RuntimeError> {
        let threadpool = XetThreadpool::from_external_with_config(rt_handle, &config)?;
        Ok(Self::new(threadpool, config))
    }

    /// Wraps a caller-provided tokio handle after validating that it meets requirements.
    #[cfg(not(target_family = "wasm"))]
    pub fn from_validated_external(rt_handle: TokioRuntimeHandle, config: XetConfig) -> Result<Self, RuntimeError> {
        let threadpool = XetThreadpool::from_validated_external(rt_handle, &config)?;
        Ok(Self::new(threadpool, config))
    }

    /// Checks whether a tokio handle meets the requirements for use with xet.
    pub fn handle_meets_requirements(handle: &TokioRuntimeHandle) -> bool {
        XetThreadpool::handle_meets_requirements(handle)
    }

    /// Gets or creates a reqwest client, delegating to the shared `XetCommon` state.
    pub fn get_or_create_reqwest_client<F>(&self, tag: String, f: F) -> crate::error::Result<Client>
    where
        F: FnOnce() -> std::result::Result<Client, reqwest::Error>,
    {
        self.common.get_or_create_reqwest_client(tag, f)
    }

    /// Returns an error if the runtime is in the middle of a SIGINT shutdown.
    #[inline]
    pub fn check_sigint_shutdown(&self) -> Result<(), RuntimeError> {
        if self.threadpool.in_sigint_shutdown() {
            Err(RuntimeError::KeyboardInterrupt)
        } else {
            Ok(())
        }
    }
}

impl std::fmt::Debug for XetRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XetRuntime")
            .field("threadpool", &self.threadpool)
            .field("config", &"...")
            .field("common", &"...")
            .finish()
    }
}
