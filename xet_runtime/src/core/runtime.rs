use std::sync::Arc;

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
    pub fn new(config: XetConfig, threadpool: Arc<XetThreadpool>) -> Self {
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
    /// If called from an owned runtime worker thread, reuses that owned threadpool.
    /// Otherwise, if called from within an existing tokio runtime, wraps that runtime.
    /// If neither is available, spins up a new owned tokio thread pool.
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Result<Self, RuntimeError> {
        let config = XetConfig::new();
        let threadpool = if let Some(threadpool) = XetThreadpool::current_if_exists() {
            threadpool
        } else if let Ok(handle) = TokioRuntimeHandle::try_current()
            && Self::handle_meets_requirements(&handle)
        {
            XetThreadpool::from_external(handle)
        } else {
            XetThreadpool::new(&config)?
        };
        Ok(Self::new(config, threadpool))
    }

    /// Wraps a caller-provided tokio handle with the given configuration.
    pub fn from_external(rt_handle: TokioRuntimeHandle, config: XetConfig) -> Self {
        Self::new(config, XetThreadpool::from_external(rt_handle))
    }

    /// Checks whether a tokio handle meets the requirements for use with xet.
    pub fn handle_meets_requirements(handle: &TokioRuntimeHandle) -> bool {
        XetThreadpool::handle_meets_requirements(handle)
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
