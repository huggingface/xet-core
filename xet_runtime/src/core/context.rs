use std::sync::Arc;

use tokio::runtime::Handle as TokioRuntimeHandle;
use tracing::info;

use super::XetCommon;
use super::runtime::XetRuntime;
use crate::config::XetConfig;
use crate::error::RuntimeError;

/// Bundles the thread pool, configuration, and shared state into a single clonable handle.
///
/// Every major struct in the codebase should accept `&XetContext` in its constructor
/// and store a clone. This replaces the thread-local globals, allowing multiple
/// independent runtimes within the same process.
#[derive(Clone)]
pub struct XetContext {
    pub runtime: Arc<XetRuntime>,
    pub config: Arc<XetConfig>,
    pub common: Arc<XetCommon>,
}

impl XetContext {
    /// Creates a context from a pre-built thread pool and configuration.
    pub fn new(config: XetConfig, runtime: Arc<XetRuntime>) -> Self {
        let config = Arc::new(config);
        let common = Arc::new(XetCommon::new(&config));
        Self {
            runtime,
            config,
            common,
        }
    }

    /// Creates a context with default configuration and an auto-detected thread pool.
    ///
    /// If called from an owned runtime worker thread, reuses that owned [`XetRuntime`].
    /// Otherwise, if called from within an existing tokio runtime, wraps that runtime.
    /// If neither is available, spins up a new owned tokio thread pool.
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Result<Self, RuntimeError> {
        Self::with_config(XetConfig::new())
    }

    /// Creates a context with the given configuration and an auto-detected thread pool.
    ///
    /// Follows the same runtime selection as [`default`](Self::default):
    /// reuse an owned runtime if available, wrap an existing tokio handle, or create a new one.
    pub fn with_config(config: XetConfig) -> Result<Self, RuntimeError> {
        let runtime = if let Some(runtime) = XetRuntime::current_if_exists() {
            runtime
        } else if let Ok(handle) = TokioRuntimeHandle::try_current()
            && Self::handle_meets_requirements(&handle)
        {
            info!(
                "Detected compatible existing Tokio runtime; using external handle instead of creating a new thread pool"
            );
            XetRuntime::from_external(handle)
        } else {
            XetRuntime::new(&config)?
        };
        Ok(Self::new(config, runtime))
    }

    /// Wraps a caller-provided tokio handle with the given configuration.
    pub fn from_external(rt_handle: TokioRuntimeHandle, config: XetConfig) -> Self {
        Self::new(config, XetRuntime::from_external(rt_handle))
    }

    /// Checks whether a tokio handle meets the requirements for use with xet.
    pub fn handle_meets_requirements(handle: &TokioRuntimeHandle) -> bool {
        XetRuntime::handle_meets_requirements(handle)
    }

    /// Returns an error if the runtime is in the middle of a SIGINT shutdown.
    #[inline]
    pub fn check_sigint_shutdown(&self) -> Result<(), RuntimeError> {
        if self.runtime.in_sigint_shutdown() {
            Err(RuntimeError::KeyboardInterrupt)
        } else {
            Ok(())
        }
    }
}

impl std::fmt::Debug for XetContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XetContext")
            .field("runtime", &self.runtime)
            .field("config", &"...")
            .field("common", &"...")
            .finish()
    }
}
