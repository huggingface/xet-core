use std::sync::Arc;

use reqwest::Client;
use tokio::runtime::Handle as TokioRuntimeHandle;

use super::XetCommon;
use super::runtime::XetRuntime;
use crate::config::XetConfig;
use crate::error::RuntimeError;

/// Bundles the runtime, configuration, and shared state into a single clonable handle.
///
/// Every major struct in the codebase should accept `&XetContext` in its constructor
/// and store a clone. This replaces the thread-local globals, allowing multiple
/// independent contexts within the same process and tokio runtime.
#[derive(Clone)]
pub struct XetContext {
    pub runtime: Arc<XetRuntime>,
    pub config: Arc<XetConfig>,
    pub common: Arc<XetCommon>,
}

impl XetContext {
    /// Creates a context from a pre-built runtime and configuration.
    ///
    /// Callers should create the runtime via one of the [`XetRuntime`] factory methods
    /// (e.g. [`XetRuntime::new`], [`XetRuntime::from_external`]).
    pub fn new(runtime: Arc<XetRuntime>, config: XetConfig) -> Self {
        let config = Arc::new(config);
        let common = Arc::new(XetCommon::new(&config));
        Self {
            runtime,
            config,
            common,
        }
    }

    /// Creates a context with default configuration and an auto-detected runtime.
    ///
    /// If called from within an existing tokio runtime, wraps that runtime.
    /// Otherwise, spins up a new owned tokio thread pool.
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Result<Self, RuntimeError> {
        Self::default_with_config(XetConfig::new())
    }

    /// Creates a context with the given configuration and an auto-detected runtime.
    ///
    /// If called from within an existing tokio runtime, wraps that runtime.
    /// Otherwise, spins up a new owned tokio thread pool.
    pub fn default_with_config(config: XetConfig) -> Result<Self, RuntimeError> {
        let runtime = if let Ok(handle) = TokioRuntimeHandle::try_current() {
            XetRuntime::from_external(handle)
        } else {
            XetRuntime::new(&config)?
        };
        Ok(Self::new(runtime, config))
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
