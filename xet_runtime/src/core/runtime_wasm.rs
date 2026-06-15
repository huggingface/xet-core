use std::fmt;
use std::sync::Arc;

use crate::config::XetConfig;
use crate::error::RuntimeError;

use super::RuntimeMode;

/// Minimal wasm runtime stub. No tokio runtime is instantiated on wasm. The bridge
/// methods in `task_runtime.rs` use cfg-gated variants that `.await` futures directly
/// via `wasm_bindgen_futures::spawn_local`, never driving a tokio handle.
/// `XetContext` holds an `Arc<XetRuntime>` on all targets for a consistent type;
/// on wasm it is purely a bookkeeping object (config + cancellation tokens).
#[derive(Debug)]
pub struct XetRuntime {}

impl XetRuntime {
    pub fn new(_config: &XetConfig) -> Result<Arc<Self>, RuntimeError> {
        Ok(Arc::new(Self {}))
    }

    /// Wasm has no real blocking thread pool — `tokio_with_wasm::task::spawn_blocking`
    /// runs `f` inline and returns a completed `JoinHandle`.
    pub fn spawn_blocking<F, R>(self: &Arc<Self>, f: F) -> tokio_with_wasm::task::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        tokio_with_wasm::task::spawn_blocking(f)
    }

    /// Always returns `None`; wasm has no owned worker threads to register.
    pub fn current_if_exists() -> Option<Arc<Self>> {
        None
    }

    /// Wasm has no SIGINT; always returns `false`.
    pub fn in_sigint_shutdown(&self) -> bool {
        false
    }

    pub fn external_executor_count(&self) -> usize {
        0
    }

    /// No-op on wasm; SIGINT is not a concept in the browser.
    pub fn perform_sigint_shutdown(&self) {}

    pub fn discard_runtime(&self) {}

    pub fn mode(&self) -> RuntimeMode {
        RuntimeMode::Owned
    }
}

impl fmt::Display for XetRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "wasm runtime (no tokio metrics)")
    }
}
