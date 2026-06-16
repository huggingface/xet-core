/// Whether the runtime owns its tokio thread pool or wraps an external handle.
///
/// - **`Owned`**: runtime created its own thread pool. Both async bridging ([`XetRuntime::bridge_async`]) and sync
///   bridging ([`XetRuntime::bridge_sync`]) are supported.
///
/// - **`External`**: runtime wraps a caller-provided tokio handle. Async bridging polls the future directly on the
///   caller's executor. Sync bridging is rejected with [`RuntimeError::InvalidRuntime`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RuntimeMode {
    Owned,
    External,
}

#[cfg(not(target_family = "wasm"))]
mod native;
#[cfg(not(target_family = "wasm"))]
pub use native::XetRuntime;

#[cfg(target_family = "wasm")]
mod wasm;
#[cfg(target_family = "wasm")]
pub use wasm::XetRuntime;
