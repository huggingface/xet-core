//! Background system-resource monitor.
//!
//! Exactly one implementation is compiled and re-exported as [`SystemMonitor`]
//! / [`SystemMonitorError`] depending on `target_family`:
//!
//! - [`native`] — uses `sysinfo` to read per-process CPU/memory/disk and host network metrics; writes JSON-line records
//!   to a file or to `tracing` at INFO level.
//! - [`wasm`]   — reads what the browser exposes (`navigator.hardwareConcurrency`, `navigator.deviceMemory`,
//!   `performance.memory`, `navigator.connection`, etc.) and emits JSON via `tracing::info!`. There is no filesystem
//!   path for output in the browser; `log_path` is accepted for API parity but ignored.

#[cfg(not(target_family = "wasm"))]
mod native;

#[cfg(target_family = "wasm")]
mod wasm;

#[cfg(not(target_family = "wasm"))]
pub use native::{SystemMonitor, SystemMonitorError};
#[cfg(target_family = "wasm")]
pub use wasm::{SystemMonitor, SystemMonitorError};
