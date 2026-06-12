//! xet-console: feature-gated live observability for xet sessions.
//!
//! See docs/design/2026-06-11-xet-console-design.md. Everything in this module
//! follows one rule: a console failure must never affect the host transfer.

#[cfg(target_family = "wasm")]
compile_error!("the `console` feature is not supported on wasm targets (axum)");

pub mod model;
pub mod registry;
pub mod ring;
pub mod state;

use std::time::{SystemTime, UNIX_EPOCH};

/// Epoch milliseconds, server-stamped on every snapshot.
pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}
