/// Default log level for the library to use. Override using the `RUST_LOG` env variable.
#[cfg(not(target_family = "wasm"))]
pub(crate) const DEFAULT_LOG_LEVEL_FILE: &str = "info";
#[cfg(not(target_family = "wasm"))]
pub(crate) const DEFAULT_LOG_LEVEL_CONSOLE: &str = "warn";

/// Default log level on wasm. Higher than the native console default because the
/// browser console is the only sink there - there is no log file collecting `info`
/// alongside it, and devtools can filter by level after the fact.
#[cfg(target_family = "wasm")]
pub(crate) const DEFAULT_LOG_LEVEL_WASM_CONSOLE: &str = "info";
