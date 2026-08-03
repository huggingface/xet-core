/// Default log level for the library to use. Override using the `RUST_LOG` env variable.
#[cfg(not(target_family = "wasm"))]
pub(crate) const DEFAULT_LOG_LEVEL_FILE: &str = "info";
pub(crate) const DEFAULT_LOG_LEVEL_CONSOLE: &str = "warn";
