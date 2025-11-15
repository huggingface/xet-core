// Constants have been moved to config groups and are accessed via xet_config()

/// Default log level for the library to use. Override using the `RUST_LOG` env variable.
pub(crate) const DEFAULT_LOG_LEVEL_FILE: &str = "info";
pub(crate) const DEFAULT_LOG_LEVEL_CONSOLE: &str = "warn";
