mod config;
#[cfg(feature = "logging")]
mod constants;
#[cfg(all(not(target_family = "wasm"), feature = "logging"))]
mod init;

#[cfg(not(target_family = "wasm"))]
pub mod system_monitor;

pub use config::{LogDirConfig, LoggingConfig, LoggingMode};
#[cfg(all(not(target_family = "wasm"), feature = "logging"))]
pub use init::{init, wait_for_log_directory_cleanup};
#[cfg(not(target_family = "wasm"))]
pub use system_monitor::SystemMonitor;
