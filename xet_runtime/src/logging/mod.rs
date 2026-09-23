// The tokio-console layer is installed by `init`, which is not compiled without
// `logging`; enabling one without the other would build cleanly and collect nothing.
#[cfg(all(feature = "tokio-console", not(feature = "logging")))]
compile_error!("the `tokio-console` feature also needs the `logging` feature - enable both");

mod config;
#[cfg(feature = "logging")]
mod constants;
#[cfg(all(not(target_family = "wasm"), feature = "logging"))]
mod init;
#[cfg(all(target_family = "wasm", feature = "logging"))]
mod init_wasm;

#[cfg(not(target_family = "wasm"))]
pub mod system_monitor;

pub use config::{LogDirConfig, LoggingConfig, LoggingMode};
#[cfg(all(not(target_family = "wasm"), feature = "logging"))]
pub use init::{init, wait_for_log_directory_cleanup};
#[cfg(all(target_family = "wasm", feature = "logging"))]
pub use init_wasm::init;
#[cfg(not(target_family = "wasm"))]
pub use system_monitor::SystemMonitor;
