pub mod errors;
pub mod exports;

pub mod runtime;

use std::sync::Arc;

pub use runtime::XetRuntime;
use xet_config::XetConfig;
pub mod sync_primatives;
pub use sync_primatives::{SyncJoinHandle, spawn_os_thread};

#[macro_use]
mod global_semaphores;
pub mod utils;

pub use global_semaphores::GlobalSemaphoreHandle;

pub mod file_handle_limits;

#[cfg(not(target_family = "wasm"))]
mod cache_dir;

#[cfg(not(target_family = "wasm"))]
pub use cache_dir::xet_cache_root;

pub fn xet_config() -> Arc<XetConfig> {
    XetRuntime::current().config().clone()
}
