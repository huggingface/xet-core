pub mod adjustable_semaphore;

pub mod byte_size;
pub use byte_size::ByteSize;

pub mod config_enum;
pub use config_enum::ConfigEnum;

pub mod configuration_utils;
pub use configuration_utils::is_high_performance;

#[cfg(not(target_family = "wasm"))]
mod file_paths;

#[cfg(not(target_family = "wasm"))]
pub use file_paths::TemplatedPathBuf;

/// On wasm, path templates and expansion are not supported. Use plain PathBuf.
#[cfg(target_family = "wasm")]
pub struct TemplatedPathBuf(std::path::PathBuf);

#[cfg(target_family = "wasm")]
impl TemplatedPathBuf {
    pub fn evaluate(path: impl AsRef<std::path::Path>) -> std::path::PathBuf {
        path.as_ref().to_path_buf()
    }
}

// Modules moved from core_structures
pub mod async_iterator;
pub mod async_read;
pub mod errors;

#[cfg(not(target_family = "wasm"))]
pub mod limited_joinset;

mod output_bytes;
pub use output_bytes::output_bytes;

#[cfg(not(target_family = "wasm"))]
pub mod singleflight;

pub mod rw_task_lock;
pub use rw_task_lock::{RwTaskLock, RwTaskLockError, RwTaskLockReadGuard};

mod guards;

#[cfg(not(target_family = "wasm"))]
pub use guards::{CwdGuard, EnvVarGuard};
pub use guards::ClosureGuard;

#[cfg(not(target_family = "wasm"))]
pub mod pipe;

mod unique_id;
pub use unique_id::UniqueId;
