#![cfg_attr(feature = "strict", deny(warnings))]

pub mod async_iterator;
pub mod async_read;
pub mod auth;
pub mod constant_declarations;
pub mod errors;
#[cfg(not(target_family = "wasm"))]
pub mod limited_joinset;
mod output_bytes;
pub mod serialization_utils;
#[cfg(not(target_family = "wasm"))]
pub mod singleflight;

pub use output_bytes::output_bytes;

pub mod rw_task_lock;
pub use rw_task_lock::{RwTaskLock, RwTaskLockError, RwTaskLockReadGuard};

#[cfg(not(target_family = "wasm"))]
mod file_paths;

#[cfg(not(target_family = "wasm"))]
pub use file_paths::{normalized_path_from_user_string, CwdGuard, EnvVarGuard};
