#![cfg_attr(feature = "strict", deny(warnings))]

pub mod async_iterator;
pub mod async_read;
pub mod auth;
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

pub mod adjustable_semaphore;

mod exp_weighted_moving_avg;
pub use exp_weighted_moving_avg::ExpWeightedMovingAvg;
#[cfg(not(target_family = "wasm"))]
mod guards;

#[cfg(not(target_family = "wasm"))]
pub use guards::{CwdGuard, EnvVarGuard};

#[cfg(not(target_family = "wasm"))]
mod file_paths;

#[cfg(not(target_family = "wasm"))]
pub use file_paths::normalized_path_from_user_string;

pub mod byte_size;
pub use byte_size::ByteSize;

pub mod configuration_utils;
pub use configuration_utils::is_high_performance;

// Macros test_configurable_constants!, test_set_constants!, and test_set_config! are exported at crate root by
// #[macro_export]

#[cfg(not(target_family = "wasm"))]
pub mod pipe;

mod unique_id;
pub use unique_id::UniqueId;
