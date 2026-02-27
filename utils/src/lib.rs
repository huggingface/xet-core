#![cfg_attr(feature = "strict", deny(warnings))]

pub mod async_iterator;
pub mod data_structures;
pub use data_structures::{MerkleHashMap, PassThroughHashMap, TruncatedMerkleHashMap, U64HashExtractable};
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
pub mod resource_semaphore;
pub use resource_semaphore::ResourceSemaphore;

mod exp_weighted_moving_avg;
pub use exp_weighted_moving_avg::ExpWeightedMovingAvg;
#[cfg(not(target_family = "wasm"))]
mod guards;

#[cfg(not(target_family = "wasm"))]
pub use guards::{CallbackGuard, CwdGuard, EnvVarGuard};

#[cfg(not(target_family = "wasm"))]
mod file_paths;

#[cfg(not(target_family = "wasm"))]
pub use file_paths::TemplatedPathBuf;

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
