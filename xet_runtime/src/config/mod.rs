mod aliases;
pub use aliases::ENVIRONMENT_NAME_ALIASES;

mod error;
pub use error::ConfigError;

pub mod macros;
pub mod xet_config;

pub mod groups;

#[cfg(feature = "python")]
pub mod python;

// Re-export types from utils for backward compatibility and for use in config_group macro
// Re-export XetConfig for convenience
pub use xet_config::XetConfig;

pub use crate::utils::configuration_utils::ParsableConfigValue;

pub type ReconstructionConfig = groups::reconstruction::ConfigValues;
pub type DataConfig = groups::data::ConfigValues;
pub type MdbShardConfig = groups::shard::ConfigValues;
pub type DeduplicationConfig = groups::deduplication::ConfigValues;
pub type ChunkCacheConfig = groups::chunk_cache::ConfigValues;
pub type ClientConfig = groups::client::ConfigValues;
pub type LogConfig = groups::log::ConfigValues;
pub type XorbConfig = groups::xorb::ConfigValues;

#[cfg(feature = "python")]
pub use xet_config::py_xet_config::PyXetConfig;
