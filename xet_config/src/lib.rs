mod aliases;
pub use aliases::ENVIRONMENT_NAME_ALIASES;

pub mod macros;
pub mod xet_config;

pub mod groups;

// Re-export types from utils for backward compatibility and for use in config_group macro
pub use utils::configuration_utils::ParsableConfigValue;
// Re-export XetConfig for convenience
pub use xet_config::XetConfig;

pub type ReconstructionConfig = groups::reconstruction::ConfigValues;
pub type DataConfig = groups::data::ConfigValues;
pub type MdbShardConfig = groups::mdb_shard::ConfigValues;
pub type DeduplicationConfig = groups::deduplication::ConfigValues;
pub type ChunkCacheConfig = groups::chunk_cache::ConfigValues;
pub type ClientConfig = groups::client::ConfigValues;
pub type LogConfig = groups::log::ConfigValues;
pub type XorbConfig = groups::xorb::ConfigValues;
