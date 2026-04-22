use std::path::PathBuf;

use xet_client::chunk_cache::error::ChunkCacheError;
use xet_client::chunk_cache::{CacheConfig, DiskCache};
use xet_runtime::config::XetConfig;

pub mod sccache;
pub mod solid_cache;

/// only used for benchmark code
pub trait ChunkCacheExt: xet_client::chunk_cache::ChunkCache + Sized + Clone {
    fn _initialize(cache_root: PathBuf, capacity: u64) -> Result<Self, ChunkCacheError>;
    fn name() -> &'static str;
}

impl ChunkCacheExt for xet_client::chunk_cache::DiskCache {
    fn _initialize(cache_root: PathBuf, capacity: u64) -> Result<Self, ChunkCacheError> {
        let config = CacheConfig {
            cache_directory: cache_root,
            cache_size: capacity,
        };
        let xet_config = XetConfig::new();
        DiskCache::initialize(&xet_config, &config)
    }

    fn name() -> &'static str {
        "disk"
    }
}
