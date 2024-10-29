use std::path::PathBuf;

use chunk_cache::error::ChunkCacheError;
use chunk_cache::{CacheConfig, DiskCache};

pub mod sccache;
pub mod solid_cache;

/// only used for benchmark code
pub trait ChunkCacheExt: chunk_cache::ChunkCache + Sized + Clone {
    fn _initialize(cache_root: PathBuf, capacity: u64) -> Result<Self, ChunkCacheError>;
    fn name() -> &'static str;
}

impl ChunkCacheExt for chunk_cache::DiskCache {
    fn _initialize(cache_root: PathBuf, capacity: u64) -> Result<Self, ChunkCacheError> {
        let config = CacheConfig {
            cache_directory: cache_root,
            cache_size: capacity,
        };
        DiskCache::initialize(&config)
    }

    fn name() -> &'static str {
        "disk"
    }
}
