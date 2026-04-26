use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, Weak};

use xet_runtime::config::XetConfig;

use super::error::ChunkCacheError;
use super::{CacheConfig, CacheEvictionPolicy, ChunkCache, DiskCache};

// single instance of CACHE_MANAGER not exposed to outside users that
// dedupes cache instances based on configurations
static CACHE_MANAGER: LazyLock<CacheManager> = LazyLock::new(CacheManager::new);

/// get_cache attempts to return a cache given the provided config parameter
pub fn get_cache(xet_config: &XetConfig, config: &CacheConfig) -> Result<Arc<dyn ChunkCache>, ChunkCacheError> {
    CACHE_MANAGER.get(xet_config, config)
}

struct CacheManager {
    vals: Mutex<HashMap<CacheManagerKey, RefCell<Weak<dyn ChunkCache>>>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CacheManagerKey {
    cache_directory: PathBuf,
    cache_size: u64,
    eviction_policy: CacheEvictionPolicy,
}

impl CacheManagerKey {
    fn new(xet_config: &XetConfig, config: &CacheConfig) -> Result<Self, ChunkCacheError> {
        Ok(Self {
            cache_directory: config.cache_directory.clone(),
            cache_size: config.cache_size,
            eviction_policy: CacheEvictionPolicy::from_xet_config(xet_config)?,
        })
    }
}

impl CacheManager {
    fn new() -> Self {
        Self {
            vals: Mutex::new(HashMap::new()),
        }
    }

    /// get takes a CacheConfig and checks if there exists a valid `DiskCache` with a matching
    /// cache directory, capacity, and eviction policy. If it doesn't exist or the `DiskCache`
    /// instance has been deallocated (CacheManager only holds a weak pointer), then it creates a
    /// new instance based on the provided config.
    fn get(&self, xet_config: &XetConfig, config: &CacheConfig) -> Result<Arc<dyn ChunkCache>, ChunkCacheError> {
        let cache_key = CacheManagerKey::new(xet_config, config)?;
        let mut vals = self.vals.lock()?;
        if let Some(v) = vals.get_mut(&cache_key) {
            let weak = v.borrow().clone();
            // if upgrade from Weak to Arc is successful, returns the upgraded pointer
            if let Some(value) = weak.upgrade() {
                return Ok(value);
            }
            // since upgrading failed, creates a new DiskCache, replaces the weak pointer with a
            // weak pointer to the new instance and then returns the Arc to the new cache instance
            let result: Arc<dyn ChunkCache> = Arc::new(DiskCache::initialize(xet_config, config)?);
            v.replace(Arc::downgrade(&result));
            Ok(result)
        } else {
            // create a new Cache and insert weak pointer to managed map
            let result: Arc<dyn ChunkCache> = Arc::new(DiskCache::initialize(xet_config, config)?);
            vals.insert(cache_key, RefCell::new(Arc::downgrade(&result)));
            Ok(result)
        }
    }
}
