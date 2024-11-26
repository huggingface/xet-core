use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};

use once_cell::sync::Lazy;

use crate::error::ChunkCacheError;
use crate::{CacheConfig, ChunkCache, DiskCache};

// single instance of CACHE_MANAGER not exposed to outside users that
// dedupes cache instances based on configurations
static CACHE_MANAGER: Lazy<CacheManager> = Lazy::new(CacheManager::new);

/// get_cache attempts to return a cache given the provided config parameter
pub fn get_cache(config: &CacheConfig) -> Result<Arc<dyn ChunkCache>, ChunkCacheError> {
    CACHE_MANAGER.get(config)
}

struct CacheManager {
    vals: Mutex<HashMap<PathBuf, RefCell<Weak<dyn ChunkCache>>>>,
}

impl CacheManager {
    fn new() -> Self {
        Self {
            vals: Mutex::new(HashMap::new()),
        }
    }

    /// get takes a CacheConfig and checks if there exists a valid `DiskCache` with a matching
    /// cache_directory then it will return an Arc to that `DiskCache` instance. If it doesn't exist
    /// or the `DiskCache` instance has been deallocated (CacheManager only holds a weak pointer)
    /// then it creates a new instance based on the provided config.
    fn get(&self, config: &CacheConfig) -> Result<Arc<dyn ChunkCache>, ChunkCacheError> {
        let mut vals = self.vals.lock()?;
        if let Some(v) = vals.get_mut(&config.cache_directory) {
            let w = v.borrow().clone();
            let result: Arc<dyn ChunkCache> = if let Some(value) = w.upgrade() {
                value
            } else {
                let result: Arc<dyn ChunkCache> = Arc::new(DiskCache::initialize(config)?);
                v.replace(Arc::downgrade(&result));
                result
            };
            return Ok(result);
        }
        let result: Arc<dyn ChunkCache> = Arc::new(DiskCache::initialize(config)?);
        vals.insert(config.cache_directory.clone(), RefCell::new(Arc::downgrade(&result)));
        Ok(result)
    }
}
