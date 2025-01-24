use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};

use once_cell::sync::Lazy;
use utils::ThreadPool;

use crate::error::ChunkCacheError;
use crate::{CacheConfig, ChunkCache, DiskCache};

// single instance of CACHE_MANAGER not exposed to outside users that
// dedupes cache instances based on configurations
static CACHE_MANAGER: Lazy<CacheManager> = Lazy::new(CacheManager::new);

/// get_cache attempts to return a cache given the provided config parameter
pub fn get_cache(config: &CacheConfig, thread_pool: Arc<ThreadPool>) -> Result<Arc<dyn ChunkCache>, ChunkCacheError> {
    CACHE_MANAGER.get(config, thread_pool)
}

struct CacheManager {
    vals: Mutex<HashMap<(PathBuf, usize), RefCell<Weak<dyn ChunkCache>>>>,
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
    fn get(&self, config: &CacheConfig, thread_pool: Arc<ThreadPool>) -> Result<Arc<dyn ChunkCache>, ChunkCacheError> {
        let thread_pool_id = thread_pool.id();
        let key = (config.cache_directory.clone(), thread_pool_id);
        let mut vals = self.vals.lock()?;
        if let Some(v) = vals.get_mut(&key) {
            let weak = v.borrow().clone();
            // if upgrade from Weak to Arc is successful, returns the upgraded pointer
            if let Some(value) = weak.upgrade() {
                return Ok(value);
            }
            // since upgrading failed, creates a new DiskCache, replaces the weak pointer with a
            // weak pointer to the new instance and then returns the Arc to the new cache instance
            let result: Arc<dyn ChunkCache> = Arc::new(DiskCache::initialize(config, thread_pool)?);
            v.replace(Arc::downgrade(&result));
            Ok(result)
        } else {
            // create a new Cache and insert weak pointer to managed map
            let result: Arc<dyn ChunkCache> = Arc::new(DiskCache::initialize(config, thread_pool)?);
            vals.insert(key, RefCell::new(Arc::downgrade(&result)));
            Ok(result)
        }
    }
}
