use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use once_cell::sync::Lazy;

use crate::error::ChunkCacheError;
use crate::{CacheConfig, ChunkCache, DiskCache};

static CACHE_MANAGER: Lazy<CacheManager> = Lazy::new(CacheManager::new);

pub fn get_cache(config: &CacheConfig) -> Result<Arc<dyn ChunkCache>, ChunkCacheError> {
    CACHE_MANAGER.get(config)
}

struct CacheManager {
    vals: Mutex<HashMap<CacheConfig, RefCell<Weak<dyn ChunkCache>>>>,
}

impl CacheManager {
    fn new() -> Self {
        Self {
            vals: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &CacheConfig) -> Result<Arc<dyn ChunkCache>, ChunkCacheError> {
        let mut vals = self.vals.lock()?;
        if let Some(v) = vals.get_mut(key) {
            let w = v.borrow().clone();
            let result: Arc<dyn ChunkCache> = if let Some(value) = w.upgrade() {
                value
            } else {
                let result: Arc<dyn ChunkCache> = Arc::new(DiskCache::initialize(key)?);
                let weak = Arc::downgrade(&result);
                v.replace(weak);
                result
            };
            return Ok(result);
        }
        let result: Arc<dyn ChunkCache> = Arc::new(DiskCache::initialize(key)?);
        vals.insert(key.clone(), RefCell::new(Arc::downgrade(&result)));
        Ok(result)
    }
}
