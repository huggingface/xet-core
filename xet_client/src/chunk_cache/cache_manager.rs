use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, Weak};

use xet_runtime::config::XetConfig;

use super::error::ChunkCacheError;
use super::{CacheConfig, CacheEvictionPolicy, ChunkCache, DiskCache};

// single instance of CACHE_MANAGER not exposed to outside users that
// dedupes cache instances by cache directory, capacity, and eviction policy
static CACHE_MANAGER: LazyLock<CacheManager> = LazyLock::new(CacheManager::new);

/// get_cache attempts to return a cache given the provided config parameter
pub fn get_cache(xet_config: &XetConfig, config: &CacheConfig) -> Result<Arc<dyn ChunkCache>, ChunkCacheError> {
    CACHE_MANAGER.get(xet_config, config)
}

struct CacheManager {
    vals: Mutex<HashMap<CacheManagerKey, RefCell<Weak<dyn ChunkCache>>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheManagerKey {
    cache_directory: PathBuf,
    cache_size: u64,
    eviction_policy: CacheEvictionPolicy,
}

impl CacheManager {
    fn new() -> Self {
        Self {
            vals: Mutex::new(HashMap::new()),
        }
    }

    /// get takes a CacheConfig and checks if there exists a valid `DiskCache` with matching
    /// cache directory, capacity, and eviction policy. If it doesn't exist or the `DiskCache`
    /// instance has been deallocated (CacheManager only holds a weak pointer), then it creates a
    /// new instance based on the provided config.
    fn get(&self, xet_config: &XetConfig, config: &CacheConfig) -> Result<Arc<dyn ChunkCache>, ChunkCacheError> {
        let eviction_policy = CacheEvictionPolicy::from_xet_config(xet_config)?;
        if config.cache_size == 0 {
            return Err(ChunkCacheError::InvalidArguments);
        }

        let cache_key = CacheManagerKey {
            cache_directory: config.cache_directory.clone(),
            cache_size: config.cache_size,
            eviction_policy,
        };
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;
    use xet_runtime::config::XetConfig;

    use super::{CacheConfig, CacheManager};

    fn lru_config() -> XetConfig {
        XetConfig::default().with_config("chunk_cache.eviction_policy", "lru").unwrap()
    }

    #[test]
    fn reuses_live_cache_for_matching_configuration() {
        let manager = CacheManager::new();
        let cache_root = TempDir::new().unwrap();
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: 10_000,
        };
        let xet_config = XetConfig::new();

        let cache_a = manager.get(&xet_config, &config).unwrap();
        let cache_b = manager.get(&xet_config, &config).unwrap();

        assert!(Arc::ptr_eq(&cache_a, &cache_b));
    }

    #[test]
    fn errors_across_eviction_policies_for_same_cache_directory() {
        let manager = CacheManager::new();
        let cache_root = TempDir::new().unwrap();
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: 10_000,
        };

        let random_cache = manager.get(&XetConfig::new(), &config);
        let lru_cache = manager.get(&lru_config(), &config);

        assert!(random_cache.is_ok());
        assert!(lru_cache.is_err());
    }

    #[test]
    fn errors_across_capacities_for_same_cache_directory() {
        let manager = CacheManager::new();
        let cache_root = TempDir::new().unwrap();
        let small_config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: 10_000,
        };
        let large_config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: 20_000,
        };
        let xet_config = XetConfig::new();

        let small_cache = manager.get(&xet_config, &small_config);
        let large_cache = manager.get(&xet_config, &large_config);

        assert!(small_cache.is_ok());
        assert!(large_cache.is_err());
    }
}
