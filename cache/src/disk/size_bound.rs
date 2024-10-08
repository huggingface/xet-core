use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use lru::LruCache;
use tracing::error;

use crate::disk::cache::EvictAction;
use crate::{util, CacheError};

/// Defines a cache bound by the size of the elements within.
///
/// ## Eviction Policy
/// This cache uses an LRU eviction strategy.
///
/// ## Admission Policy
/// All new blocks are admitted as long as their size doesn't exceed
/// the cache capacity.
///
/// If attempting to insert a block already present in the cache, then
/// the new block is allowed if it's version is >= the cached version.
///
/// ## Other Notes
/// This implementation is thread-safe. However, it does so by locking
/// the entire map for any operation.
/// TODO: Check/improve performance.
#[derive(Debug)]
pub struct SizeBoundCache {
    cache: Arc<Mutex<InternalCacheState>>,
}

/// Internal state of the cache. This is not thread-safe and is intended to be used under the
/// Arc<Mutex<>> in the SizeBoundCache.
struct InternalCacheState {
    /// How many bytes are allowed in the cache.
    capacity: u64,
    /// How many bytes are currently used in the cache.
    current_usage: u64,
    /// The cache implementation.
    ///
    /// We are using a `RefCell<>` since the `get` function of the
    /// LruCache uses `&mut self` (since ordering of the cache changes).
    /// We want to abstract away this detail/requirement from the caller
    /// and allow calls to `get` without a mutable reference.  
    values: lru::LruCache<String, CacheValue>,
    /// An optional EvictAction that will be called on eviction.
    on_evict: Option<Arc<dyn EvictAction + Send + Sync>>,
}

impl InternalCacheState {
    fn get(&mut self, key: &str) -> Option<CacheValue> {
        self.values.get(key).cloned()
    }

    /// Whether or not the key and value should be admitted into the cache.
    fn should_admit(&self, key: &str, value: &CacheValue) -> bool {
        if !value.key.is_empty() && value.key != key {
            error!("BUG: CacheValue.key doesn't match the key it is being inserted under!");
            return false;
        }
        if value.size > self.capacity {
            return false;
        }
        self.values
            .peek(key)
            .map_or(true, |old| old.version <= value.version)
    }

    /// Removes the indicated key from the cache. Updating the internal state and
    /// returning the old value if present.
    fn pop(&mut self, key: &str) -> Option<CacheValue> {
        self.values.pop(key).inspect(|val| {
            self.current_usage -= val.size;
        })
    }

    /// Pushes the key, value into the cache, updating the internal state.
    /// Expects that the key is not present in the cache
    fn push(&mut self, key: &str, mut value: CacheValue) {
        self.current_usage += value.size;

        // update value with internal state if not present (e.g. from loading on startup)
        if value.insertion_time_ms == 0 {
            value.insertion_time_ms = util::time_to_epoch_millis(SystemTime::now());
        }
        value.key = key.to_string();

        if self.values.put(key.to_string(), value).is_some() {
            panic!("BUG in SizeBoundCache: tried to insert into cache when entry already exists")
        }
    }

    /// Evicts an element from the cache.
    fn evict_next(&mut self) -> Result<CacheValue, CacheError> {
        let (_, val) = self
            .values
            .pop_lru()
            .expect("Ran out of items to evict in cache");
        if let Some(evictor) = &self.on_evict {
            evictor.evict(&val)?;
        }
        self.current_usage -= val.size;
        Ok(val)
    }

    fn space_remaining(&self) -> u64 {
        self.capacity - self.current_usage
    }
}

impl Debug for InternalCacheState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InternalCacheState")
            .field("capacity", &self.capacity)
            .field("current_usage", &self.current_usage)
            .field("num_elements", &self.values.len())
            .finish()
    }
}

#[allow(unused)]
impl SizeBoundCache {
    /// Builds a new cache bound to the indicated capacity.
    pub fn new(capacity: u64) -> Self {
        Self::new_with_eviction(capacity, None)
    }

    /// Builds a new cache bound to the indicated capacity.
    /// Can optionally accept an EvictAction as an action to run when an object
    /// is evicted.
    ///
    /// ## Implementation note
    /// We use an `LruCache::unbounded()` because we don't care about the number
    /// of entries in our cache, only the total size of all entries.
    pub fn new_with_eviction(
        capacity: u64,
        on_evict: Option<Arc<dyn EvictAction + Send + Sync>>,
    ) -> Self {
        SizeBoundCache {
            cache: Arc::new(Mutex::new(InternalCacheState {
                capacity,
                current_usage: 0,
                values: LruCache::unbounded(),
                on_evict,
            })),
        }
    }

    /// Inserts the given key with the provided value into the cache. Whether the
    /// block was inserted into the cache or not If this
    /// insertion results in cache eviction, then the evicted entries are returned.
    ///
    /// If the key already exists in the cache, then the version of the new value
    /// is compared with the existing one and if new >= old, then the value will
    /// be replaced (and the old one will be returned in the Vec).
    ///
    /// ## Implementation note
    /// We don't need an `&mut self` here since the structure is thread-safe and
    /// thus, shared access via this function is ok and doesn't need to be handled
    /// by the caller.
    pub fn put(&self, key: &str, value: CacheValue) -> Result<(bool, Vec<CacheValue>), CacheError> {
        let mut evicted = vec![];
        let mut internal = self.cache.lock().unwrap();

        if !internal.should_admit(key, &value) {
            return Ok((false, evicted));
        }
        if let Some(old) = internal.pop(key) {
            evicted.push(old);
        }
        while internal.space_remaining() < value.size {
            evicted.push(internal.evict_next()?);
        }
        internal.push(key, value);
        Ok((true, evicted))
    }

    /// Retrieves the value corresponding to this key.
    ///
    /// ## Implementation note
    /// Internally, we store
    pub fn get(&self, key: &str) -> Option<CacheValue> {
        let mut internal = self.cache.lock().unwrap();
        internal.get(key)
    }

    /// Explicitly removes the indicated key from the cache. Will return the
    /// value for the key if it exists.
    pub fn remove(&self, key: &str) -> Option<CacheValue> {
        let mut internal = self.cache.lock().unwrap();
        internal.pop(key)
    }
}

/// Wrapper for info about a particular block stored in the cache.
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct CacheValue {
    pub size: u64,
    pub version: u128,
    pub key: String,
    pub block_size: u64,
    pub block_idx: u64,

    // updated when the value is inserted into the cache
    pub insertion_time_ms: u128,
}

impl CacheValue {
    pub fn new(size: u64, version: u128, key: String, block_size: u64, block_idx: u64) -> Self {
        CacheValue {
            size,
            version,
            key,
            block_size,
            block_idx,
            ..Default::default()
        }
    }

    pub fn get_elapsed_ms(&self) -> u128 {
        util::time_to_epoch_millis(SystemTime::now()) - self.insertion_time_ms
    }
}

#[cfg(test)]
mod size_bound_cache_tests {
    use super::{CacheValue, SizeBoundCache};
    use crate::disk::cache::EvictAction;
    use crate::CacheError;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    // check that inserting a value for key with the indicated size is successful, returning
    // any evicted responses.
    fn check_valid_insertion(c: &SizeBoundCache, key: &str, size: u64) -> Vec<CacheValue> {
        check_valid_insertion_with_version(c, key, size, 1)
    }

    fn check_valid_insertion_with_version(
        c: &SizeBoundCache,
        key: &str,
        size: u64,
        version: u128,
    ) -> Vec<CacheValue> {
        let (b, v) = c
            .put(key, CacheValue::new(size, version, key.to_string(), 0, 0))
            .unwrap();
        assert!(b);
        v
    }

    #[test]
    fn test_insertion_evictions() {
        let cache = SizeBoundCache::new(50);

        assert_eq!(check_valid_insertion(&cache, "a", 20).len(), 0);
        assert_eq!(check_valid_insertion(&cache, "b", 20).len(), 0);
        let evicted = check_valid_insertion(&cache, "c", 20);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].key, "a".to_string());
    }

    #[test]
    fn test_multi_evict() {
        let cache = SizeBoundCache::new(50);

        assert_eq!(check_valid_insertion(&cache, "a", 20).len(), 0);
        assert_eq!(check_valid_insertion(&cache, "b", 20).len(), 0);
        assert_eq!(check_valid_insertion(&cache, "c", 40).len(), 2);
    }

    #[test]
    fn test_lru_eviction() {
        let cache = SizeBoundCache::new(50);
        assert_eq!(check_valid_insertion(&cache, "a", 20).len(), 0);
        assert_eq!(check_valid_insertion(&cache, "b", 20).len(), 0);
        assert_eq!(cache.get("a").unwrap().size, 20);
        let evicted = check_valid_insertion(&cache, "c", 20);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].key, "b".to_string());
    }

    #[test]
    fn test_overwrite() {
        let cache = SizeBoundCache::new(50);
        assert_eq!(check_valid_insertion(&cache, "a", 20).len(), 0);
        assert_eq!(check_valid_insertion(&cache, "b", 20).len(), 0);
        let evicted = check_valid_insertion_with_version(&cache, "b", 30, 2);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].key, "b".to_string());
    }

    #[test]
    fn test_insertion_denied_too_large() {
        let cache = SizeBoundCache::new(10);
        let key = "a";
        let (b, v) = cache
            .put(key, CacheValue::new(100, 1, key.to_string(), 0, 0))
            .unwrap();
        assert!(!b);
        assert_eq!(v.len(), 0);
        assert!(cache.get(key).is_none())
    }

    #[test]
    fn test_insertion_denied_old_version() {
        let cache = SizeBoundCache::new(50);
        let key = "a";
        assert_eq!(check_valid_insertion(&cache, key, 20).len(), 0);
        let (b, v) = cache
            .put(key, CacheValue::new(10, 0, key.to_string(), 0, 0))
            .unwrap();
        assert!(!b);
        assert_eq!(v.len(), 0);
        let cached_val = cache.get(key).unwrap();
        assert_eq!(cached_val.size, 20);
        assert_eq!(cached_val.version, 1);
    }

    #[test]
    fn test_eviction_callback() {
        let e = Arc::new(Evictor::default());
        let cache = SizeBoundCache::new_with_eviction(50, Some(e.clone()));
        assert_eq!(check_valid_insertion(&cache, "a", 30).len(), 0);
        assert_eq!(check_valid_insertion(&cache, "b", 30).len(), 1);
        // Expect that the evictor was called once
        let guard = e.evicted_keys.lock().unwrap();
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0].as_str(), "a");
    }

    #[test]
    fn test_eviction_callback_multiple() {
        let e = Arc::new(Evictor::default());
        let cache = SizeBoundCache::new_with_eviction(50, Some(e.clone()));
        assert_eq!(check_valid_insertion(&cache, "a", 20).len(), 0);
        assert_eq!(check_valid_insertion(&cache, "b", 30).len(), 0);
        assert_eq!(check_valid_insertion(&cache, "c", 40).len(), 2);
        // Expect that the evictor was called twice
        let guard = e.evicted_keys.lock().unwrap();
        assert_eq!(guard.len(), 2);
        assert_eq!(guard[0].as_str(), "a");
        assert_eq!(guard[1].as_str(), "b");
    }

    #[test]
    fn test_eviction_callback_error() {
        let e = Arc::new(Evictor {
            return_err: AtomicBool::new(true),
            ..Default::default()
        });
        let cache = SizeBoundCache::new_with_eviction(50, Some(e));
        assert_eq!(check_valid_insertion(&cache, "a", 30).len(), 0);
        let res = cache.put("b", CacheValue::new(30, 0, "b".to_string(), 0, 0));
        // We expect that there is an error returned (via the EvictAction we gave the cache) and
        // that the cache entry was evicted.
        assert!(res.is_err());
        assert_eq!(cache.cache.lock().unwrap().values.len(), 0);
    }

    /// EvictAction to help test that the SizeBoundCache is calling it.
    struct Evictor {
        evicted_keys: Arc<Mutex<Vec<String>>>,
        return_err: AtomicBool,
    }

    impl EvictAction for Evictor {
        fn evict(&self, val: &CacheValue) -> Result<(), CacheError> {
            if self.return_err.load(Ordering::SeqCst) {
                return Err(CacheError::InvariantViolated(
                    "Invariant violated test error".to_string(),
                ));
            }
            self.evicted_keys.lock().unwrap().push(val.key.clone());
            Ok(())
        }
    }

    impl Default for Evictor {
        fn default() -> Self {
            Evictor {
                evicted_keys: Arc::new(Mutex::new(vec![])),
                return_err: AtomicBool::new(false),
            }
        }
    }
}
