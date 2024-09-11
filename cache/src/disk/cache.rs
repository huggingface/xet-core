use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::{debug, error};

use crate::disk::size_bound::{CacheValue, SizeBoundCache};
use crate::disk::storage::DiskManager;
use crate::interface::{BlockReadRequest, BlockReader};
use crate::metrics::DISK_EVICTION_AGE;
use crate::CacheError::BlockNotFound;
use crate::{util, CacheError};

/// A DiskCache provides a way to cache data using the local disk.
#[derive(Debug, Clone)]
pub struct DiskCache {
    disk_manager: Arc<DiskManager>,
    cache: Arc<SizeBoundCache>,
}

impl DiskCache {
    fn new(disk_manager: Arc<DiskManager>, cache: SizeBoundCache) -> DiskCache {
        DiskCache {
            disk_manager,
            cache: Arc::new(cache),
        }
    }

    /// Build a DiskCache from a path on disk and a desired capacity. This will synchronously
    /// load any existing files located in the directory into the cache.
    pub fn from_config(root_dir: &str, capacity_bytes: u64) -> Result<DiskCache, CacheError> {
        let disk_manager = Arc::new(DiskManager::new(PathBuf::from(root_dir.to_string())));
        let cache = SizeBoundCache::new_with_eviction(capacity_bytes, Some(disk_manager.clone()));
        let disk_cache = Self::new(disk_manager, cache);
        disk_cache.load_cache()?;
        Ok(disk_cache)
    }

    pub async fn put(&self, request: &BlockReadRequest, val: &[u8]) -> Result<bool, CacheError> {
        let key = request_to_key(request);

        let value = CacheValue::new(
            val.len() as u64,
            request.metadata().version(),
            key.clone(),
            request.block_size(),
            request.block_range().idx(),
        );
        let (was_inserted, evicted_blocks) = self.cache.put(key.as_str(), value.clone())?;
        if !was_inserted {
            return Ok(false);
        }
        for block in evicted_blocks {
            if block.key == key {
                debug!("Replacing old block for: {}", key);
            } else {
                debug!("Evicted: {}", block.key);
                DISK_EVICTION_AGE.observe(block.get_elapsed_ms() as f64 / 1000.0)
            }
        }

        match self.disk_manager.write(&value, val).await {
            Ok(_) => Ok(true),
            Err(e) => {
                self.cache.remove(key.as_str());
                Err(e)
            }
        }
    }

    /// Reads the root dir for the cache, adding the entries it finds into the cache.
    fn load_cache(&self) -> Result<(), CacheError> {
        let mut err = Ok(());
        let files_to_load_iter = self.disk_manager.init()?;
        // TODO: Currently, this just loads files into the LRU cache based off of the
        //       order that they're read in, which isn't technically correct for LRU.
        //       We may want to consider loading the files in order of accessed_time
        //       (as long as the cache FS was mounted with accessed_time metadata).
        debug!(
            "Successfully initialized cache dir at {:?} , loading existing files into cache",
            self.disk_manager.get_root_dir()
        );
        let num_files = files_to_load_iter
            .map(|val| self.cache.put(val.key.clone().as_str(), val))
            .scan(&mut err, util::until_err)
            .filter(|(b, _)| *b)
            .count();

        if let Err(e) = err {
            error!("Error loading files from root dir into the cache: {:?}", e);
            return Err(e);
        }
        debug!(
            "Loaded {} files from cache dir at {:?} into disk cache",
            num_files,
            self.disk_manager.get_root_dir()
        );
        Ok(())
    }
}

#[async_trait]
impl BlockReader for DiskCache {
    async fn get(&self, request: &BlockReadRequest) -> Result<Vec<u8>, CacheError> {
        let key = request_to_key(request);
        let opt_val = self.cache.get(key.as_str());
        match opt_val {
            Some(val) => self.disk_manager.read(&val, request.range()).await,
            None => Err(BlockNotFound),
        }
    }
}

fn request_to_key(request: &BlockReadRequest) -> String {
    // Note that this format is used in the Header class to attemp to parse out this metadata.
    format!(
        "{}.{}.{}",
        request.metadata().name(),
        request.block_range().idx(),
        request.block_size(),
    )
}

/// Wrapper around some action to take when a block is evicted.
pub trait EvictAction {
    /// Evicts the given CacheValue, returning whether or not it was successful.
    fn evict(&self, val: &CacheValue) -> Result<(), CacheError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::BlockRange;
    use crate::util::test_utils::CacheDirTest;
    use crate::FileMetadata;

    fn get_block_req(key: &str, size: u64, block_id: u64) -> BlockReadRequest {
        BlockReadRequest::from_block_range(
            BlockRange::new(block_id, 0..size, size),
            Arc::new(FileMetadata::new(key.to_string(), Some(size), 0)),
        )
    }

    fn get_bytes(size: usize) -> Vec<u8> {
        (0..size).map(|_| rand::random::<u8>()).collect::<Vec<u8>>()
    }

    #[tokio::test]
    async fn test_put() {
        let dir = CacheDirTest::new("cache_put");
        let c = DiskCache::from_config(dir.get_path_str(), 50).unwrap();
        let req = get_block_req("a", 10, 1);
        let val = get_bytes(10);
        let was_inserted = c.put(&req, val.as_slice()).await.unwrap();
        assert!(was_inserted);
        let files = dir.get_entries();
        assert_eq!(files.len(), 1);
        assert!(c.cache.get(&request_to_key(&req)).is_some())
    }

    #[tokio::test]
    async fn test_put_get() {
        let dir = CacheDirTest::new("cache_put_get");
        let c = DiskCache::from_config(dir.get_path_str(), 50).unwrap();
        let req = get_block_req("a", 10, 1);
        let val = get_bytes(10);
        let was_inserted = c.put(&req, val.as_slice()).await.unwrap();
        assert!(was_inserted);
        let data = c.get(&req).await.unwrap();
        assert_eq!(data, val);
    }

    #[tokio::test]
    async fn test_put_get_multiple() {
        let dir = CacheDirTest::new("cache_put_get_multiple");
        let c = DiskCache::from_config(dir.get_path_str(), 50).unwrap();
        let req = get_block_req("a", 10, 1);
        let val = get_bytes(10);
        let was_inserted = c.put(&req, val.as_slice()).await.unwrap();
        assert!(was_inserted);
        let data = c.get(&req).await.unwrap();
        assert_eq!(data, val);

        let data = c.get(&req).await.unwrap();
        assert_eq!(data, val);
    }

    #[tokio::test]
    async fn test_put_eviction() {
        let dir = CacheDirTest::new("cache_put_eviction");
        let c = DiskCache::from_config(dir.get_path_str(), 20).unwrap();
        let req = get_block_req("a", 10, 1);
        let val = get_bytes(10);
        let mut was_inserted = c.put(&req, val.as_slice()).await.unwrap();
        assert!(was_inserted);

        let req2 = get_block_req("b", 20, 1);
        let val2 = get_bytes(20);
        was_inserted = c.put(&req2, val2.as_slice()).await.unwrap();
        assert!(was_inserted);

        // "a" should have been evicted
        let result = c.get(&req).await;
        assert!(result.is_err());
        // contents for "a" should have been removed from disk
        let files = dir.get_entries();
        assert_eq!(files.len(), 1);
        // "b" should still be fetchable
        let vec = c.get(&req2).await.unwrap();
        assert_eq!(vec, val2);
    }

    #[tokio::test]
    async fn load_existing_cache() {
        let dir = CacheDirTest::new("cache_load");
        let dir_str = dir.get_path_str();
        println!("dir: {:?}", dir_str);
        let c = DiskCache::from_config(dir.get_path_str(), 20).unwrap();
        let req = get_block_req("a", 10, 1);
        let val = get_bytes(10);
        c.put(&req, val.as_slice()).await.unwrap();
        drop(c);

        let c2 = DiskCache::from_config(dir.get_path_str(), 20).unwrap();
        let data = c2.get(&req).await.unwrap();
        assert_eq!(val, data);
    }
}
