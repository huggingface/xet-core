use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::os::unix::ffi::OsStringExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use base64::Engine;
use cas_types::{ChunkRange, Key};
use chunk_cache::error::ChunkCacheError;
use chunk_cache::ChunkCache;
use sccache::lru_disk_cache::LruDiskCache;

use crate::ChunkCacheExt;

#[derive(Clone)]
pub struct SCCache {
    cache: Arc<Mutex<LruDiskCache>>,
}

impl ChunkCacheExt for SCCache {
    fn _initialize(cache_root: PathBuf, capacity: u64) -> Result<Self, ChunkCacheError> {
        let cache = LruDiskCache::new(cache_root, capacity).map_err(ChunkCacheError::general)?;

        Ok(Self {
            cache: Arc::new(Mutex::new(cache)),
        })
    }

    fn name() -> &'static str {
        "sccache"
    }
}

impl ChunkCache for SCCache {
    fn get(&self, key: &cas_types::Key, range: &cas_types::ChunkRange) -> Result<Option<Vec<u8>>, ChunkCacheError> {
        let cache_key = CacheKey::new(key, range)?;
        let mut file = if let Ok(file) = self.cache.lock()?.get(&cache_key) {
            file
        } else {
            return Ok(None);
        };

        let mut res = Vec::new();
        file.read_to_end(&mut res)?;
        Ok(Some(res))
    }

    fn put(
        &self,
        key: &cas_types::Key,
        range: &cas_types::ChunkRange,
        _chunk_byte_indices: &[u32],
        data: &[u8],
    ) -> Result<(), ChunkCacheError> {
        let mut cache = self.cache.lock()?;

        let cache_key = CacheKey::new(key, range)?;
        if cache.get(&cache_key).is_ok() {
            return Ok(());
        }

        cache.insert_bytes(cache_key, data).map_err(ChunkCacheError::general)?;
        Ok(())
    }
}

#[derive(Debug)]
struct CacheKey(OsString);

impl CacheKey {
    fn new(key: &Key, range: &ChunkRange) -> Result<Self, ChunkCacheError> {
        let mut buf = Vec::new();
        buf.write_all(key.hash.as_bytes())?;
        buf.write_all(key.prefix.as_bytes())?;
        buf.write_all(format!("{}_{}", range.start, range.end).as_bytes())?;
        let result = base64::engine::general_purpose::URL_SAFE.encode(buf).as_bytes().to_vec();
        Ok(CacheKey(OsString::from_vec(result)))
    }
}

impl AsRef<OsStr> for CacheKey {
    fn as_ref(&self) -> &OsStr {
        &self.0
    }
}
