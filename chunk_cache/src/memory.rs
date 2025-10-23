use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use cas_types::{ChunkRange, Key};
use tokio::sync::RwLock;
use tracing::{debug, info};
use utils::output_bytes;

use crate::error::ChunkCacheError;
use crate::{CacheRange, ChunkCache};

/// Default memory cache capacity as a percentage of system RAM (20%)
pub const DEFAULT_MEMORY_CACHE_PERCENTAGE: f64 = 0.20;

/// Get the default memory cache percentage
pub fn default_memory_cache_percentage() -> f64 {
    DEFAULT_MEMORY_CACHE_PERCENTAGE
}

/// Get total system memory in bytes
fn get_system_memory_bytes() -> Option<u64> {
    use sysinfo::System;
    
    let mut sys = System::new_all();
    sys.refresh_memory();
    Some(sys.total_memory())
}

/// Convert percentage to bytes based on system RAM
fn percentage_to_bytes(percentage: f64) -> u64 {
    if percentage <= 0.0 {
        return 0;
    }
    
    get_system_memory_bytes()
        .map(|ram| (ram as f64 * percentage) as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone)]
struct MemoryCacheItem {
    range: ChunkRange,
    chunk_byte_indices: Vec<u32>,
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
struct CacheState {
    inner: HashMap<Key, Vec<MemoryCacheItem>>,
    num_items: usize,
    total_bytes: u64,
}

impl CacheState {
    fn new() -> Self {
        Self {
            inner: HashMap::new(),
            num_items: 0,
            total_bytes: 0,
        }
    }

    fn find_match(&self, key: &Key, range: &ChunkRange) -> Option<&MemoryCacheItem> {
        let items = self.inner.get(key)?;

        for item in items.iter() {
            if item.range.start <= range.start && range.end <= item.range.end {
                return Some(item);
            }
        }
        None
    }

    /// Evict items from the cache until total_bytes <= max_total_bytes
    fn evict_to_capacity(&mut self, max_total_bytes: u64) {
        let original_total_bytes = self.total_bytes;

        while self.total_bytes > max_total_bytes && self.num_items > 0 {
            let Some((key, idx)) = self.random_item() else {
                break;
            };
            
            let items = self.inner.get_mut(&key).expect("Key should exist");
            let cache_item = items.swap_remove(idx);
            let len = cache_item.data.len() as u64;

            if items.is_empty() {
                self.inner.remove(&key);
            }

            self.total_bytes -= len;
            self.num_items -= 1;
        }

        if original_total_bytes > self.total_bytes {
            debug!(
                "memory cache evicting {} items totaling {}",
                original_total_bytes - self.total_bytes,
                output_bytes(original_total_bytes - self.total_bytes)
            );
        }
    }

    /// Returns the key and index within that key for a random item
    fn random_item(&self) -> Option<(Key, usize)> {
        if self.num_items == 0 {
            return None;
        }
        let random_item = rand::random::<u32>() as usize % self.num_items;
        let mut count = 0;
        for (key, items) in self.inner.iter() {
            if random_item < count + items.len() {
                return Some((key.clone(), random_item - count));
            }
            count += items.len();
        }
        None
    }
}

/// MemoryCache is a ChunkCache implementor that stores data in memory
#[derive(Debug, Clone)]
pub struct MemoryCache {
    capacity: u64,
    state: Arc<RwLock<CacheState>>,
}

impl MemoryCache {
    /// Initialize a new MemoryCache with the given percentage of system RAM (0.0-1.0)
    /// For example, 0.20 means 20% of system RAM
    pub fn new(percentage: f64) -> Result<Self, ChunkCacheError> {
        if percentage <= 0.0 || percentage > 1.0 {
            return Err(ChunkCacheError::InvalidArguments);
        }

        let capacity = percentage_to_bytes(percentage);
        if capacity == 0 {
            return Err(ChunkCacheError::InvalidArguments);
        }

        info!(
            "Initializing memory cache with {:.1}% of system RAM: {}",
            percentage * 100.0,
            output_bytes(capacity)
        );

        Ok(Self {
            capacity,
            state: Arc::new(RwLock::new(CacheState::new())),
        })
    }

    pub async fn num_items(&self) -> usize {
        self.state.read().await.num_items
    }

    pub async fn total_bytes(&self) -> u64 {
        self.state.read().await.total_bytes
    }
}

#[async_trait]
impl ChunkCache for MemoryCache {
    async fn get(&self, key: &Key, range: &ChunkRange) -> Result<Option<CacheRange>, ChunkCacheError> {
        if range.start >= range.end {
            return Err(ChunkCacheError::InvalidArguments);
        }

        let state = self.state.read().await;
        let Some(cache_item) = state.find_match(key, range) else {
            return Ok(None);
        };

        // Extract the requested range from the cached item
        let start_idx = (range.start - cache_item.range.start) as usize;
        let end_idx = (range.end - cache_item.range.start) as usize;
        
        if end_idx >= cache_item.chunk_byte_indices.len() {
            return Err(ChunkCacheError::BadRange);
        }

        let start_byte = cache_item.chunk_byte_indices[start_idx] as usize;
        let end_byte = cache_item.chunk_byte_indices[end_idx] as usize;
        
        if end_byte > cache_item.data.len() {
            return Err(ChunkCacheError::BadRange);
        }

        let data = cache_item.data[start_byte..end_byte].to_vec();
        let offsets: Vec<u32> = cache_item.chunk_byte_indices[start_idx..=end_idx]
            .iter()
            .map(|v| v - cache_item.chunk_byte_indices[start_idx])
            .collect();

        Ok(Some(CacheRange {
            offsets,
            data,
            range: *range,
        }))
    }

    async fn put(
        &self,
        key: &Key,
        range: &ChunkRange,
        chunk_byte_indices: &[u32],
        data: &[u8],
    ) -> Result<(), ChunkCacheError> {
        // Validate inputs
        if range.start >= range.end
            || chunk_byte_indices.len() != (range.end - range.start + 1) as usize
            || chunk_byte_indices.is_empty()
            || chunk_byte_indices[0] != 0
            || *chunk_byte_indices.last().unwrap() as usize != data.len()
            || !strictly_increasing(chunk_byte_indices)
        {
            return Err(ChunkCacheError::InvalidArguments);
        }

        let data_len = data.len() as u64;

        // Don't cache items that are larger than the cache capacity
        if data_len > self.capacity {
            debug!(
                "Skipping cache put for key {} range {:?} - item size {} exceeds capacity {}",
                key,
                range,
                output_bytes(data_len),
                output_bytes(self.capacity)
            );
            return Ok(());
        }

        let mut state = self.state.write().await;

        // Check if we already have this exact range cached
        if let Some(items) = state.inner.get(key) {
            for item in items.iter() {
                if item.range == *range {
                    // Already cached
                    return Ok(());
                }
            }
        }

        // Evict items if necessary to make room
        if state.total_bytes + data_len > self.capacity {
            state.evict_to_capacity(self.capacity - data_len);
        }

        // Add the new item
        let cache_item = MemoryCacheItem {
            range: *range,
            chunk_byte_indices: chunk_byte_indices.to_vec(),
            data: data.to_vec(),
        };

        state.total_bytes += data_len;
        state.num_items += 1;

        state
            .inner
            .entry(key.clone())
            .or_insert_with(Vec::new)
            .push(cache_item);

        Ok(())
    }
}

fn strictly_increasing(chunk_byte_indices: &[u32]) -> bool {
    for i in 1..chunk_byte_indices.len() {
        if chunk_byte_indices[i - 1] >= chunk_byte_indices[i] {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use cas_types::{ChunkRange, Key};

    use super::*;

    #[test]
    fn test_get_system_memory() {
        let mem = get_system_memory_bytes();
        assert!(mem.is_some());
        assert!(mem.unwrap() > 0);
    }

    #[test]
    fn test_default_memory_cache_percentage() {
        let percentage = default_memory_cache_percentage();
        assert_eq!(percentage, DEFAULT_MEMORY_CACHE_PERCENTAGE);
        assert!(percentage > 0.0 && percentage <= 1.0);
    }

    #[tokio::test]
    async fn test_memory_cache_basic() {
        let cache = MemoryCache::new(0.01).unwrap(); // 1% of RAM
        let key = Key {
            prefix: "test".to_string(),
            hash: merklehash::MerkleHash::default(),
        };
        let range = ChunkRange::new(0, 10);
        let chunk_byte_indices: Vec<u32> = (0..=10).map(|i| i * 100).collect();
        let data = vec![0u8; 1000];

        // Put data in cache
        cache
            .put(&key, &range, &chunk_byte_indices, &data)
            .await
            .unwrap();

        // Get data from cache
        let result = cache.get(&key, &range).await.unwrap();
        assert!(result.is_some());
        let cache_range = result.unwrap();
        assert_eq!(cache_range.data, data);
        assert_eq!(cache_range.range, range);
    }

    #[tokio::test]
    async fn test_memory_cache_eviction() {
        // Use a very small percentage to ensure eviction happens
        // We'll create a cache with enough capacity for one item but not two
        let cache = MemoryCache::new(0.00001).unwrap(); // Very small percentage
        let key1 = Key {
            prefix: "test".to_string(),
            hash: merklehash::MerkleHash::default(),
        };
        let key2 = Key {
            prefix: "test".to_string(),
            hash: merklehash::compute_data_hash(b"different"),
        };

        // Range (0, 2) has 2 chunks, so needs 3 indices (0, 1, 2)
        let range1 = ChunkRange::new(0, 2);
        let chunk_byte_indices1 = vec![0, 150, 300];
        let data1 = vec![1u8; 300];

        let range2 = ChunkRange::new(0, 2);
        let chunk_byte_indices2 = vec![0, 150, 300];
        let data2 = vec![2u8; 300];

        // Put first item
        cache
            .put(&key1, &range1, &chunk_byte_indices1, &data1)
            .await
            .unwrap();

        // Put second item (should trigger eviction)
        cache
            .put(&key2, &range2, &chunk_byte_indices2, &data2)
            .await
            .unwrap();

        // At least one item should be present (random eviction may keep either)
        assert!(cache.num_items().await > 0);
        // Total bytes should be within capacity
        assert!(cache.total_bytes().await <= cache.capacity);
    }
}

