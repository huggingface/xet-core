use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use cas_types::{ChunkRange, Key};
use tokio::sync::RwLock;

use crate::error::ChunkCacheError;
use crate::{CacheRange, ChunkCache};

#[derive(Debug, Clone)]
struct MemoryCacheItem {
    range: ChunkRange,
    chunk_byte_indices: Vec<u32>,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
struct CacheState {
    inner: HashMap<Key, Vec<MemoryCacheItem>>,
    num_items: usize,
    total_bytes: u64,
}

impl CacheState {
    fn find_match(&self, key: &Key, range: &ChunkRange) -> Option<&MemoryCacheItem> {
        let items = self.inner.get(key)?;

        items
            .iter()
            .find(|&item| item.range.start <= range.start && range.end <= item.range.end)
            .map(|v| v as _)
    }
}

/// MemoryCache is a ChunkCache implementor that stores data in memory
#[derive(Debug, Clone, Default)]
pub struct MemoryCache {
    state: Arc<RwLock<CacheState>>,
}

impl MemoryCache {
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

        // Add the new item
        let cache_item = MemoryCacheItem {
            range: *range,
            chunk_byte_indices: chunk_byte_indices.to_vec(),
            data: data.to_vec(),
        };

        state.total_bytes += data_len;
        state.num_items += 1;

        state.inner.entry(key.clone()).or_insert_with(Vec::new).push(cache_item);

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

    #[tokio::test]
    async fn test_memory_cache_basic() {
        let cache = MemoryCache::default();
        let key = Key {
            prefix: "test".to_string(),
            hash: merklehash::MerkleHash::default(),
        };
        let range = ChunkRange::new(0, 10);
        let chunk_byte_indices: Vec<u32> = (0..=10).map(|i| i * 100).collect();
        let data = vec![0u8; 1000];

        // Put data in cache
        cache.put(&key, &range, &chunk_byte_indices, &data).await.unwrap();

        // Get data from cache
        let result = cache.get(&key, &range).await.unwrap();
        assert!(result.is_some());
        let cache_range = result.unwrap();
        assert_eq!(cache_range.data, data);
        assert_eq!(cache_range.range, range);
    }

    #[tokio::test]
    async fn test_memory_cache_invalid_inputs() {
        let cache = MemoryCache::default();
        let key = Key {
            prefix: "test".to_string(),
            hash: merklehash::MerkleHash::default(),
        };

        // Test invalid range (start >= end)
        let invalid_range = ChunkRange::new(10, 5);
        let data = vec![0u8; 100];
        let chunk_byte_indices = vec![0, 50, 100];
        assert!(cache.put(&key, &invalid_range, &chunk_byte_indices, &data).await.is_err());

        // Test non-increasing chunk indices
        let range = ChunkRange::new(0, 2);
        let invalid_indices = vec![0, 50, 20];
        assert!(cache.put(&key, &range, &invalid_indices, &data).await.is_err());

        // Test empty indices
        let empty_indices: Vec<u32> = vec![];
        assert!(cache.put(&key, &range, &empty_indices, &data).await.is_err());

        // Test non-zero first index
        let invalid_start = vec![10, 50, 100];
        assert!(cache.put(&key, &range, &invalid_start, &data).await.is_err());

        // Test last index != data length
        let invalid_end = vec![0, 50, 80];
        assert!(cache.put(&key, &range, &invalid_end, &data).await.is_err());
    }

    #[tokio::test]
    async fn test_memory_cache_duplicate_put() {
        let cache = MemoryCache::default();
        let key = Key {
            prefix: "test".to_string(),
            hash: merklehash::MerkleHash::default(),
        };
        let range = ChunkRange::new(0, 2);
        let chunk_byte_indices = vec![0, 50, 100];
        let data = vec![0u8; 100];

        // First put should succeed
        cache.put(&key, &range, &chunk_byte_indices, &data).await.unwrap();
        let initial_items = cache.num_items().await;
        let initial_bytes = cache.total_bytes().await;

        // Second put of same range should not increase counters
        cache.put(&key, &range, &chunk_byte_indices, &data).await.unwrap();
        assert_eq!(cache.num_items().await, initial_items);
        assert_eq!(cache.total_bytes().await, initial_bytes);
    }

    #[tokio::test]
    async fn test_memory_cache_partial_range() {
        let cache = MemoryCache::default();
        let key = Key {
            prefix: "test".to_string(),
            hash: merklehash::MerkleHash::default(),
        };
        let range = ChunkRange::new(0, 2);
        let chunk_byte_indices = vec![0, 50, 100];
        let data = vec![1u8; 100];

        cache.put(&key, &range, &chunk_byte_indices, &data).await.unwrap();

        // Get partial range
        let partial_range = ChunkRange::new(1, 2);
        let result = cache.get(&key, &partial_range).await.unwrap().unwrap();
        assert_eq!(result.range, partial_range);
        assert_eq!(result.data.len(), 50);
        assert_eq!(result.offsets, vec![0, 50]);
    }
}
