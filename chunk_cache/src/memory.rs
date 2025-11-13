use std::collections::HashMap;
use std::sync::Arc;

use cas_types::ChunkRange;
use merklehash::MerkleHash;
use tokio::sync::RwLock;

use crate::CacheRange;
use crate::error::ChunkCacheError;

#[derive(Debug, Clone)]
struct MemoryCacheItem {
    range: ChunkRange,
    chunk_byte_indices: Vec<u32>,
    data: Vec<u8>,
    ttl: Vec<u64>,
}

#[derive(Debug, Clone, Default)]
struct CacheState {
    inner: HashMap<MerkleHash, Vec<MemoryCacheItem>>,
    num_items: usize,
    total_bytes: u64,
}

impl CacheState {
    fn find_match(&self, key: &MerkleHash, range: &ChunkRange) -> Option<&MemoryCacheItem> {
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

    pub async fn get(&self, key: &MerkleHash) -> Result<MemoryCacheItem, ChunkCacheError> {
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

    pub async fn put(
        &self,
        key: &MerkleHash,
        range: ChunkRange,
        chunk_byte_indices: Vec<u32>,
        data: Vec<u8>,
        ttl: Vec<u64>,
    ) -> Result<(), ChunkCacheError> {
        // Validate inputs
        if range.start >= range.end
            || chunk_byte_indices.len() != (range.end - range.start + 1) as usize
            || chunk_byte_indices.is_empty()
            || chunk_byte_indices[0] != 0
            || *chunk_byte_indices.last().unwrap() as usize != data.len()
            || !strictly_increasing(&chunk_byte_indices)
        {
            return Err(ChunkCacheError::InvalidArguments);
        }

        let data_len = data.len() as u64;

        let mut state = self.state.write().await;

        // Check if we already have this exact range cached
        if let Some(items) = state.inner.get(key) {
            for item in items.iter() {
                if item.range == range {
                    // Already cached
                    return Ok(());
                }
            }
        }

        // Add the new item
        let cache_item = MemoryCacheItem {
            range,
            chunk_byte_indices,
            data,
            ttl,
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
