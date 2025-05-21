mod cache_manager;
mod disk;
pub mod error;

use std::path::PathBuf;

use async_trait::async_trait;
pub use cache_manager::get_cache;
use cas_types::{ChunkRange, Key};
pub use disk::test_utils::*;
pub use disk::DiskCache;
use error::ChunkCacheError;
use mockall::automock;

pub use crate::disk::DEFAULT_CHUNK_CACHE_CAPACITY;

utils::configurable_constants! {
    ref CHUNK_CACHE_SIZE_BYTES: u64 = DEFAULT_CHUNK_CACHE_CAPACITY;
}

/// Return dto for cache gets
/// offsets has 1 more than then number of chunks in the specified range
/// suppose the range is for chunks [2, 5) then offsets may look like:
/// [0, 2000, 4000, 6000] where chunk 2 is made of bytes [0, 2000)
/// chunk 3 [2000, 4000) and chunk 4 is [4000, 6000).
/// It is guaranteed that the first number in offsets is 0 and the last number is data.len()
#[derive(Debug)]
pub struct CacheRange {
    pub offsets: Vec<u32>,
    pub data: Vec<u8>,
    pub range: ChunkRange,
}

/// ChunkCache is a trait for storing and fetching Xorb ranges.
/// implementors are expected to return bytes for a key and a given chunk range
/// (no compression or further deserialization should be required)
/// Range inputs use chunk indices in a end exclusive way i.e. [start, end)
///
/// implementors are allowed to evict data, a get after a put is not required to
/// be a cache hit.
#[automock]
#[async_trait]
pub trait ChunkCache: Sync + Send {
    /// get should return an Ok() variant if significant error occurred, check the error
    /// variant for issues with IO or parsing contents etc.
    ///
    /// if get returns an Ok(None) then there was no error, but there was a cache miss
    /// otherwise returns an Ok(Some(data)) where data matches exactly the bytes for
    /// the requested key and the requested chunk index range for that key
    ///
    /// Given implementors are expected to be able to evict members there's no guarantee
    /// that a previously put range will be a cache hit
    ///
    /// key is required to be a valid CAS Key
    /// range is intended to be an index range within the xorb with constraint
    ///     0 <= range.start < range.end <= num_chunks_in_xorb(key)
    async fn get(&self, key: &Key, range: &ChunkRange) -> Result<Option<CacheRange>, ChunkCacheError>;

    /// put should return Ok(()) if the put succeeded with no error, check the error
    /// variant for issues with validating the input, cache state, IO, etc.
    ///
    /// put expects that chunk_byte_indices.len() is range.end - range.start + 1
    /// with 1 entry for each start byte index for [range.start, range.end]
    /// the first entry must be 0 (start of first chunk in the data)
    /// the last entry must be data.len() i.e. the end of data, start of chunk past end
    ///
    /// key is required to be a valid CAS Key
    /// range is intended to be an index range within the xorb with constraint
    ///     0 <= range.start < range.end <= num_chunks_in_xorb(key)
    async fn put(
        &self,
        key: &Key,
        range: &ChunkRange,
        chunk_byte_indices: &[u32],
        data: &[u8],
    ) -> Result<(), ChunkCacheError>;
}

#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub cache_directory: PathBuf,
    pub cache_size: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig {
            cache_directory: PathBuf::from("/tmp"),
            cache_size: *CHUNK_CACHE_SIZE_BYTES,
        }
    }
}
