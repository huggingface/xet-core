mod disk;
pub mod error;

use cas_types::{Key, Range};
use error::ChunkCacheError;

pub use disk::test_utils::*;
pub use disk::DiskCache;

pub trait ChunkCache {
    fn get(&self, key: &Key, range: &Range) -> Result<Option<Vec<u8>>, ChunkCacheError>;
    fn put(
        &self,
        key: &Key,
        range: &Range,
        chunk_byte_indicies: &[u32],
        data: &[u8],
    ) -> Result<(), ChunkCacheError>;
}
