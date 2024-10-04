pub mod error;

use cas_types::{Key, Range};
use error::ChunkCacheError;

pub trait ChunkCache {
    fn get(&mut self, key: &Key, range: &Range) -> Result<Option<Vec<u8>>, ChunkCacheError>;
    fn put(
        &mut self,
        key: &Key,
        range: &Range,
        chunk_byte_indicies: &[u32],
        data: &[u8],
    ) -> Result<(), ChunkCacheError>;
}
