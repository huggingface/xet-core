use std::path::PathBuf;

use async_trait::async_trait;
use merklehash::MerkleHash;

#[async_trait]
pub trait ShardDedupProber<E> {
    /// Probes for shards that provide dedup information for a chunk, and, if
    /// any are found, writes them to the destination directory given and returns
    /// their file handles.  
    ///
    /// Returns true if new shards were found.
    async fn get_dedup_shards(
        &self,
        prefix: &str,
        chunk_hash: &[MerkleHash],
        salt: &[u8; 32],
    ) -> Result<Vec<PathBuf>, E>;
}
