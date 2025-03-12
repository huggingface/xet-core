use std::result::Result;

use mdb_shard::file_structs::FileDataSequenceEntry;
use merklehash::MerkleHash;

pub trait ChunkDeduplicator<E> {
    /// Query for the
    fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> std::result::Result<Option<(usize, FileDataSequenceEntry)>, E>;

    /// Registers a new query for more information about the
    /// global deduplication.  This is expected to run in the background,
    fn register_global_dedup_query(&mut self, _chunk_hash: MerkleHash) {}

    /// Waits for all the current queries to  
    fn complete_global_dedup_queries(&mut self) -> Result<bool, E> {
        Ok(false)
    }
}

// This will need to be reimplemented on the caller side.

/*
pub struct ChunkDeduplicator {

                        // Now, query for a global dedup shard in the background to make sure that all the rest of this
                        // can continue.
                        let remote_shards = self.remote_shards.clone();
                        let query_chunk = chunk_hashes[local_chunk_index];

                        let file_name = self.file_name.clone();

                        global_dedup_queries.spawn(async move {
                                let Ok(query_result) = remote_shards.query_dedup_shard_by_chunk(&query_chunk, &salt).await.map_err(|e| {
                                    debug!("Error encountered attempting to query global dedup table: {e:?}; ignoring.");
                                    e
                                })
                                    else { return Ok(false); };

                                let Some(new_shard_file) = query_result else {
                                    debug!("Queried shard for global dedup with hash {query_chunk:?}; nothing found.");
                                    return Ok(false);
                                };

                                // The above process found something and downloaded it; it should now be in the cache directory and valid
                                // for deduplication.  Register it and restart the dedup process at the start of this chunk.
                                debug!("global dedup: {file_name:?} deduplicated by shard {new_shard_file:?}; registering.");
                                ShardFileManager::register_shard_in_existing_managers(&new_shard_file).await?;

                                debug!("global dedup: New shard {new_shard_file:?} can be used for deduplication of {file_name:?}; reprocessing file.");

                                Ok(true)
                            });

                        }
                        */
