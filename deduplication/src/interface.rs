use std::result::Result;

use async_trait::async_trait;
use mdb_shard::file_structs::FileDataSequenceEntry;
use merklehash::MerkleHash;

use crate::raw_xorb_data::RawXorbData;

/// A trait given to the  
#[async_trait]
pub trait DeduplicationDataInterface: Send + Sync + 'static {
    // The error type used for the interface
    type ErrorType;

    /// Query for possible
    async fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> std::result::Result<Option<(usize, FileDataSequenceEntry)>, Self::ErrorType>;

    /// Registers a new query for more information about the
    /// global deduplication.  This is expected to run in the background.  Simply return Ok(()) to
    /// disable global dedup queries.
    async fn register_global_dedup_query(&mut self, _chunk_hash: MerkleHash) -> Result<(), Self::ErrorType>;

    /// Waits for all the current queries to complete, then returns true if there is
    /// new deduplication information available.
    async fn complete_global_dedup_queries(&mut self) -> Result<bool, Self::ErrorType>;

    /// Registers a Xorb of new data that has no deduplication references.
    async fn register_new_xorb(&mut self, xorb: RawXorbData) -> Result<(), Self::ErrorType>;
}
