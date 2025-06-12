use std::result::Result;

use async_trait::async_trait;
use mdb_shard::file_structs::FileDataSequenceEntry;
use merklehash::MerkleHash;
use progress_tracking::upload_tracking::FileXorbDependency;

use crate::raw_xorb_data::RawXorbData;

/// The interface needed for the deduplication routines to run.  To use the deduplication code,
/// define a struct that implements these methods.  This struct must be given by value to the FileDeduper
/// struct on creation.
///
/// The two primary methods are chunk_hash_dedup_query, which determines whether and how a chunk can be deduped,  
/// and register_new_xorb, which is called intermittently when a new block of data is available for upload.
///
/// The global dedup query functions are optional but needed if global dedup is to be enabled.
#[async_trait]
pub trait DeduplicationDataInterface: Send + Sync + 'static {
    /// The error type used for the interface
    type ErrorType;

    /// Query for possible shards that
    async fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> std::result::Result<Option<(usize, FileDataSequenceEntry, bool)>, Self::ErrorType>;

    /// Registers a new query for more information about the
    /// global deduplication.  This is expected to run in the background.  Simply return Ok(()) to
    /// disable global dedup queries.
    async fn register_global_dedup_query(&mut self, _chunk_hash: MerkleHash) -> Result<(), Self::ErrorType>;

    /// Waits for all the current queries to complete, then returns true if there is
    /// new deduplication information available.
    async fn complete_global_dedup_queries(&mut self) -> Result<bool, Self::ErrorType>;

    /// Registers a Xorb of new data that has no deduplication references.
    async fn register_new_xorb(&mut self, xorb: RawXorbData) -> Result<(), Self::ErrorType>;

    /// Register a set of xorb dependencies; this is called periodically during the dedup
    /// process with a list of (xorb hash, n_bytes).  As the final bit may get
    /// returned as a partial xorb without a hash yet, it is not gauranteed that the
    /// sum of the n_bytes across all the dependencies will equal the size of the file.
    async fn register_xorb_dependencies(&mut self, dependencies: &[FileXorbDependency]);
}
