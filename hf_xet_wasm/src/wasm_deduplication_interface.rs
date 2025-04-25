use std::sync::Arc;

use async_trait::async_trait;
use deduplication::{DeduplicationDataInterface, RawXorbData};
use mdb_shard::file_structs::FileDataSequenceEntry;
use merklehash::MerkleHash;

use super::errors::*;
use super::wasm_file_upload_session::FileUploadSession;

pub struct UploadSessionDataManager {
    session: Arc<FileUploadSession>,
}

impl UploadSessionDataManager {
    pub fn new(session: Arc<FileUploadSession>) -> Self {
        Self { session }
    }
}

#[async_trait]
impl DeduplicationDataInterface for UploadSessionDataManager {
    type ErrorType = DataProcessingError;

    /// Query for possible shards that may dedup some chunks
    async fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> Result<Option<(usize, FileDataSequenceEntry)>> {
        todo!()
    }

    /// Registers a new query for more information about the
    /// global deduplication.  This is expected to run in the background.
    async fn register_global_dedup_query(&mut self, chunk_hash: MerkleHash) -> Result<()> {
        todo!()
    }

    /// Waits for all the current queries to complete, then returns true if there is
    /// new deduplication information available.
    async fn complete_global_dedup_queries(&mut self) -> Result<bool> {
        todo!()
    }

    /// Registers a Xorb of new data that has no deduplication references.
    async fn register_new_xorb(&mut self, xorb: RawXorbData) -> Result<()> {
        todo!()
    }
}
