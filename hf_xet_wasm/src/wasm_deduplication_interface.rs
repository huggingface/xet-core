use std::collections::HashMap;
use std::io::Cursor;
use std::result::Result as stdResult;
use std::sync::Arc;

use async_trait::async_trait;
use cas_client::CasClientError;
use deduplication::{DeduplicationDataInterface, RawXorbData};
use mdb_shard::file_structs::FileDataSequenceEntry;
use mdb_shard::shard_in_memory::MDBInMemoryShard;
use mdb_shard::MDBShardInfo;
use merklehash::{HMACKey, MerkleHash};
use progress_tracking::upload_tracking::FileXorbDependency;
use tokio_with_wasm::alias as wasmtokio;

use super::errors::*;
use super::wasm_file_upload_session::FileUploadSession;

pub struct UploadSessionDataManager {
    session: Arc<FileUploadSession>,
    shard: HashMap<HMACKey, MDBInMemoryShard>,
    query_tasks: wasmtokio::task::JoinSet<stdResult<Option<bytes::Bytes>, CasClientError>>,
}

impl UploadSessionDataManager {
    pub fn new(session: Arc<FileUploadSession>) -> Self {
        Self {
            session,
            shard: HashMap::default(),
            query_tasks: wasmtokio::task::JoinSet::new(),
        }
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
impl DeduplicationDataInterface for UploadSessionDataManager {
    type ErrorType = DataProcessingError;

    /// Query for possible shards that may dedup some chunks
    async fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> Result<Option<(usize, FileDataSequenceEntry, bool)>> {
        for (hmac_key, shard) in self.shard.iter() {
            let keyed_query_hashes: Vec<_> = query_hashes.iter().map(|h| h.hmac(*hmac_key)).collect();
            if let Some((count, fdse)) = shard.chunk_hash_dedup_query(&keyed_query_hashes) {
                return Ok(Some((count, fdse, true)));
            }
        }

        Ok(None)
    }

    /// Registers a new query for more information about the
    /// global deduplication.  This is expected to run in the background.
    async fn register_global_dedup_query(&mut self, chunk_hash: MerkleHash) -> Result<()> {
        let client = self.session.client.clone();
        let prefix = self.session.config.shard_config.prefix.clone();
        self.query_tasks.spawn(async move {
            client
                .query_for_global_dedup_shard(&prefix, &chunk_hash)
                .await
        });

        Ok(())
    }

    /// Waits for all the current queries to complete, then returns true if there is
    /// new deduplication information available.
    async fn complete_global_dedup_queries(&mut self) -> Result<bool> {
        let mut any_result = false;
        while let Some(ret) = self.query_tasks.join_next().await {
            let Some(serialized_shard) = ret.map_err(DataProcessingError::internal)?? else {
                continue;
            };
            let mut reader = Cursor::new(serialized_shard);
            let shard_info = MDBShardInfo::load_from_reader(&mut reader)?;

            let hmac_key = shard_info.metadata.chunk_hash_hmac_key;

            let cas_info = shard_info.read_all_cas_blocks_full(&mut reader)?;

            let keyed_shard = self.shard.entry(hmac_key).or_default();

            for ci in cas_info {
                let _ = keyed_shard.add_cas_block(ci);
            }

            any_result = true
        }

        Ok(any_result)
    }

    /// Registers a Xorb of new data that has no deduplication references.
    async fn register_new_xorb(&mut self, xorb: RawXorbData) -> Result<()> {
        // Add the xorb info to the current shard.  Note that we need to ensure all the xorb
        // uploads complete correctly before any shards get uploaded.
        self.session.session_shard.lock().await.add_cas_block(xorb.cas_info.clone())?;

        // Begin the process for upload.
        self.session.register_new_xorb_for_upload(xorb).await?;

        Ok(())
    }

    async fn register_xorb_dependencies(&mut self, _dependencies: &[FileXorbDependency]) {
        // Noop
    }
}
