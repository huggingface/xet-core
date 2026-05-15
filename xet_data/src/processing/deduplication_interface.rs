use std::sync::Arc;

use async_trait::async_trait;
use tokio::task::JoinSet;
#[cfg(target_family = "wasm")]
use tokio_with_wasm::alias as tokio;
use tracing::Instrument;
use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::metadata_shard::file_structs::FileDataSequenceEntry;

use super::file_upload_session::FileUploadSession;
use crate::deduplication::{DeduplicationDataInterface, RawXorbData};
use crate::error::{DataError, Result};
use crate::progress_tracking::upload_tracking::FileXorbDependency;

pub struct UploadSessionDataManager {
    session: Arc<FileUploadSession>,
    active_global_dedup_queries: JoinSet<Result<bool>>,
}

impl UploadSessionDataManager {
    pub fn new(session: Arc<FileUploadSession>) -> Self {
        Self {
            session,
            active_global_dedup_queries: Default::default(),
        }
    }

    fn global_dedup_queries_enabled(&self) -> bool {
        self.session.ctx.config.deduplication.global_dedup_query_enabled
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
impl DeduplicationDataInterface for UploadSessionDataManager {
    type ErrorType = DataError;

    /// Query for possible shards that may dedup some chunks.
    async fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> Result<Option<(usize, FileDataSequenceEntry, bool)>> {
        Ok(self.session.shard_interface.chunk_hash_dedup_query(query_hashes).await?)
    }

    /// Registers a new query for more information about the
    /// global deduplication.  This is expected to run in the background.
    async fn register_global_dedup_query(&mut self, chunk_hash: MerkleHash) -> Result<()> {
        if !self.global_dedup_queries_enabled() {
            return Ok(());
        }

        // Now, query for a global dedup shard in the background to make sure that all the rest of this
        // can continue.
        let session: Arc<FileUploadSession> = self.session.clone();

        self.active_global_dedup_queries.spawn(
            async move {
                session.shard_interface.query_dedup_shard_by_chunk(&chunk_hash).await?;

                Ok(true)
            }
            .instrument(tracing::info_span!("UploadSessionDataManager::dedup_task")),
        );

        Ok(())
    }

    /// Waits for all the current queries to complete, then returns true if there is
    /// new deduplication information available.
    async fn complete_global_dedup_queries(&mut self) -> Result<bool> {
        if !self.global_dedup_queries_enabled() {
            return Ok(false);
        }

        let mut any_result = false;
        while let Some(result) = self.active_global_dedup_queries.join_next().await {
            any_result |= result??;
        }
        Ok(any_result)
    }

    /// Registers a Xorb of new data that has no deduplication references.
    async fn register_new_xorb(&mut self, xorb: RawXorbData) -> Result<()> {
        // Begin the process for upload.
        self.session.register_new_xorb(xorb, &[]).await?;

        Ok(())
    }

    /// Periodically registers xorb dependencies; used for progress tracking.
    async fn register_xorb_dependencies(&mut self, dependencies: &[FileXorbDependency]) {
        self.session.register_xorb_dependencies(dependencies);
    }
}
