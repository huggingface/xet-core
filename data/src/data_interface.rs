use async_trait::async_trait;
use deduplication::DataInterface;

use crate::file_upload_session::FileUploadSession;
use crate::repo_salt::RepoSalt;

pub struct UploadSessionDataManager {
    session: Arc<FileUploadSession>,

    active_global_dedup_queries: JoinSet<bool>,
}

impl UploadSessionDataManager {
    pub fn new(session: Arc<FileUploadSession>) -> Self {
        Self {
            session,
            active_global_dedup_queries: Default::default(),
        }
    }

    fn global_dedup_queries_enabled(&self) -> bool {
        todo!()
    }
}

#[async_trait]
impl DataInterface for UploadSessionDataManager {
    type ErrorType = crate::errors::DataProcessingError;

    /// Query for possible
    async fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> std::result::Result<Option<(usize, FileDataSequenceEntry)>, Self::ErrorType> {
        Ok(self.session.shard_manager.chunk_hash_dedup_query(query_hashes).await?)
    }

    /// Registers a new query for more information about the
    /// global deduplication.  This is expected to run in the background.
    async fn register_global_dedup_query(&mut self, chunk_hash: MerkleHash) -> Result<(), Self::ErrorType> {
        if !self.global_dedup_queries_enabled() {
            return Ok(());
        }

        // Now, query for a global dedup shard in the background to make sure that all the rest of this
        // can continue.
        let query_chunk = chunk_hashes[local_chunk_index];

        let session = self.session.clone();

        self.active_global_dedup_queries.spawn(async move {
            let repo_salt: RepoSalt = session.config.shard_config.repo_salt;

            Ok(true)
        });

        Ok(())
    }

    /// Waits for all the current queries to complete, then returns true if there is
    /// new deduplication information available.
    async fn complete_global_dedup_queries(&mut self) -> Result<bool, Self::ErrorType> {
        if !self.global_dedup_queries_enabled() {
            return Ok(false);
        }

        let mut any_result = false;
        while let Some(result) = self.active_global_dedup_queries.join_next().await {
            any_result |= result?;
        }
        Ok(any_result)
    }

    /// Registers a Xorb of new data that has no deduplication references.
    async fn register_new_xorb(&mut self, xorb: RawXorbData) -> Result<(), Self::ErrorType> {
        // Add the xorb info to the current shard.  Note that we need to ensure all the xorb
        // uploads complete correctly before any shards get uploaded.
        self.session.shard_manager.add_cas_block(xorb.cas_info.clone()).await?;

        // Begin the process for upload.
        self.session.xorb_uploader.register_new_xorb_for_upload(xorb)?;

        Ok(())
    }
}
