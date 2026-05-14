//! In-memory `SessionShardInterface` for wasm32-unknown-unknown.
//!
//! Mirrors the native API in `super::native` but keeps all shard data in
//! memory — no disk staging, no cache shard manager, no resume support.
//! Global-dedup queries (`query_dedup_shard_by_chunk`) are stubbed to
//! return `Ok(false)` because we have no cache to import the result into.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;
use xet_client::cas_client::Client;
use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::metadata_shard::file_structs::{FileDataSequenceEntry, MDBFileInfo};
use xet_core_structures::metadata_shard::shard_in_memory::MDBInMemoryShard;
use xet_core_structures::metadata_shard::xorb_structs::MDBXorbInfo;
use xet_runtime::core::XetContext;

use crate::error::Result;
use crate::processing::configurations::TranslatorConfig;

pub struct SessionShardInterface {
    #[allow(dead_code)]
    ctx: XetContext,
    client: Arc<dyn Client + Send + Sync>,
    dry_run: bool,
    session_shard: Mutex<MDBInMemoryShard>,
}

impl SessionShardInterface {
    pub async fn new(
        ctx: &XetContext,
        _config: Arc<TranslatorConfig>,
        client: Arc<dyn Client + Send + Sync>,
        dry_run: bool,
    ) -> Result<Self> {
        Ok(Self {
            ctx: ctx.clone(),
            client,
            dry_run,
            session_shard: Mutex::new(MDBInMemoryShard::default()),
        })
    }

    /// Global dedup queries are skipped on wasm — there is no cache shard
    /// manager to import the result into.
    pub async fn query_dedup_shard_by_chunk(&self, _chunk_hash: &MerkleHash) -> Result<bool> {
        Ok(false)
    }

    /// Query the in-memory session shard for chunk dedup. The third tuple
    /// element ("already uploaded") is always `false`: wasm has no resumed
    /// session support.
    pub async fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> Result<Option<(usize, FileDataSequenceEntry, bool)>> {
        let guard = self.session_shard.lock().await;
        Ok(guard.chunk_hash_dedup_query(query_hashes).map(|(n, fse)| (n, fse, false)))
    }

    pub async fn add_xorb_block(&self, xorb_block_contents: Arc<MDBXorbInfo>) -> Result<()> {
        let mut guard = self.session_shard.lock().await;
        guard.add_xorb_block(xorb_block_contents)?;
        Ok(())
    }

    /// No-op on wasm: the native impl uses this to stage xorb metadata on disk
    /// for crash recovery, which we don't support.
    pub async fn add_uploaded_xorb_block(&self, _xorb_block_contents: Arc<MDBXorbInfo>) -> Result<()> {
        Ok(())
    }

    pub async fn add_file_reconstruction_info(&self, file_info: MDBFileInfo) -> Result<()> {
        let mut guard = self.session_shard.lock().await;
        guard.add_file_reconstruction_info(file_info)?;
        Ok(())
    }

    pub async fn session_file_info_list(&self) -> Result<Vec<MDBFileInfo>> {
        let guard = self.session_shard.lock().await;
        Ok(guard.file_content.values().cloned().collect())
    }

    /// Serializes the in-memory shard and uploads it via the CAS client.
    /// Returns the byte count uploaded.
    pub async fn upload_and_register_session_shards(&self) -> Result<u64> {
        let (shard_data, n_bytes) = {
            let guard = self.session_shard.lock().await;
            if guard.is_empty() {
                return Ok(0);
            }
            let data = guard.to_bytes()?;
            let n = data.len() as u64;
            (data, n)
        };

        if self.dry_run {
            return Ok(n_bytes);
        }

        let permit = self.client.acquire_upload_permit().await?;
        self.client.upload_shard(Bytes::from(shard_data), permit).await?;
        Ok(n_bytes)
    }
}
