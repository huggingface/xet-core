//! In-memory `SessionShardInterface` for wasm32-unknown-unknown.
//!
//! Mirrors the native API in `super::native` but keeps all shard data in
//! memory — no disk staging, no resume support. Global-dedup shards
//! fetched from CAS are parsed in-process and held in a small bounded
//! FIFO `dedup_cache`, separate from the session shard so they are not
//! re-uploaded.

use std::collections::VecDeque;
use std::io::Cursor;
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

/// Maximum number of fetched global-dedup shards held in `dedup_cache`.
/// At ~3-4k chunk lookup entries per shard this bounds the cache at a
/// few MB of metadata, which is comfortable for browser sessions.
const DEDUP_CACHE_MAX_SHARDS: usize = 32;

pub struct SessionShardInterface {
    ctx: XetContext,
    client: Arc<dyn Client + Send + Sync>,
    dry_run: bool,
    session_shard: Mutex<MDBInMemoryShard>,
    /// Bounded FIFO of shards fetched via global dedup queries; the oldest
    /// shard is evicted on overflow. Holds individual shards (not a single
    /// union) so eviction doesn't lose every fetched shard at once.
    dedup_cache: Mutex<VecDeque<MDBInMemoryShard>>,
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
            dedup_cache: Mutex::new(VecDeque::with_capacity(DEDUP_CACHE_MAX_SHARDS)),
        })
    }

    /// Fetch a global-dedup shard from CAS for the given chunk hash, parse
    /// it in memory, and push it into the bounded FIFO `dedup_cache`. CAS
    /// errors are logged and swallowed (matching native's behavior); the
    /// caller treats them as "no shard available."
    pub async fn query_dedup_shard_by_chunk(&self, chunk_hash: &MerkleHash) -> Result<bool> {
        let shard_bytes = match self
            .client
            .query_for_global_dedup_shard(&self.ctx.config.data.default_prefix, chunk_hash)
            .await
        {
            Ok(Some(b)) => b,
            Ok(None) => return Ok(false),
            Err(e) => {
                tracing::warn!(error = ?e, "global dedup query failed");
                return Ok(false);
            },
        };

        let fetched = MDBInMemoryShard::from_reader(&mut Cursor::new(shard_bytes.as_ref()))?;

        let mut guard = self.dedup_cache.lock().await;
        guard.push_back(fetched);
        while guard.len() > DEDUP_CACHE_MAX_SHARDS {
            guard.pop_front();
        }
        Ok(true)
    }

    /// Query the in-memory session shard first, then the global-dedup cache.
    /// The third tuple element ("already uploaded") is `false` for session
    /// hits and `true` for dedup-cache hits, since cache entries come from
    /// shards the server already has.
    pub async fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> Result<Option<(usize, FileDataSequenceEntry, bool)>> {
        {
            let guard = self.session_shard.lock().await;
            if let Some((n, fse)) = guard.chunk_hash_dedup_query(query_hashes) {
                return Ok(Some((n, fse, false)));
            }
        }

        let guard = self.dedup_cache.lock().await;
        Ok(guard
            .iter()
            .find_map(|shard| shard.chunk_hash_dedup_query(query_hashes))
            .map(|(n, fse)| (n, fse, true)))
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
