use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use cas_types::FileRange;
use mdb_shard::file_structs::MDBFileInfo;
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use merklehash::MerkleHash;
use utils::progress::ProgressUpdater;

use crate::error::Result;
use crate::interface::{RegistrationClient, ShardDedupProber};
use crate::{CasClientError, Client, ReconstructionClient, ShardClientInterface, UploadClient};

/// A client that only performs downloads and queries, but swallows all the upload calls
/// without doing anything.
pub struct DryRunClient(Arc<dyn Client + Send + Sync>);

impl DryRunClient {
    pub fn new(client: Arc<dyn Client + Send + Sync>) -> Self {
        Self(client)
    }
}

// Just pass things through to the RemoteClient, except for all put and upload requests.
#[async_trait]
impl UploadClient for DryRunClient {
    async fn put(
        &self,
        _prefix: &str,
        _hash: &MerkleHash,
        _data: Vec<u8>,
        _chunk_and_boundaries: Vec<(MerkleHash, u32)>,
    ) -> Result<usize> {
        Ok(0)
    }
    async fn exists(&self, prefix: &str, hash: &MerkleHash) -> Result<bool> {
        self.0.exists(prefix, hash).await
    }
}

#[async_trait]
impl ReconstructionClient for DryRunClient {
    async fn get_file(
        &self,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        writer: &mut Box<dyn Write + Send>,
        progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Result<()> {
        self.0.get_file(hash, byte_range, writer, progress_updater).await
    }

    async fn batch_get_file(&self, files: HashMap<MerkleHash, &mut Box<dyn Write + Send>>) -> Result<()> {
        self.0.batch_get_file(files).await
    }
}

#[async_trait]
impl RegistrationClient for DryRunClient {
    async fn upload_shard(
        &self,
        _prefix: &str,
        _hash: &MerkleHash,
        _force_sync: bool,
        _shard_data: &[u8],
        _salt: &[u8; 32],
    ) -> Result<bool> {
        Ok(true)
    }
}

#[async_trait]
impl FileReconstructor<CasClientError> for DryRunClient {
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        self.0.get_file_reconstruction_info(file_hash).await
    }
}

#[async_trait]
impl ShardDedupProber for DryRunClient {
    async fn query_for_global_dedup_shard(
        &self,
        prefix: &str,
        chunk_hash: &MerkleHash,
        salt: &[u8; 32],
    ) -> Result<Option<PathBuf>> {
        self.0.query_for_global_dedup_shard(prefix, chunk_hash, salt).await
    }
}

// Finally we can just declare this.
impl ShardClientInterface for DryRunClient {}
impl Client for DryRunClient {}
