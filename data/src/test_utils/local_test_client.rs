use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use cas_client::tests_utils::*;
use cas_client::{CasClientError, Client, LocalClient, ReconstructionClient, UploadClient};
use cas_types::FileRange;
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::ShardFileManager;
use merklehash::MerkleHash;
use reqwest_middleware::ClientWithMiddleware;

/// A CAS client only for the purpose of testing. It utilizes LocalClient to upload
/// and download xorbs and ShardFileManager to retrieve file reconstruction info.
pub struct LocalTestClient {
    prefix: String,
    cas: LocalClient,
    shard_manager: Arc<ShardFileManager>,
}

impl LocalTestClient {
    pub fn new(prefix: &str, path: &Path, shard_manager: Arc<ShardFileManager>) -> Self {
        let cas = LocalClient::new(path.to_path_buf());
        Self {
            prefix: prefix.to_owned(),
            cas,
            shard_manager,
        }
    }
}

#[async_trait]
impl UploadClient for LocalTestClient {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_and_boundaries: Vec<(MerkleHash, u32)>,
    ) -> Result<(), CasClientError> {
        self.cas.put(prefix, hash, data, chunk_and_boundaries).await
    }

    async fn exists(&self, prefix: &str, hash: &MerkleHash) -> Result<bool, CasClientError> {
        self.cas.exists(prefix, hash).await
    }
}

#[async_trait]
impl ReconstructionClient for LocalTestClient {
    async fn get_file(
        &self,
        _http_client: Arc<ClientWithMiddleware>,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        writer: &mut Box<dyn Write + Send>,
    ) -> Result<(), CasClientError> {
        let Some((file_info, _)) = self
            .shard_manager
            .get_file_reconstruction_info(hash)
            .await
            .map_err(|e| anyhow!("{e}"))?
        else {
            return Err(CasClientError::FileNotFound(*hash));
        };

        for entry in file_info.segments {
            let Some(one_range) = self
                .cas
                .get_object_range(
                    &self.prefix,
                    &entry.cas_hash,
                    vec![(entry.chunk_index_start, entry.chunk_index_end)],
                )?
                .pop()
            else {
                return Err(CasClientError::InvalidRange);
            };
            let start = byte_range.as_ref().map(|range| range.start as usize).unwrap_or(0);
            let end = byte_range.as_ref().map(|range| range.end as usize).unwrap_or(one_range.len());

            writer.write_all(&one_range[start..end])?;
        }

        Ok(())
    }
}

impl Client for LocalTestClient {}
