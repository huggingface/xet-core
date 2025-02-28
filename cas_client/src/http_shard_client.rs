use std::io::Write;
use std::path::PathBuf;

use async_trait::async_trait;
use cas_types::{Key, QueryReconstructionResponse, UploadShardResponse, UploadShardResponseType};
use error_printer::ErrorPrinter;
use file_utils::SafeFileCreator;
use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::utils::shard_file_name;
use merklehash::{HashedWrite, MerkleHash};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use utils::auth::AuthConfig;

use crate::error::{CasClientError, Result};
use crate::http_client::ResponseErrorLogger;
use crate::interface::ShardDedupProber;
use crate::{build_auth_http_client, RegistrationClient, ShardClientInterface};

const FORCE_SYNC_METHOD: reqwest::Method = reqwest::Method::PUT;
const NON_FORCE_SYNC_METHOD: reqwest::Method = reqwest::Method::POST;

/// Shard Client that uses HTTP for communication.
#[derive(Debug)]
pub struct HttpShardClient {
    endpoint: String,
    client: ClientWithMiddleware,
    shard_cache_directory: PathBuf,
}

impl HttpShardClient {
    pub fn new(endpoint: &str, auth_config: &Option<AuthConfig>, shard_cache_directory: PathBuf) -> Self {
        let client = build_auth_http_client(auth_config, &None).unwrap();
        HttpShardClient {
            endpoint: endpoint.into(),
            client,
            shard_cache_directory,
        }
    }
}

#[async_trait]
impl RegistrationClient for HttpShardClient {
    async fn upload_shard(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        force_sync: bool,
        shard_data: &[u8],
        _salt: &[u8; 32],
    ) -> Result<bool> {
        let key = Key {
            prefix: prefix.into(),
            hash: *hash,
        };

        let url = Url::parse(&format!("{}/shard/{key}", self.endpoint))?;

        let method = match force_sync {
            true => FORCE_SYNC_METHOD,
            false => NON_FORCE_SYNC_METHOD,
        };

        let response = self
            .client
            .request(method, url)
            .body(shard_data.to_vec())
            .send()
            .await
            .process_error("upload_shard")?;

        let response_parsed: UploadShardResponse =
            response.json().await.log_error("error json decoding upload_shard response")?;

        match response_parsed.result {
            UploadShardResponseType::Exists => Ok(false),
            UploadShardResponseType::SyncPerformed => Ok(true),
        }
    }
}

#[async_trait]
impl FileReconstructor<CasClientError> for HttpShardClient {
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        let url = Url::parse(&format!("{}/reconstruction/{}", self.endpoint, file_hash.hex()))?;

        let response = self.client.get(url).send().await.process_error("get_reconstruction_info")?;
        let response_info: QueryReconstructionResponse = response.json().await?;

        Ok(Some((
            MDBFileInfo {
                metadata: FileDataSequenceHeader::new(*file_hash, response_info.terms.len(), false, false),
                segments: response_info
                    .terms
                    .into_iter()
                    .map(|ce| {
                        FileDataSequenceEntry::new(ce.hash.into(), ce.unpacked_length, ce.range.start, ce.range.end)
                    })
                    .collect(),
                verification: vec![],
                metadata_ext: None,
            },
            None,
        )))
    }
}

#[async_trait]
impl ShardDedupProber for HttpShardClient {
    async fn query_for_global_dedup_shard(
        &self,
        prefix: &str,
        chunk_hash: &MerkleHash,
        _salt: &[u8; 32],
    ) -> Result<Option<PathBuf>> {
        // The API endpoint now only supports non-batched dedup request and
        // ignores salt.
        let key = Key {
            prefix: prefix.into(),
            hash: *chunk_hash,
        };

        let url = Url::parse(&format!("{0}/chunk/{key}", self.endpoint))?;

        let mut response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| CasClientError::Other(format!("request failed with error {e}")))?;

        // Dedup shard not found, return empty result
        if !response.status().is_success() {
            return Ok(None);
        }

        let writer = SafeFileCreator::new_unnamed()?;
        // Compute the actual hash to use as the shard file name
        let mut hashed_writer = HashedWrite::new(writer);

        while let Some(chunk) = response.chunk().await? {
            hashed_writer.write_all(&chunk)?;
        }
        hashed_writer.flush()?;

        let shard_hash = hashed_writer.hash();
        let file_path = self.shard_cache_directory.join(shard_file_name(&shard_hash));
        let mut writer = hashed_writer.into_inner();
        writer.set_dest_path(&file_path);
        writer.close()?;

        Ok(Some(file_path))
    }
}

impl ShardClientInterface for HttpShardClient {}
