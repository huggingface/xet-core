use async_trait::async_trait;
use bytes::Buf;
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use tracing::warn;

use cas::auth::AuthConfig;
use cas_client::build_reqwest_client;
use cas_types::Key;
use cas_types::{
    QueryChunkResponse, QueryReconstructionResponse, UploadShardResponse, UploadShardResponseType,
};
use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use mdb_shard::shard_dedup_probe::ShardDedupProber;
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use merklehash::MerkleHash;
use retry_strategy::RetryStrategy;

use crate::error::{Result, ShardClientError};
use crate::{RegistrationClient, ShardClientInterface};

const NUM_RETRIES: usize = 5;
const BASE_RETRY_DELAY_MS: u64 = 3000;

/// Shard Client that uses HTTP for communication.
#[derive(Debug)]
pub struct HttpShardClient {
    pub endpoint: String,
    client: ClientWithMiddleware,
    retry_strategy: RetryStrategy,
}

impl HttpShardClient {
    pub fn new(endpoint: &str, auth_config: &Option<AuthConfig>) -> Self {
        let client = build_reqwest_client(auth_config).unwrap();
        HttpShardClient {
            endpoint: endpoint.into(),
            client,
            // Retry policy: Exponential backoff starting at BASE_RETRY_DELAY_MS and retrying NUM_RETRIES times
            retry_strategy: RetryStrategy::new(NUM_RETRIES, BASE_RETRY_DELAY_MS),
        }
    }
}

fn retry_http_status_code(stat: &reqwest::StatusCode) -> bool {
    stat.is_server_error() || *stat == reqwest::StatusCode::TOO_MANY_REQUESTS
}

fn is_status_retriable_and_print(err: &reqwest_middleware::Error) -> bool {
    let ret = err
        .status()
        .as_ref()
        .map(retry_http_status_code)
        .unwrap_or(true); // network issues should be retried
    if ret {
        warn!("{err:?}. Retrying...");
    }
    ret
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

        let response = self
            .retry_strategy
            .retry(
                || async {
                    let url = url.clone();
                    match force_sync {
                        true => self.client.put(url).body(shard_data.to_vec()).send().await,
                        false => self.client.post(url).body(shard_data.to_vec()).send().await,
                    }
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(|e| ShardClientError::Other(format!("request failed with code {e}")))?;

        let response_body = response.bytes().await?;
        let response_parsed: UploadShardResponse = serde_json::from_reader(response_body.reader())?;

        match response_parsed.result {
            UploadShardResponseType::Exists => Ok(false),
            UploadShardResponseType::SyncPerformed => Ok(true),
        }
    }
}

#[async_trait]
impl FileReconstructor<ShardClientError> for HttpShardClient {
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        let url = Url::parse(&format!(
            "{}/reconstruction/{}",
            self.endpoint,
            file_hash.hex()
        ))?;
        let response = self
            .retry_strategy
            .retry(
                || async {
                    let url = url.clone();
                    self.client.get(url).send().await
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(|e| ShardClientError::Other(format!("request failed with code {e}")))?;

        let response_body = response.bytes().await?;
        let response_info: QueryReconstructionResponse =
            serde_json::from_reader(response_body.reader())?;

        Ok(Some((
            MDBFileInfo {
                metadata: FileDataSequenceHeader::new(
                    *file_hash,
                    response_info.reconstruction.len(),
                ),
                segments: response_info
                    .reconstruction
                    .into_iter()
                    .map(|ce| {
                        FileDataSequenceEntry::new(
                            ce.hash.into(),
                            ce.unpacked_length,
                            ce.range.start,
                            ce.range.end,
                        )
                    })
                    .collect(),
            },
            None,
        )))
    }
}

#[async_trait]
impl ShardDedupProber<ShardClientError> for HttpShardClient {
    async fn get_dedup_shards(
        &self,
        prefix: &str,
        chunk_hash: &[MerkleHash],
        _salt: &[u8; 32],
    ) -> Result<Vec<MerkleHash>> {
        debug_assert!(chunk_hash.len() == 1);

        // The API endpoint now only supports non-batched dedup request and
        // ignores salt.
        let key = Key {
            prefix: prefix.into(),
            hash: chunk_hash[0],
        };

        let url = Url::parse(&format!("{0}/chunk/{key}", self.endpoint))?;
        let response = self
            .retry_strategy
            .retry(
                || async {
                    let url = url.clone();
                    self.client.get(url).send().await
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(|e| ShardClientError::Other(format!("request failed with code {e}")))?;

        let response_body = response.bytes().await?;
        let response_info: QueryChunkResponse = serde_json::from_reader(response_body.reader())?;

        Ok(vec![response_info.shard])
    }
}

impl ShardClientInterface for HttpShardClient {}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use mdb_shard::{
        shard_dedup_probe::ShardDedupProber, shard_file_reconstructor::FileReconstructor,
        MDBShardFile, MDBShardInfo,
    };
    use merklehash::MerkleHash;

    use crate::RegistrationClient;

    use super::HttpShardClient;

    #[tokio::test]
    #[ignore = "need a local cas_server running"]
    async fn test_local() -> anyhow::Result<()> {
        let client = HttpShardClient::new("http://localhost:8080", &None);

        let path =
            PathBuf::from("./a7de567477348b23d23b667dba4d63d533c2ba7337cdc4297970bb494ba4699e.mdb");

        let shard_hash = MerkleHash::from_hex(
            "a7de567477348b23d23b667dba4d63d533c2ba7337cdc4297970bb494ba4699e",
        )?;

        let shard_data = std::fs::read(&path)?;

        let salt = [0u8; 32];

        client
            .upload_shard("default-merkledb", &shard_hash, true, &shard_data, &salt)
            .await?;

        let shard = MDBShardFile::load_from_file(&path)?;

        let mut reader = shard.get_reader()?;

        // test file reconstruction lookup
        let files = MDBShardInfo::read_file_info_ranges(&mut reader)?;
        for (file_hash, _) in files {
            let expected = shard.get_file_reconstruction_info(&file_hash)?.unwrap();
            let (result, _) = client
                .get_file_reconstruction_info(&file_hash)
                .await?
                .unwrap();

            assert_eq!(expected, result);
        }

        // test chunk dedup lookup
        let chunks = MDBShardInfo::read_cas_chunks_for_global_dedup(&mut reader)?;
        for chunk in chunks {
            let expected = shard_hash;
            let result = client
                .get_dedup_shards("default-merkledb", &[chunk], &salt)
                .await?;
            assert_eq!(expected, result[0]);
        }

        Ok(())
    }
}
