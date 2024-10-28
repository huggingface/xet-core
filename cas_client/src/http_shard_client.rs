use std::io::Write;
use std::path::PathBuf;

use async_trait::async_trait;
use cas_types::{Key, QueryReconstructionResponse, UploadShardResponse, UploadShardResponseType};
use error_printer::ErrorPrinter;
use file_utils::SafeFileCreator;
use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use mdb_shard::shard_dedup_probe::ShardDedupProber;
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use merklehash::{HashedWrite, MerkleHash};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use utils::auth::AuthConfig;

use crate::error::{CasClientError, Result};
use crate::{build_auth_http_client, RegistrationClient, ShardClientInterface};

const FORCE_SYNC_METHOD: reqwest::Method = reqwest::Method::PUT;
const NON_FORCE_SYNC_METHOD: reqwest::Method = reqwest::Method::POST;

/// Shard Client that uses HTTP for communication.
#[derive(Debug)]
pub struct HttpShardClient {
    endpoint: String,
    client: ClientWithMiddleware,
    cache_directory: Option<PathBuf>,
}

impl HttpShardClient {
    pub fn new(endpoint: &str, auth_config: &Option<AuthConfig>, shard_cache_directory: Option<PathBuf>) -> Self {
        let client = build_auth_http_client(auth_config, &None).unwrap();
        HttpShardClient {
            endpoint: endpoint.into(),
            client,
            cache_directory: shard_cache_directory.clone(),
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
            .log_error("failed request to upload_shard")?
            .error_for_status()
            .log_error("error status on upload_shard")?;

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

        let response = self
            .client
            .get(url)
            .send()
            .await
            .log_error("error invoking reconstruction API")?
            .error_for_status()
            .log_error("reconstruction api error status")?;
        let response_info: QueryReconstructionResponse = response.json().await?;

        Ok(Some((
            MDBFileInfo {
                metadata: FileDataSequenceHeader::new(*file_hash, response_info.terms.len(), false),
                segments: response_info
                    .terms
                    .into_iter()
                    .map(|ce| {
                        FileDataSequenceEntry::new(ce.hash.into(), ce.unpacked_length, ce.range.start, ce.range.end)
                    })
                    .collect(),
                verification: vec![],
            },
            None,
        )))
    }
}

#[async_trait]
impl ShardDedupProber<CasClientError> for HttpShardClient {
    async fn get_dedup_shards(
        &self,
        prefix: &str,
        chunk_hash: &[MerkleHash],
        _salt: &[u8; 32],
    ) -> Result<Vec<MerkleHash>> {
        debug_assert!(chunk_hash.len() == 1);
        let Some(shard_cache_dir) = &self.cache_directory else {
            return Err(CasClientError::ConfigurationError("cache directory not configured for shard storage".into()));
        };

        // The API endpoint now only supports non-batched dedup request and
        // ignores salt.
        let key = Key {
            prefix: prefix.into(),
            hash: chunk_hash[0],
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
            return Ok(vec![]);
        }

        let writer = SafeFileCreator::new_unnamed()?;
        // Compute the actual hash to use as the shard file name
        let mut hashed_writer = HashedWrite::new(writer);

        while let Some(chunk) = response.chunk().await? {
            hashed_writer.write_all(&chunk)?;
        }
        hashed_writer.flush()?;

        let shard_hash = hashed_writer.hash();
        let file_name = local_shard_name(&shard_hash);
        let file_path = shard_cache_dir.join(file_name);
        let mut writer = hashed_writer.into_inner();
        writer.set_dest_path(file_path);
        writer.close()?;

        Ok(vec![shard_hash])
    }
}

/// Construct a file name for a MDBShard stored under cache and session dir.
fn local_shard_name(hash: &MerkleHash) -> PathBuf {
    PathBuf::from(hash.to_string()).with_extension("mdb")
}

impl ShardClientInterface for HttpShardClient {}

#[cfg(test)]
mod test {
    use std::env;
    use std::path::PathBuf;

    use mdb_shard::shard_dedup_probe::ShardDedupProber;
    use mdb_shard::shard_file_reconstructor::FileReconstructor;
    use mdb_shard::{MDBShardFile, MDBShardInfo};
    use merklehash::MerkleHash;

    use super::HttpShardClient;
    use crate::RegistrationClient;

    #[tokio::test]
    #[ignore = "need a local cas_server running"]
    async fn test_local() -> anyhow::Result<()> {
        let client = HttpShardClient::new("http://localhost:8080", &None, Some(env::current_dir()?));

        let path = PathBuf::from("./a7de567477348b23d23b667dba4d63d533c2ba7337cdc4297970bb494ba4699e.mdb");

        let shard_hash = MerkleHash::from_hex("a7de567477348b23d23b667dba4d63d533c2ba7337cdc4297970bb494ba4699e")?;

        let shard_data = std::fs::read(&path)?;

        let salt = [0u8; 32];

        client
            .upload_shard("default-merkledb", &shard_hash, true, &shard_data, &salt)
            .await?;

        let shard = MDBShardFile::load_from_file(&path)?;

        let mut reader = shard.get_reader()?;

        // test file reconstruction lookup
        let files = MDBShardInfo::read_file_info_ranges(&mut reader)?;
        for (file_hash, _, _) in files {
            let expected = shard.get_file_reconstruction_info(&file_hash)?.unwrap();
            let (result, _) = client.get_file_reconstruction_info(&file_hash).await?.unwrap();

            assert_eq!(expected, result);
        }

        // test chunk dedup lookup
        let chunks = MDBShardInfo::filter_cas_chunks_for_global_dedup(&mut reader)?;
        for chunk in chunks {
            let expected = shard_hash;
            let result = client.get_dedup_shards("default-merkledb", &[chunk], &salt).await?;
            assert_eq!(expected, result[0]);
        }

        Ok(())
    }
}
