use crate::http_client;
use crate::interface::*;
use crate::Client;
use crate::{error::Result, CasClientError};
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Buf;
use bytes::Bytes;
use cas_object::CasObject;
use cas_types::{CASReconstructionTerm, Key, QueryReconstructionResponse, UploadXorbResponse};
use chunk_cache::{CacheConfig, ChunkCache, DiskCache};
use error_printer::ErrorPrinter;
use merklehash::MerkleHash;
use reqwest::{StatusCode, Url};
use reqwest_middleware::ClientWithMiddleware;
use std::io::{Cursor, Write};
use std::sync::Arc;
use tracing::info;
use tracing::{debug, error};
use utils::auth::AuthConfig;

pub const CAS_ENDPOINT: &str = "http://localhost:8080";
pub const PREFIX_DEFAULT: &str = "default";

const NUM_RETRIES: usize = 5;
const BASE_RETRY_DELAY_MS: u64 = 3000;

pub struct RemoteClient {
    endpoint: String,
    http_auth_client: ClientWithMiddleware,
    disk_cache: Option<Arc<dyn ChunkCache>>,
}

impl RemoteClient {
    pub fn new(
        endpoint: &str,
        auth: &Option<AuthConfig>,
        cache_config: &Option<CacheConfig>,
    ) -> Self {
        // use disk cache if cache_config provided.
        let disk_cache = if let Some(cache_config) = cache_config {
            info!(
                "Using disk cache directory: {:?}, size: {}.",
                cache_config.cache_directory, cache_config.cache_size
            );
            DiskCache::initialize(cache_config)
                .log_error("failed to initialize cache, not using cache")
                .ok().map(|disk_cache| Arc::new(disk_cache) as Arc<dyn ChunkCache>)
        } else {
            None
        };

        Self {
            endpoint: endpoint.to_string(),
            http_auth_client: http_client::build_auth_http_client(auth, &None).unwrap(),
            disk_cache,
        }
    }
}

#[async_trait]
impl UploadClient for RemoteClient {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_and_boundaries: Vec<(MerkleHash, u32)>,
    ) -> Result<()> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: *hash,
        };

        let was_uploaded = self.upload(&key, &data, chunk_and_boundaries).await?;

        if !was_uploaded {
            debug!("{key:?} not inserted into CAS.");
        } else {
            debug!("{key:?} inserted into CAS.");
        }

        Ok(())
    }

    async fn exists(&self, prefix: &str, hash: &MerkleHash) -> Result<bool> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: *hash,
        };

        let url = Url::parse(&format!("{}/xorb/{key}", self.endpoint))?;
        let response = self.http_auth_client.head(url).send().await?;
        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            e => Err(CasClientError::InternalError(anyhow!(
                "unrecognized status code {e}"
            ))),
        }
    }
}

#[async_trait]
impl ReconstructionClient for RemoteClient {
    async fn get_file(
        &self,
        http_client: &ClientWithMiddleware,
        hash: &MerkleHash,
        writer: &mut Box<dyn Write + Send>,
    ) -> Result<()> {
        // get manifest of xorbs to download
        let manifest = self.reconstruct(hash, None).await?;

        self.get_ranges(http_client, manifest, None, writer).await?;

        Ok(())
    }

    #[allow(unused_variables)]
    async fn get_file_byte_range(
        &self,
        http_client: &ClientWithMiddleware,
        hash: &MerkleHash,
        offset: u64,
        length: u64,
        writer: &mut Box<dyn Write + Send>,
    ) -> Result<()> {
        todo!()
    }
}

#[async_trait]
impl Reconstructable for RemoteClient {
    async fn reconstruct(
        &self,
        file_id: &MerkleHash,
        _bytes_range: Option<(u64, u64)>,
    ) -> Result<QueryReconstructionResponse> {
        let url = Url::parse(&format!(
            "{}/reconstruction/{}",
            self.endpoint,
            file_id.hex()
        ))?;

        let response = self.http_auth_client.get(url).send().await?;

        let response_body = response.bytes().await?;
        let response_parsed: QueryReconstructionResponse =
            serde_json::from_reader(response_body.reader())
                .map_err(|_| CasClientError::FileNotFound(*file_id))?;

        Ok(response_parsed)
    }
}

impl Client for RemoteClient {}

impl RemoteClient {
    pub async fn upload(
        &self,
        key: &Key,
        contents: &[u8],
        chunk_and_boundaries: Vec<(MerkleHash, u32)>,
    ) -> Result<bool> {
        let url = Url::parse(&format!("{}/xorb/{key}", self.endpoint))?;

        let mut writer = Cursor::new(Vec::new());

        let (_, _) = CasObject::serialize(
            &mut writer,
            &key.hash,
            contents,
            &chunk_and_boundaries,
            cas_object::CompressionScheme::LZ4,
        )?;

        debug!("Upload: POST to {url:?} for {key:?}");
        writer.set_position(0);
        let data = writer.into_inner();

        let response = self.http_auth_client.post(url).body(data).send().await?;
        let response_body = response.bytes().await?;
        let response_parsed: UploadXorbResponse = serde_json::from_reader(response_body.reader())?;

        Ok(response_parsed.was_inserted)
    }

    async fn get_ranges(
        &self,
        http_client: &ClientWithMiddleware,
        reconstruction_response: QueryReconstructionResponse,
        _byte_range: Option<(u64, u64)>,
        writer: &mut Box<dyn Write + Send>,
    ) -> Result<usize> {
        let info = reconstruction_response.reconstruction;
        let total_len = info.iter().fold(0, |acc, x| acc + x.unpacked_length);
        let futs = info.into_iter().map(|term| {
            let http_client_clone = http_client.clone();
            let disk_cache = self.disk_cache.clone();
            tokio::spawn(async move {
                get_one_range(&http_client_clone, disk_cache.clone(), &term).await
            })
        });
        for fut in futs {
            let piece = fut
                .await
                .map_err(|e| CasClientError::InternalError(anyhow!("join error {e}")))??;
            writer.write_all(&piece)?;
        }
        Ok(total_len as usize)
    }
}

pub(crate) async fn get_one_range(
    http_client: &ClientWithMiddleware,
    disk_cache: Option<Arc<dyn ChunkCache>>,
    term: &CASReconstructionTerm,
) -> Result<Bytes> {
    debug!("term: {term:?}");

    if term.range.end < term.range.start || term.url_range.end < term.url_range.start {
        return Err(CasClientError::InvalidRange);
    }

    // check disk cache
    if let Some(cache) = &disk_cache {
        let key = Key {
            prefix: PREFIX_DEFAULT.to_string(),
            hash: term.hash.into(),
        };

        let cached = cache
            .get(&key, &term.range)
            .map_err(|e| CasClientError::InternalError(anyhow!("cache error {e}")))?;
        if let Some(cached) = cached {
            return Ok(Bytes::from(cached));
        }
    }

    let url = Url::parse(term.url.as_str())?;

    let response = http_client
        .get(url)
        .header(
            reqwest::header::RANGE,
            format!("bytes={}-{}", term.url_range.start, term.url_range.end - 1),
        )
        .send()
        .await
        .map_err(|e| CasClientError::InternalError(anyhow!("request failed with code {e}")))?;

    let xorb_bytes = response.bytes().await?;
    if xorb_bytes.len() as u32 != term.url_range.end - term.url_range.start {
        error!("got back a smaller range than requested");
        return Err(CasClientError::InvalidRange);
    }
    let mut readseek = Cursor::new(xorb_bytes.to_vec());
    let (data, chunk_byte_indices) = cas_object::deserialize_chunks(&mut readseek)?;

    // now write it back to cache
    if let Some(cache) = disk_cache {
        let key = Key {
            prefix: PREFIX_DEFAULT.to_string(),
            hash: term.hash.into(),
        };
        cache
            .put(&key, &term.range, &chunk_byte_indices, &data)
            .map_err(|e| CasClientError::InternalError(anyhow!("cache error {e}")))?;
    }

    Ok(Bytes::from(data))
}

#[cfg(test)]
mod tests {

    use super::*;
    use cas_object::test_utils::*;
    use tracing_test::traced_test;

    #[ignore = "requires a running CAS server"]
    #[traced_test]
    #[tokio::test]
    async fn test_basic_put() {
        // Arrange
        let prefix = PREFIX_DEFAULT;
        let (c, _, data, chunk_boundaries) = build_cas_object(
            3,
            ChunkSize::Random(512, 10248),
            cas_object::CompressionScheme::LZ4,
        );

        let client = RemoteClient::new(CAS_ENDPOINT, &None, &None);
        // Act
        let result = client
            .put(prefix, &c.info.cashash, data, chunk_boundaries)
            .await;

        // Assert
        assert!(result.is_ok());
    }
}
