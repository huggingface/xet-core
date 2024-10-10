use crate::interface::*;
use crate::Client;
use crate::{error::Result, CasClientError};
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Buf;
use bytes::Bytes;
use cas_object::CasObject;
use cas_types::{CASReconstructionTerm, Key, QueryReconstructionResponse, UploadXorbResponse};
use merklehash::MerkleHash;
use reqwest::{StatusCode, Url};
use reqwest_middleware::ClientWithMiddleware;
use std::io::{Cursor, Write};
use tracing::{debug, error, warn};

pub const CAS_ENDPOINT: &str = "http://localhost:8080";
pub const PREFIX_DEFAULT: &str = "default";

const NUM_RETRIES: usize = 5;
const BASE_RETRY_DELAY_MS: u64 = 3000;

fn retry_http_status_code(stat: &reqwest::StatusCode) -> bool {
    stat.is_server_error() || *stat == reqwest::StatusCode::TOO_MANY_REQUESTS
}

fn is_status_retryable_and_print(err: &reqwest::Error) -> bool {
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

fn is_middleware_status_retryable_and_print(err: &reqwest_middleware::Error) -> bool {
    match err {
        reqwest_middleware::Error::Reqwest(error) => is_status_retryable_and_print(error),
        _ => false,
    }
}

#[derive(Debug)]
pub struct RemoteClient {}

#[async_trait]
impl UploadClient for RemoteClient {
    async fn put(
        endpoint: &str,
        http_auth_client: &ClientWithMiddleware,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_and_boundaries: Vec<(MerkleHash, u32)>,
    ) -> Result<()> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: *hash,
        };

        let was_uploaded = RemoteClient::upload(
            endpoint,
            http_auth_client,
            &key,
            &data,
            chunk_and_boundaries,
        )
        .await?;

        if !was_uploaded {
            debug!("{key:?} not inserted into CAS.");
        } else {
            debug!("{key:?} inserted into CAS.");
        }

        Ok(())
    }

    async fn exists(
        endpoint: &str,
        http_auth_client: &ClientWithMiddleware,
        prefix: &str,
        hash: &MerkleHash,
    ) -> Result<bool> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: *hash,
        };

        let url = Url::parse(&format!("{}/xorb/{key}", endpoint))?;
        let response = http_auth_client.head(url).send().await?;
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
        endpoint: &str,
        http_client_auth: &ClientWithMiddleware,
        http_client: &ClientWithMiddleware,
        hash: &MerkleHash,
        writer: &mut Box<dyn Write + Send>,
    ) -> Result<()> {
        // get manifest of xorbs to download
        let manifest = RemoteClient::reconstruct(endpoint, http_client_auth, hash, None).await?;

        RemoteClient::get_ranges(http_client, manifest, None, writer).await?;

        Ok(())
    }

    #[allow(unused_variables)]
    async fn get_file_byte_range(
        endpoint: &str,
        http_client_auth: &ClientWithMiddleware,
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
        endpoint: &str,
        http_client: &ClientWithMiddleware,
        file_id: &MerkleHash,
        _bytes_range: Option<(u64, u64)>,
    ) -> Result<QueryReconstructionResponse> {
        let url = Url::parse(&format!("{}/reconstruction/{}", endpoint, file_id.hex()))?;

        let response = http_client.get(url).send().await?;

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
        endpoint: &str,
        http_client_auth: &ClientWithMiddleware,
        key: &Key,
        contents: &[u8],
        chunk_and_boundaries: Vec<(MerkleHash, u32)>,
    ) -> Result<bool> {
        let url = Url::parse(&format!("{}/xorb/{key}", endpoint))?;

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

        let response = http_client_auth.post(url).body(data).send().await?;
        let response_body = response.bytes().await?;
        let response_parsed: UploadXorbResponse = serde_json::from_reader(response_body.reader())?;

        Ok(response_parsed.was_inserted)
    }

    async fn get_ranges(
        http_client: &ClientWithMiddleware,
        reconstruction_response: QueryReconstructionResponse,
        _byte_range: Option<(u64, u64)>,
        writer: &mut Box<dyn Write + Send>,
    ) -> Result<usize> {
        let info = reconstruction_response.reconstruction;
        let total_len = info.iter().fold(0, |acc, x| acc + x.unpacked_length);
        let futs = info.into_iter().map(|term| {
            let http_client_clone = http_client.clone();
            tokio::spawn(async move { get_one_range(&http_client_clone, &term).await })
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
    term: &CASReconstructionTerm,
) -> Result<Bytes> {
    debug!("term: {term:?}");

    if term.range.end < term.range.start || term.url_range.end < term.url_range.start {
        return Err(CasClientError::InvalidRange);
    }

    let url = Url::parse(term.url.as_str())?;

    let response = http_client
        .get(url)
        .header(
            reqwest::header::RANGE,
            format!("bytes={}-{}", term.url_range.start, term.url_range.end - 1),
        )
        .send()
        .await?;

    let xorb_bytes = response.bytes().await?;
    if xorb_bytes.len() as u32 != term.url_range.end - term.url_range.start {
        error!("got back a smaller range than requested");
        return Err(CasClientError::InvalidRange);
    }
    let mut readseek = Cursor::new(xorb_bytes.to_vec());
    let data = cas_object::deserialize_chunks(&mut readseek)?;

    Ok(Bytes::from(data))
}

#[cfg(test)]
mod tests {
    use crate::http_client;

    use super::*;
    use cas_object::test_utils::*;
    use tracing_test::traced_test;
    use utils::auth::AuthConfig;

    #[ignore = "requires a running CAS server"]
    #[traced_test]
    #[tokio::test]
    async fn test_basic_put() {
        // Arrange
        let auth_config = AuthConfig::maybe_new(None, None, None);
        let http_client_auth = http_client::build_auth_http_client(&auth_config).unwrap();
        let prefix = PREFIX_DEFAULT;
        let (c, _, data, chunk_boundaries) = build_cas_object(
            3,
            ChunkSize::Random(512, 10248),
            cas_object::CompressionScheme::LZ4,
        );

        // Act
        let result = RemoteClient::put(
            CAS_ENDPOINT,
            &http_client_auth,
            prefix,
            &c.info.cashash,
            data,
            chunk_boundaries,
        )
        .await;

        // Assert
        assert!(result.is_ok());
    }
}
