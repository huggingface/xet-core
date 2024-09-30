use std::io::{Cursor, Write};

use anyhow::anyhow;
use bytes::Buf;
use bytes::Bytes;
use reqwest::{StatusCode, Url};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Middleware};
use serde::{de::DeserializeOwned, Serialize};
use tracing::{debug, warn};

use cas::auth::AuthConfig;
use cas::key::Key;
use cas_object::CasObject;
use cas_types::CASReconstructionTerm;
use cas_types::{QueryChunkResponse, QueryReconstructionResponse, UploadXorbResponse};
use error_printer::OptionPrinter;
use merklehash::MerkleHash;

use crate::Client;
use crate::{error::Result, AuthMiddleware, CasClientError};

pub const CAS_ENDPOINT: &str = "http://localhost:8080";
pub const PREFIX_DEFAULT: &str = "default";

#[derive(Debug)]
pub struct RemoteClient {
    client: CASAPIClient,
}

// TODO: add retries
#[async_trait::async_trait]
impl Client for RemoteClient {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<()> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: *hash,
        };

        let was_uploaded = self.client.upload(&key, &data, chunk_boundaries).await?;

        if !was_uploaded {
            debug!("{key:?} not inserted into CAS.");
        } else {
            debug!("{key:?} inserted into CAS.");
        }

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    async fn get(&self, _prefix: &str, _hash: &merklehash::MerkleHash) -> Result<Vec<u8>> {
        Err(CasClientError::InvalidArguments)
    }

    async fn get_object_range(
        &self,
        _prefix: &str,
        _hash: &merklehash::MerkleHash,
        _ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>> {
        Err(CasClientError::InvalidArguments)
    }

    async fn get_length(&self, prefix: &str, hash: &merklehash::MerkleHash) -> Result<u64> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: *hash,
        };
        match self.client.get_length(&key).await? {
            Some(length) => Ok(length),
            None => Err(CasClientError::XORBNotFound(*hash)),
        }
    }
}

impl RemoteClient {
    pub async fn from_config(endpoint: String, auth_config: &Option<AuthConfig>) -> Self {
        Self {
            client: CASAPIClient::new(&endpoint, auth_config),
        }
    }
}

#[derive(Debug)]
pub struct CASAPIClient {
    client: ClientWithMiddleware,
    endpoint: String,
}

impl Default for CASAPIClient {
    fn default() -> Self {
        Self::new(CAS_ENDPOINT, &None)
    }
}

impl CASAPIClient {
    pub fn new(endpoint: &str, auth_config: &Option<AuthConfig>) -> Self {
        let client = build_reqwest_client(auth_config).unwrap();
        Self {
            client,
            endpoint: endpoint.to_string(),
        }
    }

    pub async fn exists(&self, key: &Key) -> Result<bool> {
        let url = Url::parse(&format!("{}/xorb/{key}", self.endpoint))?;
        let response = self.client.head(url).send().await?;
        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            e => Err(CasClientError::InternalError(anyhow!(
                "unrecognized status code {e}"
            ))),
        }
    }

    pub async fn get_length(&self, key: &Key) -> Result<Option<u64>> {
        let url = Url::parse(&format!("{}/xorb/{key}", self.endpoint))?;
        let response = self.client.head(url).send().await?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if status != StatusCode::OK {
            return Err(CasClientError::InternalError(anyhow!(
                "unrecognized status code {status}"
            )));
        }
        let hv = match response.headers().get("Content-Length") {
            Some(hv) => hv,
            None => {
                return Err(CasClientError::InternalError(anyhow!(
                    "HEAD missing content length header"
                )))
            }
        };
        let length: u64 = hv
            .to_str()
            .map_err(|_| {
                CasClientError::InternalError(anyhow!("HEAD missing content length header"))
            })?
            .parse()
            .map_err(|_| CasClientError::InternalError(anyhow!("failed to parse length")))?;

        Ok(Some(length))
    }

    pub async fn upload(
        &self,
        key: &Key,
        contents: &[u8],
        chunk_boundaries: Vec<u64>,
    ) -> Result<bool> {
        let url = Url::parse(&format!("{}/xorb/{key}", self.endpoint))?;

        let mut writer = Cursor::new(Vec::new());

        let (_, _) = CasObject::serialize(
            &mut writer,
            &key.hash,
            contents,
            &chunk_boundaries.into_iter().map(|x| x as u32).collect(),
            cas_object::CompressionScheme::LZ4,
        )?;

        debug!("Upload: POST to {url:?} for {key:?}");
        writer.set_position(0);
        let data = writer.into_inner();

        let response = self.client.post(url).body(data).send().await?;
        let response_body = response.bytes().await?;
        let response_parsed: UploadXorbResponse = serde_json::from_reader(response_body.reader())?;

        Ok(response_parsed.was_inserted)
    }

    /// Reconstruct a file and write to writer.
    pub async fn write_file<W: Write>(
        &self,
        file_id: &MerkleHash,
        writer: &mut W,
    ) -> Result<usize> {
        // get manifest of xorbs to download
        let manifest = self.reconstruct_file(file_id).await?;

        self.reconstruct(manifest, writer).await
    }

    async fn reconstruct<W: Write>(
        &self,
        reconstruction_response: QueryReconstructionResponse,
        writer: &mut W,
    ) -> Result<usize> {
        let info = reconstruction_response.reconstruction;
        let total_len = info.iter().fold(0, |acc, x| acc + x.unpacked_length);
        let futs = info.into_iter().map(|term| {
            tokio::spawn(async move {
                let piece = get_one(&term).await?;
                if piece.len() != (term.range.end - term.range.start) as usize {
                    warn!("got back a smaller range than requested");
                }
                Result::<Bytes>::Ok(piece)
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

    /// Reconstruct the file
    async fn reconstruct_file(&self, file_id: &MerkleHash) -> Result<QueryReconstructionResponse> {
        let url = Url::parse(&format!(
            "{}/reconstruction/{}",
            self.endpoint,
            file_id.hex()
        ))?;

        let response = self.client.get(url).send().await?;
        let response_body = response.bytes().await?;
        let response_parsed: QueryReconstructionResponse =
            serde_json::from_reader(response_body.reader())?;

        Ok(response_parsed)
    }

    pub async fn shard_query_chunk(&self, key: &Key) -> Result<QueryChunkResponse> {
        let url = Url::parse(&format!("{}/chunk/{key}", self.endpoint))?;
        let response = self.client.get(url).send().await?;
        let response_body = response.bytes().await?;
        let response_parsed: QueryChunkResponse = serde_json::from_reader(response_body.reader())?;

        Ok(response_parsed)
    }

    async fn post_json<ReqT, RespT>(&self, url: Url, request_body: &ReqT) -> Result<RespT>
    where
        ReqT: Serialize,
        RespT: DeserializeOwned,
    {
        let body = serde_json::to_vec(request_body)?;
        let response = self.client.post(url).body(body).send().await?;
        let response_bytes = response.bytes().await?;
        serde_json::from_reader(response_bytes.reader()).map_err(CasClientError::SerdeError)
    }
}

async fn get_one(term: &CASReconstructionTerm) -> Result<Bytes> {
    debug!("term: {term:?}");

    if term.range.end < term.range.start || term.url_range.end < term.url_range.start {
        return Err(CasClientError::InternalError(anyhow!(
            "invalid range in reconstruction"
        )));
    }

    let url = Url::parse(term.url.as_str())?;
    let response = reqwest::Client::new()
        .request(hyper::Method::GET, url)
        .header(
            reqwest::header::RANGE,
            format!("bytes={}-{}", term.url_range.start, term.url_range.end),
        )
        .send()
        .await?
        .error_for_status()?;
    let xorb_bytes = response
        .bytes()
        .await
        .map_err(CasClientError::ReqwestError)?;
    let mut readseek = Cursor::new(xorb_bytes.to_vec());
    let data = cas_object::deserialize_chunks(&mut readseek)?;
    let len = (term.range.end - term.range.start) as usize;
    let offset = term.range_start_offset as usize;

    let sliced = data[offset..offset + len].to_vec();

    Ok(Bytes::from(sliced))
}

/// builds the client to talk to CAS.
pub fn build_reqwest_client(
    auth_config: &Option<AuthConfig>,
) -> std::result::Result<ClientWithMiddleware, reqwest::Error> {
    let auth_middleware = auth_config
        .as_ref()
        .map(AuthMiddleware::from)
        .info_none("CAS auth disabled");
    let reqwest_client = reqwest::Client::builder().build()?;
    Ok(ClientBuilder::new(reqwest_client)
        .maybe_with(auth_middleware)
        .build())
}

/// Helper trait to allow the reqwest_middleware client to optionally add a middleware.
trait OptionalMiddleware {
    fn maybe_with<M: Middleware>(self, middleware: Option<M>) -> Self;
}

impl OptionalMiddleware for ClientBuilder {
    fn maybe_with<M: Middleware>(self, middleware: Option<M>) -> Self {
        match middleware {
            Some(m) => self.with(m),
            None => self,
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use tracing_test::traced_test;

    use super::*;
    use merkledb::{prelude::MerkleDBHighLevelMethodsV1, Chunk, MerkleMemDB};
    use merklehash::DataHash;

    #[ignore]
    #[traced_test]
    #[tokio::test]
    async fn test_basic_put() {
        // Arrange
        let rc = RemoteClient::from_config(CAS_ENDPOINT.to_string(), &None).await;
        let prefix = PREFIX_DEFAULT;
        let (hash, data, chunk_boundaries) = gen_dummy_xorb(3, 10248, true);

        // Act
        let result = rc.put(prefix, &hash, data, chunk_boundaries).await;

        // Assert
        assert!(result.is_ok());
    }

    fn gen_dummy_xorb(
        num_chunks: u32,
        uncompressed_chunk_size: u32,
        randomize_chunk_sizes: bool,
    ) -> (DataHash, Vec<u8>, Vec<u64>) {
        let mut contents = Vec::new();
        let mut chunks: Vec<Chunk> = Vec::new();
        let mut chunk_boundaries = Vec::with_capacity(num_chunks as usize);
        for _idx in 0..num_chunks {
            let chunk_size: u32 = if randomize_chunk_sizes {
                let mut rng = rand::thread_rng();
                rng.gen_range(1024..=uncompressed_chunk_size)
            } else {
                uncompressed_chunk_size
            };

            let bytes = gen_random_bytes(chunk_size);

            chunks.push(Chunk {
                hash: merklehash::compute_data_hash(&bytes),
                length: bytes.len(),
            });

            contents.extend(bytes);
            chunk_boundaries.push(contents.len() as u64);
        }

        let mut db = MerkleMemDB::default();
        let mut staging = db.start_insertion_staging();
        db.add_file(&mut staging, &chunks);
        let ret = db.finalize(staging);
        let hash = *ret.hash();

        (hash, contents, chunk_boundaries)
    }

    fn gen_random_bytes(uncompressed_chunk_size: u32) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut data = vec![0u8; uncompressed_chunk_size as usize];
        rng.fill(&mut data[..]);
        data
    }
}
