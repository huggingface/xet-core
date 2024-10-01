use crate::interface::*;
use crate::Client;
use crate::{error::Result, AuthMiddleware, CasClientError};
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Buf;
use bytes::Bytes;
use cas::auth::AuthConfig;
use cas_object::CasObject;
use cas_types::{CASReconstructionTerm, Key, QueryReconstructionResponse, UploadXorbResponse};
use error_printer::OptionPrinter;
use merklehash::MerkleHash;
use reqwest::{StatusCode, Url};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Middleware};
use std::io::{Cursor, Write};
use tracing::{debug, warn};

pub const CAS_ENDPOINT: &str = "http://localhost:8080";
pub const PREFIX_DEFAULT: &str = "default";

#[derive(Debug)]
pub struct RemoteClient {
    client: ClientWithMiddleware,
    endpoint: String,
}

// TODO: add retries
#[async_trait]
impl UploadClient for RemoteClient {
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

        let was_uploaded = self.upload(&key, &data, chunk_boundaries).await?;

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
        let response = self.client.head(url).send().await?;
        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            e => Err(CasClientError::InternalError(anyhow!(
                "unrecognized status code {e}"
            ))),
        }
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ReconstructionClient for RemoteClient {
    async fn get_file(&self, hash: &MerkleHash, writer: &mut Box<dyn Write + Send>) -> Result<()> {
        // get manifest of xorbs to download
        let manifest = self.reconstruct_file(hash, None).await?;

        self.reconstruct(manifest, None, writer).await?;

        Ok(())
    }

    #[allow(unused_variables)]
    async fn get_file_byte_range(
        &self,
        hash: &MerkleHash,
        offset: u64,
        length: u64,
        writer: &mut Box<dyn Write + Send>,
    ) -> Result<()> {
        todo!()
    }
}

impl Client for RemoteClient {}

impl RemoteClient {
    pub fn new(endpoint: &str, auth_config: &Option<AuthConfig>) -> Self {
        let client = build_reqwest_client(auth_config).unwrap();
        Self {
            client,
            endpoint: endpoint.to_string(),
        }
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

    async fn reconstruct(
        &self,
        reconstruction_response: QueryReconstructionResponse,
        _byte_range: Option<(u64, u64)>,
        writer: &mut Box<dyn Write + Send>,
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
    async fn reconstruct_file(
        &self,
        file_id: &MerkleHash,
        _bytes_range: Option<(u64, u64)>,
    ) -> Result<QueryReconstructionResponse> {
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
        let rc = RemoteClient::new(CAS_ENDPOINT, &None);
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
