use anyhow::anyhow;
use bytes::Buf;
use cas::key::Key;
use cas_types::compression_scheme::CompressionScheme;
use cas_types::{
    QueryChunkParams, QueryChunkResponse, QueryFileParams,
    SyncShardResponse, SyncShardResponseType, UploadXorbResponse
};
use reqwest::{StatusCode, Url};
use serde::{de::DeserializeOwned, Serialize};

use merklehash::MerkleHash;

use crate::error::{CasClientError, Result};
use crate::Client;

pub const CAS_ENDPOINT: &str = "localhost:4884";
pub const SCHEME: &str = "localhost:4884";

#[derive(Debug)]
pub struct RemoteClient {
    endpoint: String,
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
        self.client
            .upload(&key, data, chunk_boundaries)
            .await
            .map(|_| ())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    async fn get(&self, prefix: &str, hash: &merklehash::MerkleHash) -> Result<Vec<u8>> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: *hash,
        };
        let result = self.client.get(&key).await?;
        Ok(result)
    }

    async fn get_object_range(
        &self,
        prefix: &str,
        hash: &merklehash::MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: *hash,
        };
        let xorb = self.client.get(&key).await?;
        let mut res = Vec::with_capacity(ranges.len());
        for (start, end) in ranges {
            let start = start as usize;
            let end = end as usize;
            if start > xorb.len() || end > xorb.len() {
                return Err(CasClientError::InvalidRange);
            }
            let section = &xorb[start..end];
            res.push(Vec::from(section))
        }

        Ok(res)
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

#[derive(Debug)]
pub struct CASAPIClient {
    client: reqwest::Client,
}

impl Default for CASAPIClient {
    fn default() -> Self {
        Self::new()
    }
}

impl CASAPIClient {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .build()
            .unwrap();
        Self { client }
    }

    pub async fn get(&self, key: &Key) -> Result<Vec<u8>> {
        let url = format!("{SCHEME}/{CAS_ENDPOINT}/{key}").parse()?;
        let request = reqwest::Request::new(reqwest::Method::GET, url);
        let response = self.client.execute(request).await?;
        let xorb_data = response.bytes().await?;
        Ok(xorb_data.to_vec())
    }

    pub async fn exists(&self, key: &Key) -> Result<bool> {
        let url: Url = format!("{SCHEME}/{CAS_ENDPOINT}/{key}").parse()?;
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
        let url: Url = format!("{SCHEME}/{CAS_ENDPOINT}/{key}").parse()?;
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

    pub async fn upload<T: Into<reqwest::Body>>(
        &self,
        key: &Key,
        contents: T,
        chunk_boundaries: Vec<u64>,
    ) -> Result<bool> {
        let chunk_boundaries_query = chunk_boundaries
            .iter()
            .map(|num| num.to_string())
            .collect::<Vec<String>>()
            .join(",");
        let url: Url = format!("{SCHEME}/{CAS_ENDPOINT}/{key}?{chunk_boundaries_query}").parse()?;

        let response = self.client.post(url).body(contents.into()).send().await?;
        let response_body = response.bytes().await?;
        let response_parsed: UploadXorbResponse = serde_json::from_reader(response_body.reader())?;

        Ok(response_parsed.was_inserted)
    }

    pub async fn shard_sync(&self, key: &Key, force_sync: bool, salt: &[u8; 32]) -> Result<bool> {
        let url: Url = format!("{SCHEME}/{CAS_ENDPOINT}/shard/sync").parse()?;
        let params = SyncShardParams {
            key: key.clone(),
            force_sync,
            salt: Vec::from(salt),
        };

        let response_value: SyncShardResponse = self.post_json(url, &params).await?;
        match response_value.response {
            SyncShardResponseType::Exists => Ok(false),
            SyncShardResponseType::SyncPerformed => Ok(true),
        }
    }

    pub async fn shard_query_file(&self, file_id: &MerkleHash) -> Result<QueryFileResponse> {
        let url: Url = format!("{SCHEME}/{CAS_ENDPOINT}/shard/query_file").parse()?;
        let params = QueryFileParams { file_id: *file_id };
        let response_value: QueryFileResponse = self.post_json(url, &params).await?;
        Ok(response_value)
    }

    pub async fn shard_query_chunk(
        &self,
        prefix: &str,
        chunk: Vec<MerkleHash>,
    ) -> Result<QueryChunkResponse> {
        let url: Url = format!("{SCHEME}/{CAS_ENDPOINT}/shard/query_chunk").parse()?;
        let params = QueryChunkParams {
            prefix: prefix.to_string(),
            chunk,
        };
        let response_value: QueryChunkResponse = self.post_json(url, &params).await?;
        Ok(response_value)
    }

    async fn post_json<ReqT, RespT>(&self, url: Url, request_body: &ReqT) -> Result<RespT>
    where
        ReqT: Serialize,
        RespT: DeserializeOwned,
    {
        let body = serde_json::to_vec(request_body)?;
        let response = self.client.post(url).body(body).send().await?;
        let response_bytes = response.bytes().await?;
        serde_json::from_reader(response_bytes.reader()).map_err(CasClientError::SerdeJSONError)
    }
}