use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use cas_object::CasObject;
use cas_types::{
    CASReconstructionFetchInfo, CASReconstructionTerm, HexMerkleHash, Key, QueryReconstructionResponse, Range,
    UploadXorbResponse,
};
use chunk_cache::{CacheConfig, ChunkCache, DiskCache};
use error_printer::ErrorPrinter;
use merklehash::MerkleHash;
use reqwest::{StatusCode, Url};
use reqwest_middleware::ClientWithMiddleware;
use tracing::{debug, error, info};
use utils::auth::AuthConfig;
use utils::singleflight;

use crate::error::Result;
use crate::interface::*;
use crate::{http_client, CasClientError, Client};

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
    pub fn new(endpoint: &str, auth: &Option<AuthConfig>, cache_config: &Option<CacheConfig>) -> Self {
        // use disk cache if cache_config provided.
        let disk_cache = if let Some(cache_config) = cache_config {
            info!("Using disk cache directory: {:?}, size: {}.", cache_config.cache_directory, cache_config.cache_size);
            DiskCache::initialize(cache_config)
                .log_error("failed to initialize cache, not using cache")
                .ok()
                .map(|disk_cache| Arc::new(disk_cache) as Arc<dyn ChunkCache>)
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
            e => Err(CasClientError::InternalError(anyhow!("unrecognized status code {e}"))),
        }
    }
}

#[async_trait]
impl ReconstructionClient for RemoteClient {
    async fn get_file(
        &self,
        http_client: Arc<ClientWithMiddleware>,
        hash: &MerkleHash,
        writer: &mut Box<dyn Write + Send>,
    ) -> Result<()> {
        // get manifest of xorbs to download, api call to CAS
        let manifest = self.get_reconstruction(hash, None).await?;

        self.reconstruct_file_to_writer(http_client, manifest, None, writer).await?;

        Ok(())
    }

    #[allow(unused_variables)]
    async fn get_file_byte_range(
        &self,
        http_client: Arc<ClientWithMiddleware>,
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
    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        _bytes_range: Option<(u64, u64)>,
    ) -> Result<QueryReconstructionResponse> {
        let url = Url::parse(&format!("{}/reconstruction/{}", self.endpoint, file_id.hex()))?;

        let response = self
            .http_auth_client
            .get(url)
            .send()
            .await
            .log_error("error invoking reconstruction api")?
            .error_for_status()
            .log_error("reconstruction api returned error code")?;

        let len = response.content_length();
        debug!("fileid: {file_id} query_reconstruction len {len:?}");

        let query_reconstruction_response: QueryReconstructionResponse = response
            .json()
            .await
            .log_error("error json parsing QueryReconstructionResponse")?;
        Ok(query_reconstruction_response)
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
        let response_parsed: UploadXorbResponse = response.json().await?;

        Ok(response_parsed.was_inserted)
    }

    /// use the reconstruction response from CAS to re-create the described file for any calls
    /// to download files from S3/blob store using urls from the fetch information section of
    /// of the response it will use the provided http client.
    ///
    /// TODO: respect the byte_range argument to reconstruct only a section.
    ///
    /// To reconstruct, this function will iterate through each CASReconstructionTerm in the `terms` section
    /// of the QueryReconstructionResponse, fetching the data for that term. Each term is a range of chunks
    /// from a specific Xorb. The range is an end exclusive [start, end) chunk index range of the xorb
    /// specified by the hash key of the CASReconstructionTerm.
    ///
    /// To fetch the data for each term, this function will consult the fetch_info section of the reconstruction
    /// response. See `get_one_term`.
    async fn reconstruct_file_to_writer(
        &self,
        http_client: Arc<ClientWithMiddleware>,
        reconstruction_response: QueryReconstructionResponse,
        _byte_range: Option<(u64, u64)>,
        writer: &mut Box<dyn Write + Send>,
    ) -> Result<usize> {
        let terms = reconstruction_response.terms;
        let fetch_info = Arc::new(reconstruction_response.fetch_info);

        let single_flight_group = Arc::new(singleflight::Group::new());

        let total_len = terms.iter().fold(0, |acc, x| acc + x.unpacked_length);
        let term_data_futures = terms.into_iter().map(|term| {
            let http_client = http_client.clone();
            let disk_cache = self.disk_cache.clone();
            let fetch_info = fetch_info.clone();
            let single_flight_group = single_flight_group.clone();
            tokio::spawn(get_one_term(http_client, disk_cache, term, fetch_info, single_flight_group))
        });
        for fut in term_data_futures {
            let term_data = fut
                .await
                .map_err(|e| CasClientError::InternalError(anyhow!("join error {e}")))?
                .log_error("error getting one term")?;
            writer.write_all(&term_data)?;
        }
        // TODO: respect the offset_from_first_byte field of response
        //  only necessary for range-reconstruction, not needed for full file
        Ok(total_len as usize)
    }
}

// Right now if all ranges are fetched "at once" (all tasks spawned in brief succession)
// they may get a cache miss, and issue the S3 get, we use a singleflight group
// to avoid double gets for these requests, with the singleflight key being the S3 url.
pub(crate) type ChunkDataSingleFlightGroup = singleflight::Group<(Vec<u8>, Vec<u32>), CasClientError>;

/// fetch the data requested for the term argument (data from a range of chunks
/// in a xorb).
/// if provided, it will first check a ChunkCache for the existence of this range.
///
/// To fetch the data from S3/blob store, get_one_term will match the term's hash/xorb to a vec of
/// CASReconstructionFetchInfo in the fetch_info information. Then pick from this vec (by a linear scan)
/// a fetch_info term that contains the requested term's range. Then:
///     download the url -> deserialize chunks -> trim the output to the requested term's chunk range
///
/// If the fetch_info section (provided as in the QueryReconstrutionResponse) fails to contain a term
/// that matches our requested CASReconstructionTerm, it is considered a bad output from the CAS API.
pub(crate) async fn get_one_term(
    http_client: Arc<ClientWithMiddleware>,
    disk_cache: Option<Arc<dyn ChunkCache>>,
    term: CASReconstructionTerm,
    fetch_info: Arc<HashMap<HexMerkleHash, Vec<CASReconstructionFetchInfo>>>,
    single_flight_group: Arc<ChunkDataSingleFlightGroup>,
) -> Result<Vec<u8>> {
    debug!("term: {term:?}");

    if term.range.end < term.range.start {
        return Err(CasClientError::InvalidRange);
    }

    // check disk cache for the exact range we want for the reconstruction term
    if let Some(cache) = &disk_cache {
        let key = Key {
            prefix: PREFIX_DEFAULT.to_string(),
            hash: term.hash.into(),
        };
        if let Some(cached) = cache.get(&key, &term.range)? {
            return Ok(cached);
        }
    }

    // get the fetch info term for the key
    // then find the term within the ranges that will match our requested range
    // if either operation fails, this is a result of a bad response from the reconstruction api.
    let hash_fetch_info = fetch_info
        .get(&term.hash)
        .ok_or(CasClientError::InvalidArguments)
        .log_error("invalid response from CAS server: failed to get term hash in fetchables")?;
    let fetch_term = hash_fetch_info
        .iter()
        .find(|fterm| fterm.range.start <= term.range.start && fterm.range.end >= term.range.end)
        .ok_or(CasClientError::InvalidArguments)
        .log_error("invalid response from CAS server: failed to match hash in fetch_info")?
        .clone();

    // fetch the range from blob store and deserialize the chunks
    // then put into the cache if used
    let single_flight_group_key = fetch_term.url.as_str();
    let fetch_term_owned = fetch_term.clone();
    let (mut data, chunk_byte_indices) = single_flight_group
        .work_dump_caller_info(single_flight_group_key, async move {
            let (data, chunk_byte_indices) = download_range(http_client, &fetch_term_owned).await?;
            // now write it to cache, the whole fetched term
            if let Some(cache) = disk_cache {
                let key = Key {
                    prefix: PREFIX_DEFAULT.to_string(),
                    hash: term.hash.into(),
                };
                cache.put(&key, &fetch_term_owned.range, &chunk_byte_indices, &data)?;
            }
            Ok((data, chunk_byte_indices))
        })
        .await?;

    // if the requested range is smaller than the fetched range, trim it down to the right data
    // the requested range cannot be larger than the fetched range.
    // "else" case data matches exact, save some work, return whole data.
    if term.range != fetch_term.range {
        let start_idx = term.range.start - fetch_term.range.start;
        let end_idx = term.range.end - fetch_term.range.start;
        let start_byte_index = chunk_byte_indices[start_idx as usize] as usize;
        let end_byte_index = chunk_byte_indices[end_idx as usize] as usize;
        debug_assert!(start_byte_index < data.len());
        debug_assert!(end_byte_index <= data.len());
        debug_assert!(start_byte_index < end_byte_index);
        // [0, len] -> [0, end_byte_index)
        data.truncate(end_byte_index);
        // [0, end_byte_index) -> [start_byte_index, end_byte_index)
        data = data.split_off(start_byte_index);
    }

    if data.len() != term.unpacked_length as usize {
        return Err(CasClientError::Other(format!(
            "result term data length {} did not match expected value {}",
            data.len(),
            term.unpacked_length
        )));
    }

    Ok(data)
}

fn range_header(range: &Range) -> String {
    format!("bytes={}-{}", range.start, range.end)
}

/// use the provided http_client to make requests to S3/blob store using the url and url_range
/// parts of a CASReconstructionFetchInfo. The url_range part is used directly in an http Range header
/// value (see fn `range_header`).
async fn download_range(
    http_client: Arc<ClientWithMiddleware>,
    fetch_term: &CASReconstructionFetchInfo,
) -> Result<(Vec<u8>, Vec<u32>)> {
    let url = Url::parse(fetch_term.url.as_str())?;
    let response = http_client
        .get(url)
        .header(reqwest::header::RANGE, range_header(&fetch_term.url_range))
        .send()
        .await
        .log_error("error getting from s3")?
        .error_for_status()
        .log_error("get from s3 error code")?;
    let xorb_bytes = response.bytes().await?;

    // + 1 since range S3/HTTP range is inclusive on both ends
    // remove this check to be agnostic to range-end-exclusive blob store requests
    let expected_len = fetch_term.url_range.end - fetch_term.url_range.start + 1;
    if xorb_bytes.len() as u32 != expected_len {
        error!("got back a smaller byte range ({}) than requested ({expected_len}) from s3", xorb_bytes.len());
        return Err(CasClientError::InvalidRange);
    }
    let mut readseek = Cursor::new(xorb_bytes);
    let (data, chunk_byte_indices) = cas_object::deserialize_chunks(&mut readseek)?;
    Ok((data, chunk_byte_indices))
}

#[cfg(test)]
mod tests {

    use cas_object::test_utils::*;
    use tracing_test::traced_test;

    use super::*;

    #[ignore = "requires a running CAS server"]
    #[traced_test]
    #[tokio::test]
    async fn test_basic_put() {
        // Arrange
        let prefix = PREFIX_DEFAULT;
        let (c, _, data, chunk_boundaries) =
            build_cas_object(3, ChunkSize::Random(512, 10248), cas_object::CompressionScheme::LZ4);

        let client = RemoteClient::new(CAS_ENDPOINT, &None, &None);
        // Act
        let result = client.put(prefix, &c.info.cashash, data, chunk_boundaries).await;

        // Assert
        assert!(result.is_ok());
    }
}
