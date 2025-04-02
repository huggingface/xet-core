use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Write};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use cas_object::{CasObject, CompressionScheme};
use cas_types::{
    BatchQueryReconstructionResponse, CASReconstructionFetchInfo, CASReconstructionTerm, FileRange, HexMerkleHash,
    HttpRange, Key, QueryReconstructionResponse, UploadShardResponse, UploadShardResponseType, UploadXorbResponse,
};
use chunk_cache::{CacheConfig, ChunkCache};
use error_printer::ErrorPrinter;
use file_utils::SafeFileCreator;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use http::header::RANGE;
use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::utils::shard_file_name;
use merklehash::{HashedWrite, MerkleHash};
use reqwest::{StatusCode, Url};
use reqwest_middleware::ClientWithMiddleware;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, trace};
use utils::auth::AuthConfig;
use utils::progress::ProgressUpdater;
use utils::singleflight::Group;
use xet_threadpool::ThreadPool;

use crate::error::{CasClientError, Result};
use crate::http_client::{ResponseErrorLogger, RetryConfig};
use crate::interface::{ShardDedupProber, *};
use crate::{http_client, Client, RegistrationClient, ShardClientInterface};

const FORCE_SYNC_METHOD: reqwest::Method = reqwest::Method::PUT;
const NON_FORCE_SYNC_METHOD: reqwest::Method = reqwest::Method::POST;

pub const CAS_ENDPOINT: &str = "http://localhost:8080";
pub const PREFIX_DEFAULT: &str = "default";

utils::configurable_constants! {
   ref NUM_CONCURRENT_RANGE_GETS: usize = 16;

// Env (HF_XET_RECONSTRUCT_WRITE_SEQUENTIALLY) to switch to writing terms sequentially to disk.
// Benchmarks have shown that on SSD machines, writing in parallel seems to far outperform
// sequential term writes.
// However, this is not likely the case for writing to HDD and may in fact be worse,
// so for those machines, setting this env may help download perf.
    ref RECONSTRUCT_WRITE_SEQUENTIALLY: bool = false;
}

type RangeDownloadSingleFlight = Arc<Group<(Vec<u8>, Vec<u32>), CasClientError>>;

pub struct RemoteClient {
    endpoint: String,
    compression: Option<CompressionScheme>,
    dry_run: bool,
    http_client: Arc<ClientWithMiddleware>,
    authenticated_http_client: Arc<ClientWithMiddleware>,
    conservative_authenticated_http_client: Arc<ClientWithMiddleware>,
    chunk_cache: Option<Arc<dyn ChunkCache>>,
    threadpool: Arc<ThreadPool>,
    range_download_single_flight: RangeDownloadSingleFlight,
    shard_cache_directory: PathBuf,
}

impl RemoteClient {
    pub fn new(
        threadpool: Arc<ThreadPool>,
        endpoint: &str,
        compression: Option<CompressionScheme>,
        auth: &Option<AuthConfig>,
        cache_config: &Option<CacheConfig>,
        shard_cache_directory: PathBuf,
        dry_run: bool,
    ) -> Self {
        // use disk cache if cache_config provided.
        let chunk_cache = if let Some(cache_config) = cache_config {
            if cache_config.cache_size == 0 {
                info!("Chunk cache size set to 0, disabling chunk cache");
                None
            } else {
                debug!(
                    "Using disk cache directory: {:?}, size: {}.",
                    cache_config.cache_directory, cache_config.cache_size
                );
                chunk_cache::get_cache(cache_config)
                    .log_error("failed to initialize cache, not using cache")
                    .ok()
            }
        } else {
            None
        };
        let range_download_single_flight = Arc::new(Group::new());

        Self {
            endpoint: endpoint.to_string(),
            compression,
            dry_run,
            authenticated_http_client: Arc::new(
                http_client::build_auth_http_client(auth, RetryConfig::default()).unwrap(),
            ),
            conservative_authenticated_http_client: Arc::new(
                http_client::build_auth_http_client(auth, RetryConfig::no429retry()).unwrap(),
            ),
            http_client: Arc::new(http_client::build_http_client(RetryConfig::default()).unwrap()),
            chunk_cache,
            threadpool,
            range_download_single_flight,
            shard_cache_directory,
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
    ) -> Result<usize> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: *hash,
        };

        let (was_uploaded, nbytes_trans) = self.upload(&key, data, chunk_and_boundaries).await?;

        if !was_uploaded {
            debug!("{key:?} not inserted into CAS.");
        } else {
            debug!("{key:?} inserted into CAS.");
        }

        Ok(nbytes_trans)
    }

    async fn exists(&self, prefix: &str, hash: &MerkleHash) -> Result<bool> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: *hash,
        };

        let url = Url::parse(&format!("{}/xorb/{key}", self.endpoint))?;
        let response = self.authenticated_http_client.head(url).send().await?;
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
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        output_provider: &OutputProvider,
        progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Result<u64> {
        // get manifest of xorbs to download, api call to CAS
        let manifest = self.get_reconstruction(hash, byte_range.clone()).await?;
        let terms = manifest.terms;
        let fetch_info = Arc::new(manifest.fetch_info);

        // If the user has set the `HF_XET_RECONSTRUCT_WRITE_SEQUENTIALLY=true` env variable, then we
        // should write the file to the output sequentially instead of in parallel.
        if *RECONSTRUCT_WRITE_SEQUENTIALLY {
            self.reconstruct_file_to_writer(
                terms,
                fetch_info,
                manifest.offset_into_first_range,
                byte_range,
                output_provider,
                progress_updater,
            )
            .await
        } else {
            self.reconstruct_file_to_writer_parallel(
                terms,
                fetch_info,
                manifest.offset_into_first_range,
                byte_range,
                output_provider,
                progress_updater,
            )
            .await
        }
    }

    async fn batch_get_file(&self, files: HashMap<MerkleHash, &OutputProvider>) -> Result<u64> {
        let requested_file_ids = files.keys().cloned().collect::<HashSet<_>>();
        let manifest = self.batch_get_reconstruction(requested_file_ids.iter()).await?;
        let fetch_info = Arc::new(manifest.fetch_info);

        let received_file_ids: HashSet<MerkleHash> = manifest.files.keys().map(Into::into).collect::<HashSet<_>>();
        if requested_file_ids != received_file_ids {
            return Err(CasClientError::Other(
                "CAS returned a batch response not matching requested files".to_string(),
            ));
        }

        // TODO: spawn threads to reconstruct each file
        let mut ret_size = 0;
        for (hash, terms) in manifest.files {
            let w = files.get(&(hash.into())).unwrap();
            ret_size += if *RECONSTRUCT_WRITE_SEQUENTIALLY {
                self.reconstruct_file_to_writer(terms, fetch_info.clone(), 0, None, w, None)
                    .await?
            } else {
                self.reconstruct_file_to_writer_parallel(terms, fetch_info.clone(), 0, None, w, None)
                    .await?
            }
        }

        Ok(ret_size)
    }
}

#[async_trait]
impl Reconstructable for RemoteClient {
    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<QueryReconstructionResponse> {
        let url = Url::parse(&format!("{}/reconstruction/{}", self.endpoint, file_id.hex()))?;

        let mut request = self.authenticated_http_client.get(url);
        if let Some(range) = bytes_range {
            // convert exclusive-end to inclusive-end range
            request = request.header(RANGE, format!("{}-{}", range.start, range.end - 1))
        }
        let response = request.send().await.process_error("get_reconstruction")?;

        let len = response.content_length();
        debug!("file_id: {file_id} query_reconstruction len {len:?}");

        let query_reconstruction_response: QueryReconstructionResponse = response
            .json()
            .await
            .log_error("error json parsing QueryReconstructionResponse")?;
        Ok(query_reconstruction_response)
    }
}

impl Client for RemoteClient {}

impl RemoteClient {
    async fn batch_get_reconstruction(
        &self,
        file_ids: impl Iterator<Item = &MerkleHash>,
    ) -> Result<BatchQueryReconstructionResponse> {
        let mut url_str = format!("{}/reconstructions?", self.endpoint);
        let mut is_first = true;
        for hash in file_ids {
            if is_first {
                is_first = false;
            } else {
                url_str.push('&');
            }
            url_str.push_str("file_id=");
            url_str.push_str(hash.hex().as_str());
        }
        let url: Url = url_str.parse()?;

        let response = self
            .authenticated_http_client
            .get(url)
            .send()
            .await
            .process_error("batch_get_reconstruction")?;

        let query_reconstruction_response: BatchQueryReconstructionResponse = response
            .json()
            .await
            .log_error("error json parsing BatchQueryReconstructionResponse")?;
        Ok(query_reconstruction_response)
    }

    pub async fn upload(
        &self,
        key: &Key,
        contents: Vec<u8>,
        chunk_and_boundaries: Vec<(MerkleHash, u32)>,
    ) -> Result<(bool, usize)> {
        let url = Url::parse(&format!("{}/xorb/{key}", self.endpoint))?;

        let mut writer = Cursor::new(Vec::new());

        let (_, nbytes_trans) =
            CasObject::serialize(&mut writer, &key.hash, &contents, &chunk_and_boundaries, self.compression)?;
        // free memory before the "slow" network transfer below
        drop(contents);

        debug!("Upload: POST to {url:?} for {key:?}");
        writer.set_position(0);
        let data = writer.into_inner();

        if !self.dry_run {
            let response = self
                .authenticated_http_client
                .post(url)
                .body(data)
                .send()
                .await
                .process_error("upload_xorb")?;
            let response_parsed: UploadXorbResponse = response.json().await?;

            Ok((response_parsed.was_inserted, nbytes_trans))
        } else {
            Ok((true, nbytes_trans))
        }
    }

    /// use the reconstruction response from CAS to re-create the described file for any calls
    /// to download files from S3/blob store using urls from the fetch information section of
    /// the response it will use the provided http client.
    ///
    /// To reconstruct, this function will iterate through each CASReconstructionTerm in the `terms` section
    /// of the QueryReconstructionResponse, fetching the data for that term. Each term is a range of chunks
    /// from a specific Xorb. The range is an end exclusive [start, end) chunk index range of the xorb
    /// specified by the hash key of the CASReconstructionTerm.
    ///
    /// To fetch the data for each term, this function will consult the fetch_info section of the reconstruction
    /// response. See `get_one_term`.
    #[allow(clippy::too_many_arguments)]
    pub async fn reconstruct_file_to_writer(
        &self,
        terms: Vec<CASReconstructionTerm>,
        fetch_info: Arc<HashMap<HexMerkleHash, Vec<CASReconstructionFetchInfo>>>,
        offset_into_first_range: u64,
        byte_range: Option<FileRange>,
        writer: &OutputProvider,
        progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Result<u64> {
        let total_len = if let Some(range) = byte_range {
            range.end - range.start
        } else {
            terms.iter().fold(0, |acc, x| acc + x.unpacked_length as u64)
        };
        let mut writer = writer.get_writer_at(0)?;

        let futs_iter = terms.into_iter().map(|term| {
            get_one_term(
                self.http_client.clone(),
                self.chunk_cache.clone(),
                term,
                fetch_info.clone(),
                self.range_download_single_flight.clone(),
            )
        });
        let mut futs_buffered_enumerated = futures::stream::iter(futs_iter)
            .buffered(*NUM_CONCURRENT_RANGE_GETS)
            .enumerate();

        let mut remaining_len = total_len;
        while let Some((term_idx, term_data_result)) = futs_buffered_enumerated.next().await {
            let term_data = term_data_result.log_error(format!("error fetching 1 term at index {term_idx}"))?;
            let start = if term_idx == 0 {
                // use offset_into_first_range for the first term if needed
                max(0, offset_into_first_range as usize)
            } else {
                0
            };
            let end: usize = min(remaining_len + start as u64, term_data.len() as u64) as usize;
            writer.write_all(&term_data[start..end])?;
            let len_written = (end - start) as u64;
            remaining_len -= len_written;
            progress_updater.as_ref().inspect(|updater| updater.update(len_written));
        }

        writer.flush()?;

        Ok(total_len)
    }

    /// Uses the reconstruction response and optionally requested FileRange to re-create a file,
    /// outputting the file content via indicated OutputProvider.
    ///
    /// Unlike `reconstruct_file_to_writer`, this function will spawn a task for writing each
    /// term to the output, with each task writing to its part of the file.
    pub async fn reconstruct_file_to_writer_parallel(
        &self,
        terms: Vec<CASReconstructionTerm>,
        fetch_info: Arc<HashMap<HexMerkleHash, Vec<CASReconstructionFetchInfo>>>,
        offset_into_first_range: u64,
        byte_range: Option<FileRange>,
        output_provider: &OutputProvider,
        progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Result<u64> {
        let total_len = if let Some(range) = byte_range {
            range.end - range.start
        } else {
            terms.iter().fold(0, |acc, x| acc + x.unpacked_length as u64)
        };
        let task_info = TermWriteTask {
            http_client: self.http_client.clone(),
            chunk_cache: self.chunk_cache.clone(),
            range_download_single_flight: self.range_download_single_flight.clone(),
            fetch_info,
            semaphore: Arc::new(Semaphore::new(*NUM_CONCURRENT_RANGE_GETS)),
            output: output_provider.clone(),
        };
        // Build term tasks, computing the offsets needed for the downloaded term and
        // offset for the output.
        let mut bytes_written = 0;
        let mut remaining = total_len;
        let term_tasks = terms.into_iter().enumerate().map(|(idx, term)| {
            let start = if idx == 0 { offset_into_first_range as usize } else { 0 };
            let end = min(start as u64 + remaining, term.unpacked_length as u64) as usize;
            let file_offset = bytes_written;
            let len = (end - start) as u64;

            bytes_written += len;
            remaining -= len;

            let task = task_info.clone();
            task.write_term(term, start..end, file_offset)
        });

        // Spawn the tasks
        let mut handles = FuturesUnordered::new();
        term_tasks.for_each(|task| {
            let handle = self.threadpool.spawn(task);
            handles.push(handle);
        });

        // Join the tasks as they come in.
        let mut total_written = 0;
        while let Some(result) = handles.next().await {
            match result {
                Ok(Ok(len_written)) => {
                    progress_updater.as_ref().inspect(|updater| updater.update(len_written));
                    total_written += len_written;
                },
                Ok(Err(e)) => Err(e)?,
                Err(e) => Err(CasClientError::Other(format!("Error joining download task {e:?}")))?,
            }
        }
        Ok(total_written)
    }
}

/// Helper object containing the structs needed when downloading and writing a term during
/// reconstruction. Can be cheaply cloned so that the write_term function can be spawned for
/// each term.
#[derive(Clone)]
struct TermWriteTask {
    http_client: Arc<ClientWithMiddleware>,
    chunk_cache: Option<Arc<dyn ChunkCache>>,
    range_download_single_flight: RangeDownloadSingleFlight,
    fetch_info: Arc<HashMap<HexMerkleHash, Vec<CASReconstructionFetchInfo>>>,
    semaphore: Arc<Semaphore>,
    output: OutputProvider,
}

impl TermWriteTask {
    /// Download the term and write it to the underlying storage.
    async fn write_term(self, term: CASReconstructionTerm, term_range: Range<usize>, file_offset: u64) -> Result<u64> {
        // acquire permit from the semaphore limiting the download parallelism.
        let _permit = self
            .semaphore
            .acquire_owned()
            .await
            .log_error("Couldn't download term")
            .map_err(|_| CasClientError::Other("couldn't acquire semaphore".to_string()))?;

        // download the term
        let term_data =
            get_one_term(self.http_client, self.chunk_cache, term, self.fetch_info, self.range_download_single_flight)
                .await
                .log_error("error fetching 1 term")?;

        if term_range.end > term_data.len() {
            error!(
                "Error: expected term range: {} larger than received term length: {}",
                term_range.end,
                term_data.len()
            );
            return Err(CasClientError::Other("term range received invalid".to_string()));
        }
        let len = (term_range.end - term_range.start) as u64;

        // write the term
        let mut writer = self.output.get_writer_at(file_offset)?;
        writer.write_all(&term_data[term_range])?;
        writer.flush()?;
        Ok(len)
    }
}

/// fetch the data requested for the term argument (data from a range of chunks
/// in a xorb).
/// if provided, it will first check a ChunkCache for the existence of this range.
///
/// To fetch the data from S3/blob store, get_one_term will match the term's hash/xorb to a vec of
/// CASReconstructionFetchInfo in the fetch_info information. Then pick from this vec (by a linear scan)
/// a fetch_info term that contains the requested term's range. Then:
///     download the url -> deserialize chunks -> trim the output to the requested term's chunk range
///
/// If the fetch_info section (provided as in the QueryReconstructionResponse) fails to contain a term
/// that matches our requested CASReconstructionTerm, it is considered a bad output from the CAS API.
pub(crate) async fn get_one_term(
    http_client: Arc<ClientWithMiddleware>,
    chunk_cache: Option<Arc<dyn ChunkCache>>,
    term: CASReconstructionTerm,
    fetch_info: Arc<HashMap<HexMerkleHash, Vec<CASReconstructionFetchInfo>>>,
    range_download_single_flight: RangeDownloadSingleFlight,
) -> Result<Vec<u8>> {
    debug!("term: {term:?}");

    if term.range.end < term.range.start {
        return Err(CasClientError::InvalidRange);
    }

    // check disk cache for the exact range we want for the reconstruction term
    if let Some(cache) = &chunk_cache {
        let key = Key {
            prefix: PREFIX_DEFAULT.to_string(),
            hash: term.hash.into(),
        };
        if let Ok(Some(cached)) = cache.get(&key, &term.range).log_error("cache error") {
            return Ok(cached);
        }
    }

    // get the fetch info term for the key
    // then find the term within the ranges that will match our requested range
    // if either operation fails, this is a result of a bad response from the reconstruction api.
    let hash_fetch_info = fetch_info
        .get(&term.hash)
        .ok_or(CasClientError::InvalidArguments)
        .log_error("invalid response from CAS server: failed to get term hash in fetch info")?;
    let fetch_term = hash_fetch_info
        .iter()
        .find(|fterm| fterm.range.start <= term.range.start && fterm.range.end >= term.range.end)
        .ok_or(CasClientError::InvalidArguments)
        .log_error("invalid response from CAS server: failed to match hash in fetch_info")?
        .clone();

    // fetch the range from blob store and deserialize the chunks
    // then put into the cache if used
    let (mut data, chunk_byte_indices) = range_download_single_flight
        .work_dump_caller_info(&fetch_term.url, download_range(http_client, fetch_term.clone(), term.hash))
        .await?;

    // now write it to cache, the whole fetched term
    if let Some(cache) = chunk_cache {
        let key = Key {
            prefix: PREFIX_DEFAULT.to_string(),
            hash: term.hash.into(),
        };
        cache.put(&key, &fetch_term.range, &chunk_byte_indices, &data)?;
    }

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

fn range_header(range: &HttpRange) -> String {
    format!("bytes={}-{}", range.start, range.end)
}

/// use the provided http_client to make requests to S3/blob store using the url and url_range
/// parts of a CASReconstructionFetchInfo. The url_range part is used directly in a http Range header
/// value (see fn `range_header`).
async fn download_range(
    http_client: Arc<ClientWithMiddleware>,
    fetch_term: CASReconstructionFetchInfo,
    hash: HexMerkleHash,
) -> Result<(Vec<u8>, Vec<u32>)> {
    trace!("{hash},{},{}", fetch_term.range.start, fetch_term.range.end);

    let url = Url::parse(fetch_term.url.as_str())?;
    let response = http_client
        .get(url)
        .header(RANGE, range_header(&fetch_term.url_range))
        .send()
        .await
        .log_error("error getting from s3")?
        .error_for_status()
        .log_error("get from s3 error code")?;

    if let Some(content_length) = response.content_length() {
        // + 1 since range S3/HTTP range is inclusive on both ends
        // remove this check to be agnostic to range-end-exclusive blob store requests
        let expected_len = fetch_term.url_range.end - fetch_term.url_range.start + 1;
        if content_length != expected_len as u64 {
            error!("got back a smaller byte range ({content_length}) than requested ({expected_len}) from s3");
            return Err(CasClientError::InvalidRange);
        }
    }

    let (data, chunk_byte_indices) = cas_object::deserialize_async::deserialize_chunks_from_stream(
        response.bytes_stream().map_err(std::io::Error::other),
    )
    .await?;
    Ok((data, chunk_byte_indices))
}

#[async_trait]
impl RegistrationClient for RemoteClient {
    async fn upload_shard(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        force_sync: bool,
        shard_data: &[u8],
        _salt: &[u8; 32],
    ) -> Result<bool> {
        if self.dry_run {
            return Ok(true);
        }

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
            .authenticated_http_client
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
impl FileReconstructor<CasClientError> for RemoteClient {
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        let url = Url::parse(&format!("{}/reconstruction/{}", self.endpoint, file_hash.hex()))?;

        let response = self
            .authenticated_http_client
            .get(url)
            .send()
            .await
            .process_error("get_reconstruction_info")?;
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
impl ShardDedupProber for RemoteClient {
    async fn query_for_global_dedup_shard(
        &self,
        prefix: &str,
        chunk_hash: &MerkleHash,
        _salt: &[u8; 32],
    ) -> Result<Option<PathBuf>> {
        if self.shard_cache_directory == PathBuf::default() {
            return Err(CasClientError::ConfigurationError(
                "Shard Write Directory not set; cannot download.".to_string(),
            ));
        }

        // The API endpoint now only supports non-batched dedup request and
        // ignores salt.
        let key = Key {
            prefix: prefix.into(),
            hash: *chunk_hash,
        };

        let url = Url::parse(&format!("{0}/chunk/{key}", self.endpoint))?;

        let mut response = self
            .conservative_authenticated_http_client
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

impl ShardClientInterface for RemoteClient {}

#[cfg(test)]
mod tests {
    use cas_object::test_utils::{build_cas_object, ChunkSize};
    use cas_types::ChunkRange;
    use chunk_cache::MockChunkCache;
    use tracing_test::traced_test;

    use super::*;
    use crate::interface::buffer::BufferProvider;

    // test reconstruction contains 20 chunks, where each chunk size is 10 bytes
    const TEST_CHUNK_RANGE: ChunkRange = ChunkRange {
        start: 0,
        end: TEST_NUM_CHUNKS as u32,
    };
    const TEST_CHUNK_SIZE: usize = 10;
    const TEST_NUM_CHUNKS: usize = 20;
    const TEST_UNPACKED_LEN: u32 = (TEST_CHUNK_SIZE * TEST_NUM_CHUNKS) as u32;

    #[ignore = "requires a running CAS server"]
    #[traced_test]
    #[test]
    fn test_basic_put() {
        // Arrange
        let prefix = PREFIX_DEFAULT;
        let (c, _, data, chunk_boundaries) = build_cas_object(3, ChunkSize::Random(512, 10248), CompressionScheme::LZ4);

        let threadpool = Arc::new(ThreadPool::new().unwrap());
        let client = RemoteClient::new(
            threadpool.clone(),
            CAS_ENDPOINT,
            Some(CompressionScheme::LZ4),
            &None,
            &None,
            "".into(),
            false,
        );
        // Act
        let result = threadpool
            .external_run_async_task(async move { client.put(prefix, &c.info.cashash, data, chunk_boundaries).await })
            .unwrap();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_reconstruct_file_to_writer() {
        #[derive(Clone)]
        struct TestCase {
            reconstruction_response: QueryReconstructionResponse,
            range: Option<FileRange>,
            expected_len: u64,
            expect_error: bool,
        }
        let test_cases = vec![
            // full file reconstruction
            TestCase {
                reconstruction_response: QueryReconstructionResponse {
                    offset_into_first_range: 0,
                    terms: vec![CASReconstructionTerm {
                        hash: HexMerkleHash::default(),
                        range: TEST_CHUNK_RANGE,
                        unpacked_length: TEST_UNPACKED_LEN,
                    }],
                    fetch_info: HashMap::new(),
                },
                range: None,
                expected_len: TEST_UNPACKED_LEN as u64,
                expect_error: false,
            },
            // skip first 100 bytes
            TestCase {
                reconstruction_response: QueryReconstructionResponse {
                    offset_into_first_range: 100,
                    terms: vec![CASReconstructionTerm {
                        hash: HexMerkleHash::default(),
                        range: TEST_CHUNK_RANGE,
                        unpacked_length: TEST_UNPACKED_LEN,
                    }],
                    fetch_info: HashMap::new(),
                },
                range: Some(FileRange {
                    start: 100,
                    end: TEST_UNPACKED_LEN as u64,
                }),
                expected_len: (TEST_UNPACKED_LEN - 100) as u64,
                expect_error: false,
            },
            // skip last 100 bytes
            TestCase {
                reconstruction_response: QueryReconstructionResponse {
                    offset_into_first_range: 0,
                    terms: vec![CASReconstructionTerm {
                        hash: HexMerkleHash::default(),
                        range: TEST_CHUNK_RANGE,
                        unpacked_length: TEST_UNPACKED_LEN,
                    }],
                    fetch_info: HashMap::new(),
                },
                range: Some(FileRange {
                    start: 0,
                    end: (TEST_UNPACKED_LEN - 100) as u64,
                }),
                expected_len: (TEST_UNPACKED_LEN - 100) as u64,
                expect_error: false,
            },
            // skip first and last 100 bytes, 2 terms
            TestCase {
                reconstruction_response: QueryReconstructionResponse {
                    offset_into_first_range: 100,
                    terms: vec![
                        CASReconstructionTerm {
                            hash: HexMerkleHash::default(),
                            range: TEST_CHUNK_RANGE,
                            unpacked_length: TEST_UNPACKED_LEN,
                        };
                        2
                    ],
                    fetch_info: HashMap::new(),
                },
                range: Some(FileRange {
                    start: 100,
                    end: (2 * TEST_UNPACKED_LEN - 100) as u64,
                }),
                expected_len: (2 * TEST_UNPACKED_LEN - 200) as u64,
                expect_error: false,
            },
        ];
        for test in test_cases {
            let test1 = test.clone();
            // test writing to file term-by-term
            let mut chunk_cache = MockChunkCache::new();
            chunk_cache
                .expect_get()
                .returning(|_, range| Ok(Some(vec![1; (range.end - range.start) as usize * TEST_CHUNK_SIZE])));

            let http_client = Arc::new(http_client::build_http_client(RetryConfig::default()).unwrap());

            let threadpool = Arc::new(ThreadPool::new().unwrap());
            let client = RemoteClient {
                chunk_cache: Some(Arc::new(chunk_cache)),
                authenticated_http_client: http_client.clone(),
                conservative_authenticated_http_client: http_client.clone(),
                http_client,
                endpoint: "".to_string(),
                compression: Some(CompressionScheme::LZ4),
                dry_run: false,
                threadpool: threadpool.clone(),
                range_download_single_flight: Arc::new(Group::new()),
                shard_cache_directory: "".into(),
            };

            let provider = BufferProvider::default();
            let buf = provider.buf.clone();
            let writer = OutputProvider::Buffer(provider);
            let resp = threadpool
                .external_run_async_task(async move {
                    client
                        .reconstruct_file_to_writer(
                            test1.reconstruction_response.terms,
                            Arc::new(test1.reconstruction_response.fetch_info),
                            test1.reconstruction_response.offset_into_first_range,
                            test1.range,
                            &writer,
                            None,
                        )
                        .await
                })
                .unwrap();
            assert_eq!(test1.expect_error, resp.is_err());
            if !test1.expect_error {
                assert_eq!(test1.expected_len, resp.unwrap());
                assert_eq!(vec![1; test1.expected_len as usize], buf.value());
            }

            // test writing terms to file in parallel
            let mut chunk_cache = MockChunkCache::new();
            chunk_cache
                .expect_get()
                .returning(|_, range| Ok(Some(vec![1; (range.end - range.start) as usize * TEST_CHUNK_SIZE])));

            let http_client = Arc::new(http_client::build_http_client(RetryConfig::default()).unwrap());
            let authenticated_http_client = http_client.clone();
            let conservative_authenticated_http_client =
                Arc::new(http_client::build_http_client(RetryConfig::no429retry()).unwrap());

            let threadpool = Arc::new(ThreadPool::new().unwrap());
            let client = RemoteClient {
                chunk_cache: Some(Arc::new(chunk_cache)),
                authenticated_http_client,
                http_client,
                endpoint: "".to_string(),
                compression: Some(CompressionScheme::LZ4),
                dry_run: false,
                threadpool: threadpool.clone(),
                range_download_single_flight: Arc::new(Group::new()),
                shard_cache_directory: "".into(),
                conservative_authenticated_http_client,
            };
            let provider = BufferProvider::default();
            let buf = provider.buf.clone();
            let writer = OutputProvider::Buffer(provider);
            let resp = threadpool
                .external_run_async_task(async move {
                    client
                        .reconstruct_file_to_writer_parallel(
                            test.reconstruction_response.terms,
                            Arc::new(test.reconstruction_response.fetch_info),
                            test.reconstruction_response.offset_into_first_range,
                            test.range,
                            &writer,
                            None,
                        )
                        .await
                })
                .unwrap();

            assert_eq!(test.expect_error, resp.is_err());
            if !test.expect_error {
                assert_eq!(test.expected_len, resp.unwrap());
                assert_eq!(vec![1; test.expected_len as usize], buf.value());
            }
        }
    }
}
