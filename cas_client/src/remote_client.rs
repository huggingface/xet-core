use std::cmp::min;
use std::io::{Cursor, Write};
use std::mem::take;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use cas_object::{CasObject, CompressionScheme};
use cas_types::{
    BatchQueryReconstructionResponse, FileRange, HttpRange, Key, QueryReconstructionResponse, UploadShardResponse,
    UploadShardResponseType, UploadXorbResponse,
};
use chunk_cache::{CacheConfig, ChunkCache};
use error_printer::ErrorPrinter;
use file_utils::SafeFileCreator;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::StreamExt;
use http::header::RANGE;
use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::utils::shard_file_name;
use merklehash::{HashedWrite, MerkleHash};
use reqwest::{StatusCode, Url};
use reqwest_middleware::ClientWithMiddleware;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info};
use utils::auth::AuthConfig;
use utils::progress::ProgressUpdater;
use utils::singleflight::Group;
use xet_threadpool::ThreadPool;

use crate::download_utils::*;
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
        // If the user has set the `HF_XET_RECONSTRUCT_WRITE_SEQUENTIALLY=true` env variable, then we
        // should write the file to the output sequentially instead of in parallel.
        if *RECONSTRUCT_WRITE_SEQUENTIALLY {
            info!("reconstruct terms sequentially");
            self.reconstruct_file_to_writer_segmented(hash, byte_range, output_provider, progress_updater)
                .await
        } else {
            info!("reconstruct terms in parallel");
            self.reconstruct_file_to_writer_segmented_parallel_write(
                hash,
                byte_range,
                output_provider,
                progress_updater,
            )
            .await
        }
    }
}

#[async_trait]
impl Reconstructable for RemoteClient {
    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>> {
        get_reconstruction_with_endpoint_and_client(
            &self.endpoint,
            &self.authenticated_http_client,
            file_id,
            bytes_range,
        )
        .await
    }
}

pub(crate) async fn get_reconstruction_with_endpoint_and_client(
    endpoint: &str,
    client: &ClientWithMiddleware,
    file_id: &MerkleHash,
    bytes_range: Option<FileRange>,
) -> Result<Option<QueryReconstructionResponse>> {
    let url = Url::parse(&format!("{}/reconstruction/{}", endpoint, file_id.hex()))?;

    let mut request = client.get(url);
    if let Some(range) = bytes_range {
        // convert exclusive-end to inclusive-end range
        request = request.header(RANGE, HttpRange::from(range).range_header())
    }
    let response = request.send().await.process_error("get_reconstruction");

    let Ok(response) = response else {
        let e = response.unwrap_err();

        // bytes_range not satisfiable
        if let CasClientError::ReqwestError(e) = &e {
            if let Some(StatusCode::RANGE_NOT_SATISFIABLE) = e.status() {
                return Ok(None);
            }
        }

        return Err(e);
    };

    let len = response.content_length();
    debug!("file_id: {file_id} query_reconstruction len {len:?}");

    let query_reconstruction_response: QueryReconstructionResponse = response
        .json()
        .await
        .log_error("error json parsing QueryReconstructionResponse")?;
    Ok(Some(query_reconstruction_response))
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

    // Segmented download such that the file reconstruction and fetch info is not queried in its entirety
    // at the beginning of the download, but queried in segments. Range downloadeds are executed with
    // a certain degree of parallilism, but writing out to storage is sequential. Ideal when the external
    // storage uses HDDs.
    async fn reconstruct_file_to_writer_segmented(
        &self,
        file_hash: &MerkleHash,
        byte_range: Option<FileRange>,
        writer: &OutputProvider,
        progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Result<u64> {
        // queue size is inherently bounded by degree of concurrency.
        let (task_tx, mut task_rx) = mpsc::unbounded_channel::<DownloadQueueItem<TermDownload>>();
        let running_downloads = Arc::new(tokio::sync::Mutex::new(FuturesOrdered::new()));

        // derive the actual range to reconstruct
        let file_reconstruct_range = byte_range.unwrap_or_else(|| FileRange::full());
        let total_len = file_reconstruct_range.length();

        // kick start the download by enqueue the fetch info task.
        task_tx.send(DownloadQueueItem::Metadata(FetchInfo::new(
            *file_hash,
            file_reconstruct_range,
            self.endpoint.clone(),
            self.authenticated_http_client.clone(),
        )))?;

        // Start the queue processing logic
        //
        // If the queue item is `DownloadQueueItem::Metadata`, it fetches the file reconstruction info
        // of the first segment, whose size is linear to `num_concurrent_range_gets`. Once fetched, term
        // download tasks are enqueued and spawned with the degree of concurrency equal to `num_concurrent_range_gets`.
        // After the above, a task that defines fetching the remainder of the file reconstruction info is enqueued,
        // which will execute after the first of the above term download tasks finishes.
        let threadpool = self.threadpool.clone();
        let running_downloads_clone = running_downloads.clone();
        let chunk_cache = self.chunk_cache.clone();
        let range_download_single_flight = self.range_download_single_flight.clone();
        let download_scheduler = DownloadScheduler::new(*NUM_CONCURRENT_RANGE_GETS);
        let download_scheduler_clone = download_scheduler.clone();

        let queue_dispatcher: JoinHandle<Result<()>> = self.threadpool.spawn(async move {
            while let Some(item) = task_rx.recv().await {
                match item {
                    DownloadQueueItem::End => {
                        // everything processed
                        break;
                    },
                    DownloadQueueItem::Term(term_download) => {
                        // acquire the permit before spawning the task, so that there's limited
                        // number of active downloads.
                        let permit = download_scheduler_clone.download_permit().await?;
                        debug!("spawning 1 download task");
                        let future: JoinHandle<Result<TermDownloadResult<Vec<u8>>>> = threadpool.spawn(async move {
                            let data = term_download.run().await?;
                            drop(permit);
                            Ok(data)
                        });
                        running_downloads_clone.lock().await.push_back(future);
                    },
                    DownloadQueueItem::Metadata(fetch_info) => {
                        // query for the file info of the first segment
                        let segment_size = download_scheduler_clone.next_segment_size()?;
                        info!("querying file info of size {segment_size}");
                        let (segment, maybe_remainder) = fetch_info.take_segment(segment_size);

                        let Some((offset_into_first_range, terms)) = segment.query().await? else {
                            // signal termination
                            task_tx.send(DownloadQueueItem::End)?;
                            continue;
                        };

                        let segment = Arc::new(segment);
                        // define the term download tasks
                        info!("enqueueing {} download tasks", terms.len());
                        for (i, term) in terms.into_iter().enumerate() {
                            let download_task = TermDownload {
                                term,
                                skip_bytes: if i == 0 { offset_into_first_range } else { 0 },
                                fetch_info: segment.clone(),
                                chunk_cache: chunk_cache.clone(),
                                range_download_single_flight: range_download_single_flight.clone(),
                            };
                            debug!("enqueueing {download_task:?}");
                            task_tx.send(DownloadQueueItem::Term(download_task))?;
                        }

                        // enqueue the remainder of file info fetch task
                        if let Some(remainder) = maybe_remainder {
                            task_tx.send(DownloadQueueItem::Metadata(remainder))?;
                        }
                    },
                }
            }

            Ok(())
        });

        let mut writer = writer.get_writer_at(0)?;
        let mut total_written = 0;
        while let Some(result) = running_downloads.lock().await.next().await {
            match result {
                Ok(Ok(mut download_result)) => {
                    let data = take(&mut download_result.data);
                    // If not get file with `Some(byte_range)`, total_len is u64::MAX and this will always
                    // write the full range.
                    let write_len = min(total_len - total_written, data.len() as u64);
                    writer.write_all(&data[..write_len as usize])?;
                    total_written += write_len;
                    info!("writing {write_len} bytes, total {total_written} bytes");
                    progress_updater.as_ref().inspect(|updater| updater.update(write_len));

                    // Now inspect the download metrics and tune the download degree of concurrency
                    download_scheduler.tune_on(download_result)?;
                },
                Ok(Err(e)) => Err(e)?,
                Err(e) => Err(anyhow!("{e:?}"))?,
            }
        }
        writer.flush()?;

        queue_dispatcher.await??;

        Ok(total_written)
    }

    // Segmented download such that the file reconstruction and fetch info is not queried in its entirety
    // at the beginning of the download, but queried in segments. Range downloadeds are executed with
    // a certain degree of parallilism, and so does writing out to storage. Ideal when the external
    // storage is fast at seeks, e.g. RAM or SSDs.
    async fn reconstruct_file_to_writer_segmented_parallel_write(
        &self,
        file_hash: &MerkleHash,
        byte_range: Option<FileRange>,
        writer: &OutputProvider,
        progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Result<u64> {
        // queue size is inherently bounded by degree of concurrency.
        let (task_tx, mut task_rx) = mpsc::unbounded_channel::<DownloadQueueItem<TermDownloadAndWrite>>();
        // note this uses `FuturesUnordered`
        let running_downloads = Arc::new(tokio::sync::Mutex::new(FuturesUnordered::new()));

        // derive the actual range to reconstruct
        let file_reconstruct_range = byte_range.unwrap_or_else(|| FileRange::full());
        let total_len = file_reconstruct_range.length();

        // kick start the download by enqueue the fetch info task.
        task_tx.send(DownloadQueueItem::Metadata(FetchInfo::new(
            *file_hash,
            file_reconstruct_range,
            self.endpoint.clone(),
            self.authenticated_http_client.clone(),
        )))?;

        // Start the queue processing logic
        //
        // If the queue item is `DownloadQueueItem::Metadata`, it fetches the file reconstruction info
        // of the first segment, whose size is linear to `num_concurrent_range_gets`. Once fetched, term
        // download tasks are enqueued and spawned with the degree of concurrency equal to `num_concurrent_range_gets`.
        // After the above, a task that defines fetching the remainder of the file reconstruction info is enqueued,
        // which will execute after the first of the above term download tasks finishes.
        let threadpool = self.threadpool.clone();
        let running_downloads_clone = running_downloads.clone();
        let chunk_cache = self.chunk_cache.clone();
        let range_download_single_flight = self.range_download_single_flight.clone();
        let download_scheduler = DownloadScheduler::new(*NUM_CONCURRENT_RANGE_GETS);
        let download_scheduler_clone = download_scheduler.clone();
        let writer_clone = writer.clone();

        let queue_dispatcher: JoinHandle<Result<()>> = self.threadpool.spawn(async move {
            let mut total_written = 0;
            while let Some(item) = task_rx.recv().await {
                match item {
                    DownloadQueueItem::End => {
                        // everything processed
                        break;
                    },
                    DownloadQueueItem::Term(term_download) => {
                        // acquire the permit before spawning the task, so that there's limited
                        // number of active downloads.
                        let permit = download_scheduler_clone.download_permit().await?;
                        info!("spawning 1 download task");
                        let future: JoinHandle<Result<TermDownloadResult<usize>>> = threadpool.spawn(async move {
                            let data = term_download.run().await?;
                            drop(permit);
                            Ok(data)
                        });
                        running_downloads_clone.lock().await.push(future);
                    },
                    DownloadQueueItem::Metadata(fetch_info) => {
                        // query for the file info of the first segment
                        let segment_size = download_scheduler_clone.next_segment_size()?;
                        info!("querying file info of size {segment_size}");
                        let (segment, maybe_remainder) = fetch_info.take_segment(segment_size);

                        let Some((offset_into_first_range, terms)) = segment.query().await? else {
                            // signal termination
                            task_tx.send(DownloadQueueItem::End)?;
                            continue;
                        };

                        let segment = Arc::new(segment);
                        // define the term download tasks
                        info!("enqueueing {} download tasks", terms.len());
                        for (i, term) in terms.into_iter().enumerate() {
                            let write_len = min(total_len - total_written, term.unpacked_length as u64);
                            let download_and_write_task = TermDownloadAndWrite {
                                download: TermDownload {
                                    term,
                                    skip_bytes: if i == 0 { offset_into_first_range } else { 0 },
                                    fetch_info: segment.clone(),
                                    chunk_cache: chunk_cache.clone(),
                                    range_download_single_flight: range_download_single_flight.clone(),
                                },
                                take: write_len as usize,
                                write_offset: total_written,
                                output: writer_clone.clone(),
                            };
                            debug!("enqueueing {download_and_write_task:?}");
                            task_tx.send(DownloadQueueItem::Term(download_and_write_task))?;

                            total_written += write_len;
                        }

                        // enqueue the remainder of file info fetch task
                        if let Some(remainder) = maybe_remainder {
                            task_tx.send(DownloadQueueItem::Metadata(remainder))?;
                        }
                    },
                }
            }

            Ok(())
        });

        let mut total_written = 0;
        while let Some(result) = running_downloads.lock().await.next().await {
            match result {
                Ok(Ok(download_result)) => {
                    let write_len = download_result.data;
                    total_written += write_len as u64;
                    progress_updater.as_ref().inspect(|updater| updater.update(write_len as u64));

                    // Now inspect the download metrics and tune the download degree of concurrency
                    download_scheduler.tune_on(download_result)?;
                },
                Ok(Err(e)) => Err(e)?,
                Err(e) => Err(anyhow!("{e:?}"))?,
            }
        }

        queue_dispatcher.await??;

        Ok(total_written)
    }
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

        let writer = SafeFileCreator::new_unnamed(&self.shard_cache_directory)?;
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
    use std::collections::HashMap;

    use cas_object::test_utils::{build_cas_object, ChunkSize};
    use cas_types::{CASReconstructionTerm, ChunkRange, HexMerkleHash};
    use chunk_cache::MockChunkCache;
    use tracing_test::traced_test;

    use super::*;
    use crate::interface::buffer::BufferProvider;

    // test reconstruction contains 20 chunks, where each chunk size is 10 bytes
    const TEST_CHUNK_RANGE: ChunkRange = ChunkRange {
        start: 0,
        end: TEST_NUM_CHUNKS as u32,
        _marker: std::marker::PhantomData,
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
                range: Some(FileRange::new(100, TEST_UNPACKED_LEN as u64)),
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
                range: Some(FileRange::new(0, (TEST_UNPACKED_LEN - 100) as u64)),
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
                range: Some(FileRange::new(100, (2 * TEST_UNPACKED_LEN - 100) as u64)),
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
