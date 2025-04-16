use std::io::{Cursor, Write};
use std::mem::take;
use std::path::PathBuf;
use std::result::Result as stdResult;
use std::sync::atomic::{AtomicBool, Ordering};
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
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use http::header::RANGE;
use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::utils::shard_file_name;
use merklehash::{HashedWrite, MerkleHash};
use reqwest::{StatusCode, Url};
use reqwest_middleware::ClientWithMiddleware;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle, JoinSet};
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
    ref NUM_CONCURRENT_RANGE_GETS: usize = GlobalConfigMode::HighPerformanceOption {
        standard: 16,
        high_performance: 100,
    };

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
        let file_reconstruct_range = byte_range.unwrap_or_else(FileRange::full);
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
        let end_signal = Arc::new(AtomicBool::new(false));
        let end_signal_clone = end_signal.clone();

        let queue_dispatcher: JoinHandle<Result<()>> = self.threadpool.spawn(async move {
            let mut remaining_total_len = total_len;
            while let Some(item) = task_rx.recv().await {
                match item {
                    DownloadQueueItem::End => {
                        // everything processed
                        info!("download queue emptyed");
                        end_signal_clone.store(true, Ordering::Relaxed);
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
                        let mut remaining_segment_len = segment_size;
                        info!("enqueueing {} download tasks", terms.len());
                        for (i, term) in terms.into_iter().enumerate() {
                            let skip_bytes = if i == 0 { offset_into_first_range } else { 0 };
                            let take = remaining_total_len
                                .min(remaining_segment_len)
                                .min(term.unpacked_length as u64 - skip_bytes);

                            let download_task = TermDownload {
                                term,
                                skip_bytes,
                                take,
                                fetch_info: segment.clone(),
                                chunk_cache: chunk_cache.clone(),
                                range_download_single_flight: range_download_single_flight.clone(),
                            };

                            remaining_total_len -= take;
                            remaining_segment_len -= take;
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
        loop {
            // polling the futures queue will return None when there's no future in it yet.
            let Some(result) = running_downloads.lock().await.next().await else {
                if end_signal.load(Ordering::Relaxed) {
                    break;
                } else {
                    continue;
                }
            };
            match result {
                Ok(Ok(mut download_result)) => {
                    let data = take(&mut download_result.data);
                    writer.write_all(&data)?;
                    progress_updater.as_ref().inspect(|updater| updater.update(data.len() as u64));
                    total_written += data.len() as u64;

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
        let mut running_downloads = JoinSet::<Result<TermDownloadResult<usize>>>::new();

        // derive the actual range to reconstruct
        let file_reconstruct_range = byte_range.unwrap_or_else(FileRange::full);
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
        let download_scheduler = DownloadScheduler::new(*NUM_CONCURRENT_RANGE_GETS);

        let process_result =
            move |result: stdResult<stdResult<TermDownloadResult<usize>, CasClientError>, JoinError>,
                  total_written: &mut u64,
                  download_scheduler: &DownloadScheduler|
                  -> Result<()> {
                match result {
                    Ok(Ok(download_result)) => {
                        let write_len = download_result.data;
                        *total_written += write_len as u64;
                        progress_updater.as_ref().inspect(|updater| updater.update(write_len as u64));

                        // Now inspect the download metrics and tune the download degree of concurrency
                        download_scheduler.tune_on(download_result)?;
                        Ok(())
                    },
                    Ok(Err(e)) => Err(e)?,
                    Err(e) => Err(anyhow!("{e:?}"))?,
                }
            };

        let mut total_written = 0;
        let mut remaining_total_len = total_len;
        while let Some(item) = task_rx.recv().await {
            // first try to join some tasks
            while let Some(result) = running_downloads.try_join_next() {
                process_result(result, &mut total_written, &download_scheduler)?;
            }

            match item {
                DownloadQueueItem::End => {
                    // everything processed
                    debug!("download queue emptyed");
                    break;
                },
                DownloadQueueItem::Term(term_download) => {
                    // acquire the permit before spawning the task, so that there's limited
                    // number of active downloads.
                    let permit = download_scheduler.download_permit().await?;
                    debug!("spawning 1 download task");
                    running_downloads.spawn(async move {
                        let data = term_download.run().await?;
                        drop(permit);
                        Ok(data)
                    });
                },
                DownloadQueueItem::Metadata(fetch_info) => {
                    // query for the file info of the first segment
                    let segment_size = download_scheduler.next_segment_size()?;
                    debug!("querying file info of size {segment_size}");
                    let (segment, maybe_remainder) = fetch_info.take_segment(segment_size);

                    let Some((offset_into_first_range, terms)) = segment.query().await? else {
                        // signal termination
                        task_tx.send(DownloadQueueItem::End)?;
                        continue;
                    };

                    let segment = Arc::new(segment);
                    // define the term download tasks
                    let mut remaining_segment_len = segment_size;
                    debug!("enqueueing {} download tasks", terms.len());
                    for (i, term) in terms.into_iter().enumerate() {
                        let skip_bytes = if i == 0 { offset_into_first_range } else { 0 };
                        let take = remaining_total_len
                            .min(remaining_segment_len)
                            .min(term.unpacked_length as u64 - skip_bytes);

                        let download_and_write_task = TermDownloadAndWrite {
                            download: TermDownload {
                                term,
                                skip_bytes,
                                take,
                                fetch_info: segment.clone(),
                                chunk_cache: self.chunk_cache.clone(),
                                range_download_single_flight: self.range_download_single_flight.clone(),
                            },
                            write_offset: total_len - remaining_total_len,
                            output: writer.clone(),
                        };

                        remaining_total_len -= take;
                        remaining_segment_len -= take;
                        debug!("enqueueing {download_and_write_task:?}");
                        task_tx.send(DownloadQueueItem::Term(download_and_write_task))?;
                    }

                    // enqueue the remainder of file info fetch task
                    if let Some(remainder) = maybe_remainder {
                        task_tx.send(DownloadQueueItem::Metadata(remainder))?;
                    }
                },
            }
        }

        while let Some(result) = running_downloads.join_next().await {
            process_result(result, &mut total_written, &download_scheduler)?;
        }

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
}
