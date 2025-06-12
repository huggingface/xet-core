use std::collections::HashMap;
use std::io::Write;
use std::mem::take;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use cas_object::SerializedCasObject;
use cas_types::{
    BatchQueryReconstructionResponse, CASReconstructionTerm, ChunkRange, FileRange, HttpRange, Key,
    QueryReconstructionResponse, UploadShardResponse, UploadShardResponseType, UploadXorbResponse,
};
use chunk_cache::{CacheConfig, ChunkCache};
use error_printer::ErrorPrinter;
use file_utils::SafeFileCreator;
use http::header::{CONTENT_LENGTH, RANGE};
use http::HeaderValue;
use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::utils::shard_file_name;
use merklehash::{HashedWrite, MerkleHash};
use progress_tracking::item_tracking::SingleItemProgressUpdater;
use progress_tracking::upload_tracking::CompletionTracker;
use reqwest::{Body, StatusCode, Url};
use reqwest_middleware::ClientWithMiddleware;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, info, instrument};
use utils::auth::AuthConfig;
use utils::singleflight::Group;

use crate::download_utils::*;
use crate::error::{CasClientError, Result};
use crate::http_client::{Api, ResponseErrorLogger, RetryConfig};
use crate::interface::{ShardDedupProber, *};
use crate::retry_utils::retry_wrapper;
use crate::{http_client, Client, RegistrationClient, ShardClientInterface};

const FORCE_SYNC_METHOD: reqwest::Method = reqwest::Method::PUT;
const NON_FORCE_SYNC_METHOD: reqwest::Method = reqwest::Method::POST;

pub const CAS_ENDPOINT: &str = "http://localhost:8080";
pub const PREFIX_DEFAULT: &str = "default";

utils::configurable_constants! {
// Env (HF_XET_NUM_CONCURRENT_RANGE_GETS) to set the number of concurrent range gets.
// setting this value to 0 disables the limit, sets it to the max, this is not recommended as it may lead to errors
    ref NUM_CONCURRENT_RANGE_GETS: usize = GlobalConfigMode::HighPerformanceOption {
        standard: 128,
        high_performance: 512,
    };

    // Send a report of successful partial upload every 512kb.
    ref UPLOAD_REPORTING_BLOCK_SIZE : usize = 512 * 1024;
}

utils::configurable_bool_constants! {
// Env (HF_XET_RECONSTRUCT_WRITE_SEQUENTIALLY) to switch to writing terms sequentially to disk.
// Benchmarks have shown that on SSD machines, writing in parallel seems to far outperform
// sequential term writes.
// However, this is not likely the case for writing to HDD and may in fact be worse,
// so for those machines, setting this env may help download perf.
    ref RECONSTRUCT_WRITE_SEQUENTIALLY = false;
}

pub struct RemoteClient {
    endpoint: String,
    dry_run: bool,
    http_client: Arc<ClientWithMiddleware>,
    authenticated_http_client: Arc<ClientWithMiddleware>,
    authenticated_http_client_no_retry: Arc<ClientWithMiddleware>,
    conservative_authenticated_http_client: Arc<ClientWithMiddleware>,
    chunk_cache: Option<Arc<dyn ChunkCache>>,
    range_download_single_flight: RangeDownloadSingleFlight,
    concurrent_gets_semaphore: Arc<Semaphore>,
    shard_cache_directory: PathBuf,
}

impl RemoteClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint: &str,
        auth: &Option<AuthConfig>,
        cache_config: &Option<CacheConfig>,
        shard_cache_directory: PathBuf,
        session_id: &str,
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
        let num_range_gets = if *NUM_CONCURRENT_RANGE_GETS == 0 {
            Semaphore::MAX_PERMITS // virtually no limit
        } else {
            NUM_CONCURRENT_RANGE_GETS.min(Semaphore::MAX_PERMITS)
        };
        let concurrent_gets_semaphore = Arc::new(Semaphore::new(num_range_gets));

        Self {
            endpoint: endpoint.to_string(),
            dry_run,
            authenticated_http_client: Arc::new(
                http_client::build_auth_http_client(auth, RetryConfig::default(), session_id).unwrap(),
            ),
            authenticated_http_client_no_retry: Arc::new(
                http_client::build_auth_http_client_no_retry(auth, session_id).unwrap(),
            ),
            conservative_authenticated_http_client: Arc::new(
                http_client::build_auth_http_client(auth, RetryConfig::no429retry(), session_id).unwrap(),
            ),
            http_client: Arc::new(http_client::build_http_client(RetryConfig::default(), session_id).unwrap()),
            chunk_cache,
            range_download_single_flight,
            concurrent_gets_semaphore,
            shard_cache_directory,
        }
    }
}

#[async_trait]
impl UploadClient for RemoteClient {
    #[instrument(skip_all, name = "RemoteClient::upload_xorb", fields(key = Key{prefix : prefix.to_string(), hash : serialized_cas_object.hash}.to_string(),
                 xorb.len = serialized_cas_object.serialized_data.len(), xorb.num_chunks = serialized_cas_object.num_chunks
    ))]
    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_cas_object: SerializedCasObject,
        upload_tracker: Option<Arc<CompletionTracker>>,
    ) -> Result<u64> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: serialized_cas_object.hash,
        };

        let url = Url::parse(&format!("{}/xorb/{key}", self.endpoint))?;

        let n_upload_bytes = serialized_cas_object.serialized_data.len() as u64;

        // Backing out the incremental progress reporting for now until we figure out the middleware issue.
        use crate::upload_progress_stream::UploadProgressStream;

        let n_raw_bytes = serialized_cas_object.raw_num_bytes;
        let xorb_hash = serialized_cas_object.hash;

        let progress_callback = move |bytes_sent: u64| {
            if let Some(utr) = upload_tracker.as_ref() {
                // First, recallibrate the sending, as the compressed size is different than the actual data size.
                let adjusted_update = (bytes_sent * n_raw_bytes) / n_upload_bytes;

                utr.clone().register_xorb_upload_progress_background(xorb_hash, adjusted_update);
            }
        };

        let upload_stream = UploadProgressStream::new(
            serialized_cas_object.serialized_data,
            *UPLOAD_REPORTING_BLOCK_SIZE,
            progress_callback,
        );

        let xorb_uploaded = {
            if !self.dry_run {
                let client = self.authenticated_http_client_no_retry.clone();

                let response = retry_wrapper(
                    move || {
                        let upload_stream = upload_stream.clone_with_reset();
                        let url = url.clone();

                        client
                            .post(url)
                            .with_extension(Api("cas::upload_xorb"))
                            .header(CONTENT_LENGTH, HeaderValue::from(n_upload_bytes)) // must be set because of streaming
                            .body(Body::wrap_stream(upload_stream))
                            .send()
                    },
                    RetryConfig::default(),
                )
                .await?;

                let response_parsed: UploadXorbResponse = response.json().await?;

                response_parsed.was_inserted
            } else {
                true
            }
        };

        if !xorb_uploaded {
            debug!("{key:?} not inserted into CAS.");
        } else {
            debug!("{key:?} inserted into CAS.");
        }

        Ok(n_upload_bytes)
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
            e => Err(CasClientError::internal(format!("unrecognized status code {e}"))),
        }
    }

    fn use_xorb_footer(&self) -> bool {
        false
    }
}

#[async_trait]
impl ReconstructionClient for RemoteClient {
    async fn get_file(
        &self,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        output_provider: &OutputProvider,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
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

    let mut request = client.get(url).with_extension(Api("cas::get_reconstruction"));
    if let Some(range) = bytes_range {
        // convert exclusive-end to inclusive-end range
        request = request.header(RANGE, HttpRange::from(range).range_header())
    }
    let response = request.send().await.process_error("get_reconstruction");

    let Ok(response) = response else {
        let e = response.unwrap_err();

        // bytes_range not satisfiable
        if let CasClientError::ReqwestError(e, _) = &e {
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
    #[instrument(skip_all, name = "RemoteClient::batch_get_reconstruction")]
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
            .with_extension(Api("cas::batch_get_reconstruction"))
            .send()
            .await
            .process_error("batch_get_reconstruction")?;

        let query_reconstruction_response: BatchQueryReconstructionResponse = response
            .json()
            .await
            .log_error("error json parsing BatchQueryReconstructionResponse")?;
        Ok(query_reconstruction_response)
    }

    // Segmented download such that the file reconstruction and fetch info is not queried in its entirety
    // at the beginning of the download, but queried in segments. Range downloads are executed with
    // a certain degree of parallelism, but writing out to storage is sequential. Ideal when the external
    // storage uses HDDs.
    #[instrument(skip_all, name = "RemoteClient::reconstruct_file_segmented", fields(file.hash = file_hash.hex()
    ))]
    async fn reconstruct_file_to_writer_segmented(
        &self,
        file_hash: &MerkleHash,
        byte_range: Option<FileRange>,
        writer: &OutputProvider,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64> {
        // queue size is inherently bounded by degree of concurrency.
        let (task_tx, mut task_rx) = mpsc::unbounded_channel::<DownloadQueueItem<SequentialTermDownload>>();
        let (running_downloads_tx, mut running_downloads_rx) =
            mpsc::unbounded_channel::<JoinHandle<Result<(TermDownloadResult<Vec<u8>>, OwnedSemaphorePermit)>>>();

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
        let chunk_cache = self.chunk_cache.clone();
        let term_download_client = self.http_client.clone();
        let range_download_single_flight = self.range_download_single_flight.clone();
        let download_scheduler = DownloadSegmentLengthTuner::from_configurable_constants();
        let download_scheduler_clone = download_scheduler.clone();
        let concurrent_gets_semaphore = self.concurrent_gets_semaphore.clone();

        let queue_dispatcher: JoinHandle<Result<()>> = tokio::spawn(async move {
            let mut remaining_total_len = total_len;
            while let Some(item) = task_rx.recv().await {
                match item {
                    DownloadQueueItem::End => {
                        // everything processed
                        debug!("download queue emptyed");
                        drop(running_downloads_tx);
                        break;
                    },
                    DownloadQueueItem::DownloadTask(term_download) => {
                        // acquire the permit before spawning the task, so that there's limited
                        // number of active downloads.
                        let permit = concurrent_gets_semaphore.clone().acquire_owned().await?;
                        debug!("spawning 1 download task");
                        let future: JoinHandle<Result<(TermDownloadResult<Vec<u8>>, OwnedSemaphorePermit)>> =
                            tokio::spawn(async move {
                                let data = term_download.run().await?;
                                Ok((data, permit))
                            });
                        running_downloads_tx.send(future)?;
                    },
                    DownloadQueueItem::Metadata(fetch_info) => {
                        // query for the file info of the first segment
                        let segment_size = download_scheduler_clone.next_segment_size()?;
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
                            let (individual_fetch_info, _) = segment.find((term.hash, term.range)).await?;

                            let download_task = SequentialTermDownload {
                                download: FetchTermDownload {
                                    hash: term.hash.into(),
                                    range: individual_fetch_info.range,
                                    fetch_info: segment.clone(),
                                    chunk_cache: chunk_cache.clone(),
                                    client: term_download_client.clone(),
                                    range_download_single_flight: range_download_single_flight.clone(),
                                },
                                term,
                                skip_bytes,
                                take,
                            };

                            remaining_total_len -= take;
                            remaining_segment_len -= take;
                            debug!("enqueueing {download_task:?}");
                            task_tx.send(DownloadQueueItem::DownloadTask(download_task))?;
                        }

                        // enqueue the remainder of file info fetch task
                        if let Some(remainder) = maybe_remainder {
                            task_tx.send(DownloadQueueItem::Metadata(remainder))?;
                        } else {
                            task_tx.send(DownloadQueueItem::End)?;
                        }
                    },
                }
            }

            Ok(())
        });

        let mut writer = writer.get_writer_at(0)?;
        let mut total_written = 0;
        while let Some(result) = running_downloads_rx.recv().await {
            match result.await {
                Ok(Ok((mut download_result, permit))) => {
                    let data = take(&mut download_result.payload);
                    writer.write_all(&data)?;
                    // drop permit after data written out so they don't accumulate in memory unbounded
                    drop(permit);

                    if let Some(updater) = progress_updater.as_ref() {
                        updater.update(data.len() as u64).await;
                    }

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
    // at the beginning of the download, but queried in segments. Range downloads are executed with
    // a certain degree of parallelism, and so does writing out to storage. Ideal when the external
    // storage is fast at seeks, e.g. RAM or SSDs.
    #[instrument(skip_all, name = "RemoteClient::reconstruct_file_segmented_parallel", fields(file.hash = file_hash.hex()
    ))]
    async fn reconstruct_file_to_writer_segmented_parallel_write(
        &self,
        file_hash: &MerkleHash,
        byte_range: Option<FileRange>,
        writer: &OutputProvider,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64> {
        // queue size is inherently bounded by degree of concurrency.
        let (task_tx, mut task_rx) =
            mpsc::unbounded_channel::<DownloadQueueItem<FetchTermDownloadOnceAndWriteEverywhereUsed>>();
        let mut running_downloads = JoinSet::<Result<TermDownloadResult<u64>>>::new();

        // derive the actual range to reconstruct
        let file_reconstruct_range = byte_range.unwrap_or_else(FileRange::full);
        let base_write_negative_offset = file_reconstruct_range.start;

        // kick-start the download by enqueue the fetch info task.
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
        let term_download_client = self.http_client.clone();
        let download_scheduler = DownloadSegmentLengthTuner::from_configurable_constants();
        let concurrent_gets_semaphore = self.concurrent_gets_semaphore.clone();

        let process_result = move |result: TermDownloadResult<u64>,
                                   total_written: &mut u64,
                                   download_scheduler: &DownloadSegmentLengthTuner|
              -> Result<u64> {
            let write_len = result.payload;
            *total_written += write_len;

            // Now inspect the download metrics and tune the download degree of concurrency
            download_scheduler.tune_on(result)?;
            Ok(write_len)
        };

        let mut total_written = 0;
        while let Some(item) = task_rx.recv().await {
            // first try to join some tasks
            while let Some(result) = running_downloads.try_join_next() {
                let write_len = process_result(result??, &mut total_written, &download_scheduler)?;
                if let Some(updater) = progress_updater.as_ref() {
                    updater.update(write_len).await;
                }
            }

            match item {
                DownloadQueueItem::End => {
                    // everything processed
                    debug!("download queue emptied");
                    break;
                },
                DownloadQueueItem::DownloadTask(term_download) => {
                    // acquire the permit before spawning the task, so that there's limited
                    // number of active downloads.
                    let permit = concurrent_gets_semaphore.clone().acquire_owned().await?;
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
                    let tasks = map_fetch_info_into_download_tasks(
                        segment.clone(),
                        terms,
                        offset_into_first_range,
                        base_write_negative_offset,
                        self.chunk_cache.clone(),
                        term_download_client.clone(),
                        self.range_download_single_flight.clone(),
                        writer,
                    )
                    .await?;

                    debug!("enqueueing {} download tasks", tasks.len());
                    for task_def in tasks {
                        task_tx.send(DownloadQueueItem::DownloadTask(task_def))?;
                    }

                    // enqueue the remainder of file info fetch task
                    if let Some(remainder) = maybe_remainder {
                        task_tx.send(DownloadQueueItem::Metadata(remainder))?;
                    } else {
                        task_tx.send(DownloadQueueItem::End)?;
                    }
                },
            }
        }

        while let Some(result) = running_downloads.join_next().await {
            let write_len = process_result(result??, &mut total_written, &download_scheduler)?;
            if let Some(updater) = progress_updater.as_ref() {
                updater.update(write_len).await;
            }
        }

        Ok(total_written)
    }
}

#[allow(clippy::too_many_arguments)]
async fn map_fetch_info_into_download_tasks(
    segment: Arc<FetchInfo>,
    terms: Vec<CASReconstructionTerm>,
    offset_into_first_range: u64,
    base_write_negative_offset: u64,
    chunk_cache: Option<Arc<dyn ChunkCache>>,
    client: Arc<ClientWithMiddleware>,
    range_download_single_flight: Arc<Group<DownloadRangeResult, CasClientError>>,
    output_provider: &OutputProvider,
) -> Result<Vec<FetchTermDownloadOnceAndWriteEverywhereUsed>> {
    // the actual segment length.
    // the file_range end may actually exceed the file total length for the last segment.
    // in that case, the maximum length of this segment will be the total of all terms given
    //  minus the start offset
    let seg_len = segment
        .file_range
        .length()
        .min(terms.iter().fold(0, |acc, term| acc + term.unpacked_length as u64) - offset_into_first_range);

    let initial_writer_offset = segment.file_range.start - base_write_negative_offset;
    let mut total_taken = 0;

    let mut fetch_info_term_map: HashMap<(MerkleHash, ChunkRange), FetchTermDownloadOnceAndWriteEverywhereUsed> =
        HashMap::new();
    for (i, term) in terms.into_iter().enumerate() {
        let (individual_fetch_info, _) = segment.find((term.hash, term.range)).await?;

        let skip_bytes = if i == 0 { offset_into_first_range } else { 0 };
        // amount to take is min of the whole term after skipped bytes or the remainder of the segment
        let take = (term.unpacked_length as u64 - skip_bytes).min(seg_len - total_taken);
        let write_term = ChunkRangeWrite {
            // term details
            chunk_range: term.range,
            unpacked_length: term.unpacked_length,

            // write details
            skip_bytes,
            take,
            writer_offset: initial_writer_offset + total_taken,
        };

        let task = fetch_info_term_map
            .entry((term.hash.into(), individual_fetch_info.range))
            .or_insert_with(|| FetchTermDownloadOnceAndWriteEverywhereUsed {
                download: FetchTermDownload {
                    hash: term.hash.into(),
                    range: individual_fetch_info.range,
                    fetch_info: segment.clone(),
                    chunk_cache: chunk_cache.clone(),
                    client: client.clone(),
                    range_download_single_flight: range_download_single_flight.clone(),
                },
                writes: vec![],
                output: output_provider.clone(),
            });
        task.writes.push(write_term);

        total_taken += take;
    }

    let tasks = fetch_info_term_map.into_values().collect();

    Ok(tasks)
}

#[async_trait]
impl RegistrationClient for RemoteClient {
    #[instrument(skip_all, name = "RemoteClient::upload_shard", fields(shard.hash = hash.hex(), shard.len = shard_data.len()
    ))]
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
            .with_extension(Api("cas::upload_shard"))
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
    #[instrument(skip_all, name = "RemoteClient::get_file_reconstruction", fields(file.hash = file_hash.hex()
    ))]
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        let url = Url::parse(&format!("{}/reconstruction/{}", self.endpoint, file_hash.hex()))?;

        let response = self
            .authenticated_http_client
            .get(url)
            .with_extension(Api("cas::get_reconstruction_info"))
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
    #[instrument(skip_all, name = "RemoteClient::query_global_dedup")]
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
            .with_extension(Api("cas::query_dedup"))
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

    use anyhow::Result;
    use cas_object::test_utils::*;
    use cas_object::CompressionScheme;
    use cas_types::{CASReconstructionFetchInfo, CASReconstructionTerm, ChunkRange};
    use deduplication::constants::MAX_XORB_BYTES;
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use merkledb::constants::TARGET_CDC_CHUNK_SIZE;
    use tracing_test::traced_test;
    use xet_threadpool::ThreadPool;

    use super::*;
    use crate::interface::buffer::BufferProvider;

    #[ignore = "requires a running CAS server"]
    #[traced_test]
    #[test]
    fn test_basic_put() {
        // Arrange
        let prefix = PREFIX_DEFAULT;
        let raw_xorb = build_raw_xorb(3, ChunkSize::Random(512, 10248));

        let threadpool = ThreadPool::new().unwrap();
        let client = RemoteClient::new(CAS_ENDPOINT, &None, &None, "".into(), "", false);

        let cas_object = build_and_verify_cas_object(raw_xorb, Some(CompressionScheme::LZ4));

        // Act
        let result = threadpool
            .external_run_async_task(async move { client.upload_xorb(prefix, cas_object, None).await })
            .unwrap();

        // Assert
        assert!(result.is_ok());
    }

    #[derive(Clone)]
    struct TestCase {
        file_hash: MerkleHash,
        reconstruction_response: QueryReconstructionResponse,
        file_range: FileRange,
        expected_data: Vec<u8>,
        expect_error: bool,
    }

    const NUM_CHUNKS: u32 = 128;
    const CHUNK_SIZE: u32 = TARGET_CDC_CHUNK_SIZE as u32;

    macro_rules! mock_no_match_range_header {
        ($range_to_compare:expr) => {
            |req| {
                let Some(h) = &req.headers else {
                    return false;
                };
                let Some((_range_header, range_value)) =
                    h.iter().find(|(k, _v)| k.eq_ignore_ascii_case(RANGE.as_str()))
                else {
                    return false;
                };

                let Ok(range) = HttpRange::try_from(range_value.trim_start_matches("bytes=")) else {
                    return false;
                };

                range != $range_to_compare
            }
        };
    }

    #[test]
    fn test_reconstruct_file_full_file() -> Result<()> {
        // Arrange server
        let server = MockServer::start();

        let xorb_hash: MerkleHash = MerkleHash::default();
        let (cas_object, chunks_serialized, raw_data, _raw_data_chunk_hash_and_boundaries) =
            build_cas_object(NUM_CHUNKS, ChunkSize::Fixed(CHUNK_SIZE), CompressionScheme::ByteGrouping4LZ4);

        // Workaround to make this variable const. Change this accordingly if
        // real value of the two static variables below change.
        const FIRST_SEGMENT_SIZE: u64 = 16 * 64 * 1024 * 1024;
        assert_eq!(FIRST_SEGMENT_SIZE, *NUM_RANGE_IN_SEGMENT_BASE as u64 * *MAX_XORB_BYTES as u64);

        // Test case: full file reconstruction
        const FIRST_SEGMENT_FILE_RANGE: FileRange = FileRange {
            start: 0,
            end: FIRST_SEGMENT_SIZE,
            _marker: std::marker::PhantomData,
        };

        let test_case = TestCase {
            file_hash: MerkleHash::from_hex(&format!("{:0>64}", "1"))?, // "0....1"
            reconstruction_response: QueryReconstructionResponse {
                offset_into_first_range: 0,
                terms: vec![CASReconstructionTerm {
                    hash: xorb_hash.into(),
                    range: ChunkRange::new(0, NUM_CHUNKS),
                    unpacked_length: raw_data.len() as u32,
                }],
                fetch_info: HashMap::from([(
                    xorb_hash.into(),
                    vec![CASReconstructionFetchInfo {
                        range: ChunkRange::new(0, NUM_CHUNKS),
                        url: server.url(format!("/get_xorb/{xorb_hash}/")),
                        url_range: {
                            let (start, end) = cas_object.get_byte_offset(0, NUM_CHUNKS)?;
                            HttpRange::from(FileRange::new(start as u64, end as u64))
                        },
                    }],
                )]),
            },
            file_range: FileRange::full(),
            expected_data: raw_data,
            expect_error: false,
        };

        // Arrange server mocks
        let _mock_fi_416 = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/reconstruction/{}", test_case.file_hash))
                .matches(mock_no_match_range_header!(HttpRange::from(FIRST_SEGMENT_FILE_RANGE)));
            then.status(416);
        });
        let _mock_fi_200 = server.mock(|when, then| {
            let w = when.method(GET).path(format!("/reconstruction/{}", test_case.file_hash));
            w.header(RANGE.as_str(), HttpRange::from(FIRST_SEGMENT_FILE_RANGE).range_header());
            then.status(200).json_body_obj(&test_case.reconstruction_response);
        });
        for (k, v) in &test_case.reconstruction_response.fetch_info {
            for term in v {
                let data = FileRange::from(term.url_range);
                let data = chunks_serialized[data.start as usize..data.end as usize].to_vec();
                let _mock_data = server.mock(|when, then| {
                    when.method(GET)
                        .path(format!("/get_xorb/{k}/"))
                        .header(RANGE.as_str(), term.url_range.range_header());
                    then.status(200).body(&data);
                });
            }
        }

        test_reconstruct_file(test_case, &server.base_url())
    }

    #[test]
    fn test_reconstruct_file_skip_front_bytes() -> Result<()> {
        // Arrange server
        let server = MockServer::start();

        let xorb_hash: MerkleHash = MerkleHash::default();
        let (cas_object, chunks_serialized, raw_data, _raw_data_chunk_hash_and_boundaries) =
            build_cas_object(NUM_CHUNKS, ChunkSize::Fixed(CHUNK_SIZE), CompressionScheme::ByteGrouping4LZ4);

        // Workaround to make this variable const. Change this accordingly if
        // real value of the two static variables below change.
        const FIRST_SEGMENT_SIZE: u64 = 16 * 64 * 1024 * 1024;
        assert_eq!(FIRST_SEGMENT_SIZE, *NUM_RANGE_IN_SEGMENT_BASE as u64 * *MAX_XORB_BYTES as u64);

        // Test case: skip first 100 bytes
        const SKIP_BYTES: u64 = 100;
        const FIRST_SEGMENT_FILE_RANGE: FileRange = FileRange {
            start: SKIP_BYTES,
            end: SKIP_BYTES + FIRST_SEGMENT_SIZE,
            _marker: std::marker::PhantomData,
        };

        let test_case = TestCase {
            file_hash: MerkleHash::from_hex(&format!("{:0>64}", "1"))?, // "0....1"
            reconstruction_response: QueryReconstructionResponse {
                offset_into_first_range: SKIP_BYTES,
                terms: vec![CASReconstructionTerm {
                    hash: xorb_hash.into(),
                    range: ChunkRange::new(0, NUM_CHUNKS),
                    unpacked_length: raw_data.len() as u32,
                }],
                fetch_info: HashMap::from([(
                    xorb_hash.into(),
                    vec![CASReconstructionFetchInfo {
                        range: ChunkRange::new(0, NUM_CHUNKS),
                        url: server.url(format!("/get_xorb/{xorb_hash}/")),
                        url_range: {
                            let (start, end) = cas_object.get_byte_offset(0, NUM_CHUNKS)?;
                            HttpRange::from(FileRange::new(start as u64, end as u64))
                        },
                    }],
                )]),
            },
            file_range: FileRange::new(SKIP_BYTES, u64::MAX),
            expected_data: raw_data[SKIP_BYTES as usize..].to_vec(),
            expect_error: false,
        };

        // Arrange server mocks
        let _mock_fi_416 = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/reconstruction/{}", test_case.file_hash))
                .matches(mock_no_match_range_header!(HttpRange::from(FIRST_SEGMENT_FILE_RANGE)));
            then.status(416);
        });
        let _mock_fi_200 = server.mock(|when, then| {
            let w = when.method(GET).path(format!("/reconstruction/{}", test_case.file_hash));
            w.header(RANGE.as_str(), HttpRange::from(FIRST_SEGMENT_FILE_RANGE).range_header());
            then.status(200).json_body_obj(&test_case.reconstruction_response);
        });
        for (k, v) in &test_case.reconstruction_response.fetch_info {
            for term in v {
                let data = FileRange::from(term.url_range);
                let data = chunks_serialized[data.start as usize..data.end as usize].to_vec();
                let _mock_data = server.mock(|when, then| {
                    when.method(GET)
                        .path(format!("/get_xorb/{k}/"))
                        .header(RANGE.as_str(), term.url_range.range_header());
                    then.status(200).body(&data);
                });
            }
        }

        test_reconstruct_file(test_case, &server.base_url())
    }

    #[test]
    fn test_reconstruct_file_skip_back_bytes() -> Result<()> {
        // Arrange server
        let server = MockServer::start();

        let xorb_hash: MerkleHash = MerkleHash::default();
        let (cas_object, chunks_serialized, raw_data, _raw_data_chunk_hash_and_boundaries) =
            build_cas_object(NUM_CHUNKS, ChunkSize::Fixed(CHUNK_SIZE), CompressionScheme::ByteGrouping4LZ4);

        // Test case: skip last 100 bytes
        const FILE_SIZE: u64 = NUM_CHUNKS as u64 * CHUNK_SIZE as u64;
        const SKIP_BYTES: u64 = 100;
        const FIRST_SEGMENT_FILE_RANGE: FileRange = FileRange {
            start: 0,
            end: FILE_SIZE - SKIP_BYTES,
            _marker: std::marker::PhantomData,
        };

        let test_case = TestCase {
            file_hash: MerkleHash::from_hex(&format!("{:0>64}", "1"))?, // "0....1"
            reconstruction_response: QueryReconstructionResponse {
                offset_into_first_range: 0,
                terms: vec![CASReconstructionTerm {
                    hash: xorb_hash.into(),
                    range: ChunkRange::new(0, NUM_CHUNKS),
                    unpacked_length: raw_data.len() as u32,
                }],
                fetch_info: HashMap::from([(
                    xorb_hash.into(),
                    vec![CASReconstructionFetchInfo {
                        range: ChunkRange::new(0, NUM_CHUNKS),
                        url: server.url(format!("/get_xorb/{xorb_hash}/")),
                        url_range: {
                            let (start, end) = cas_object.get_byte_offset(0, NUM_CHUNKS)?;
                            HttpRange::from(FileRange::new(start as u64, end as u64))
                        },
                    }],
                )]),
            },
            file_range: FileRange::new(0, FILE_SIZE - SKIP_BYTES),
            expected_data: raw_data[..(FILE_SIZE - SKIP_BYTES) as usize].to_vec(),
            expect_error: false,
        };

        // Arrange server mocks
        let _mock_fi_416 = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/reconstruction/{}", test_case.file_hash))
                .matches(mock_no_match_range_header!(HttpRange::from(FIRST_SEGMENT_FILE_RANGE)));
            then.status(416);
        });
        let _mock_fi_200 = server.mock(|when, then| {
            let w = when.method(GET).path(format!("/reconstruction/{}", test_case.file_hash));
            w.header(RANGE.as_str(), HttpRange::from(FIRST_SEGMENT_FILE_RANGE).range_header());
            then.status(200).json_body_obj(&test_case.reconstruction_response);
        });
        for (k, v) in &test_case.reconstruction_response.fetch_info {
            for term in v {
                let data = FileRange::from(term.url_range);
                let data = chunks_serialized[data.start as usize..data.end as usize].to_vec();
                let _mock_data = server.mock(|when, then| {
                    when.method(GET)
                        .path(format!("/get_xorb/{k}/"))
                        .header(RANGE.as_str(), term.url_range.range_header());
                    then.status(200).body(&data);
                });
            }
        }

        test_reconstruct_file(test_case, &server.base_url())
    }

    #[test]
    fn test_reconstruct_file_two_terms() -> Result<()> {
        // Arrange server
        let server = MockServer::start();

        let xorb_hash_1: MerkleHash = MerkleHash::from_hex(&format!("{:0>64}", "1"))?; // "0....1"
        let xorb_hash_2: MerkleHash = MerkleHash::from_hex(&format!("{:0>64}", "2"))?; // "0....2"
        let (cas_object, chunks_serialized, raw_data, _raw_data_chunk_hash_and_boundaries) =
            build_cas_object(NUM_CHUNKS, ChunkSize::Fixed(CHUNK_SIZE), CompressionScheme::ByteGrouping4LZ4);

        // Test case: two terms and skip first and last 100 bytes
        const FILE_SIZE: u64 = (NUM_CHUNKS - 1) as u64 * CHUNK_SIZE as u64;
        const SKIP_BYTES: u64 = 100;
        const FIRST_SEGMENT_FILE_RANGE: FileRange = FileRange {
            start: SKIP_BYTES,
            end: FILE_SIZE - SKIP_BYTES,
            _marker: std::marker::PhantomData,
        };

        let test_case = TestCase {
            file_hash: MerkleHash::from_hex(&format!("{:0>64}", "1"))?, // "0....3"
            reconstruction_response: QueryReconstructionResponse {
                offset_into_first_range: SKIP_BYTES,
                terms: vec![
                    CASReconstructionTerm {
                        hash: xorb_hash_1.into(),
                        range: ChunkRange::new(0, 5),
                        unpacked_length: CHUNK_SIZE * 5,
                    },
                    CASReconstructionTerm {
                        hash: xorb_hash_2.into(),
                        range: ChunkRange::new(6, NUM_CHUNKS),
                        unpacked_length: CHUNK_SIZE * (NUM_CHUNKS - 6),
                    },
                ],
                fetch_info: HashMap::from([
                    (
                        // this constructs the first term
                        xorb_hash_1.into(),
                        vec![CASReconstructionFetchInfo {
                            range: ChunkRange::new(0, 7),
                            url: server.url(format!("/get_xorb/{xorb_hash_1}/")),
                            url_range: {
                                let (start, end) = cas_object.get_byte_offset(0, 7)?;
                                HttpRange::from(FileRange::new(start as u64, end as u64))
                            },
                        }],
                    ),
                    (
                        // this constructs the second term
                        xorb_hash_2.into(),
                        vec![CASReconstructionFetchInfo {
                            range: ChunkRange::new(4, NUM_CHUNKS),
                            url: server.url(format!("/get_xorb/{xorb_hash_2}/")),
                            url_range: {
                                let (start, end) = cas_object.get_byte_offset(4, NUM_CHUNKS)?;
                                HttpRange::from(FileRange::new(start as u64, end as u64))
                            },
                        }],
                    ),
                ]),
            },
            file_range: FileRange::new(SKIP_BYTES, FILE_SIZE - SKIP_BYTES),
            expected_data: [
                &raw_data[SKIP_BYTES as usize..(5 * CHUNK_SIZE) as usize],
                &raw_data[(6 * CHUNK_SIZE) as usize..(NUM_CHUNKS * CHUNK_SIZE) as usize - SKIP_BYTES as usize],
            ]
            .concat(),
            expect_error: false,
        };

        // Arrange server mocks
        let _mock_fi_416 = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/reconstruction/{}", test_case.file_hash))
                .matches(mock_no_match_range_header!(HttpRange::from(FIRST_SEGMENT_FILE_RANGE)));
            then.status(416);
        });
        let _mock_fi_200 = server.mock(|when, then| {
            let w = when.method(GET).path(format!("/reconstruction/{}", test_case.file_hash));
            w.header(RANGE.as_str(), HttpRange::from(FIRST_SEGMENT_FILE_RANGE).range_header());
            then.status(200).json_body_obj(&test_case.reconstruction_response);
        });
        for (k, v) in &test_case.reconstruction_response.fetch_info {
            for term in v {
                let data = FileRange::from(term.url_range);
                let data = chunks_serialized[data.start as usize..data.end as usize].to_vec();
                let _mock_data = server.mock(|when, then| {
                    when.method(GET)
                        .path(format!("/get_xorb/{k}/"))
                        .header(RANGE.as_str(), term.url_range.range_header());
                    then.status(200).body(&data);
                });
            }
        }

        test_reconstruct_file(test_case, &server.base_url())
    }

    fn test_reconstruct_file(test_case: TestCase, endpoint: &str) -> Result<()> {
        let threadpool = ThreadPool::new()?;

        // test reconstruct and sequential write
        let test = test_case.clone();
        let client = RemoteClient::new(endpoint, &None, &None, "".into(), "", false);
        let provider = BufferProvider::default();
        let buf = provider.buf.clone();
        let writer = OutputProvider::Buffer(provider);
        let resp = threadpool.external_run_async_task(async move {
            client
                .reconstruct_file_to_writer_segmented(&test.file_hash, Some(test.file_range), &writer, None)
                .await
        })?;

        assert_eq!(test.expect_error, resp.is_err());
        if !test.expect_error {
            assert_eq!(test.expected_data.len() as u64, resp.unwrap());
            assert_eq!(test.expected_data, buf.value());
        }

        // test reconstruct and parallel write
        let test = test_case;
        let client = RemoteClient::new(endpoint, &None, &None, "".into(), "", false);
        let provider = BufferProvider::default();
        let buf = provider.buf.clone();
        let writer = OutputProvider::Buffer(provider);
        let resp = threadpool.external_run_async_task(async move {
            client
                .reconstruct_file_to_writer_segmented_parallel_write(
                    &test.file_hash,
                    Some(test.file_range),
                    &writer,
                    None,
                )
                .await
        })?;

        assert_eq!(test.expect_error, resp.is_err());
        if !test.expect_error {
            assert_eq!(test.expected_data.len() as u64, resp.unwrap());
            let value = buf.value();
            assert_eq!(&test.expected_data[..100], &value[..100]);
            let idx = test.expected_data.len() - 100;
            assert_eq!(&test.expected_data[idx..], &value[idx..]);
            assert_eq!(test.expected_data, value);
        }

        Ok(())
    }
}
