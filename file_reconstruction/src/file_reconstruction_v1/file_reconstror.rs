use std::collections::HashMap;
use std::io::Write;
use std::mem::take;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::anyhow;
use cas_types::{
    BatchQueryReconstructionResponse, CASReconstructionFetchInfo, CASReconstructionTerm, ChunkRange, FileRange, Key,
    QueryReconstructionResponse,
};
use chunk_cache::{CacheConfig, ChunkCache};
use error_printer::ErrorPrinter;
use lazy_static::lazy_static;
use merklehash::MerkleHash;
use progress_tracking::item_tracking::SingleItemProgressUpdater;
use tokio::io::AsyncWriteExt;
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, info};
use utils::singleflight::Group;
use xet_runtime::{GlobalSemaphoreHandle, XetRuntime, global_semaphore_handle, xet_config};

use crate::Client;
use crate::error::{CasClientError, Result};
use crate::file_reconstruction::terms::{
    DownloadQueueItem, DownloadRangeResult, DownloadSegmentLengthTuner, FetchInfo, FetchTermDownload,
    FetchTermDownloadInfo, RangeDownloadSingleFlight, SequentialTermDownload, TermDownloadOutput, TermDownloadResult,
};
use crate::output_provider::{SeekingOutputProvider, SequentialOutput};

lazy_static! {
    static ref DOWNLOAD_CHUNK_RANGE_CONCURRENCY_LIMITER: GlobalSemaphoreHandle =
        global_semaphore_handle!(xet_config().client.num_concurrent_range_gets);
    static ref FN_CALL_ID: AtomicU64 = AtomicU64::new(1);
}

#[derive(Debug)]
pub struct ChunkRangeWrite {
    pub chunk_range: ChunkRange,
    pub unpacked_length: u32,
    pub skip_bytes: u64,
    pub take: u64,
    pub writer_offset: u64,
}

#[derive(Debug)]
pub struct FetchTermDownloadOnceAndWriteEverywhereUsed {
    pub download: FetchTermDownloadInfo,
    pub output: SeekingOutputProvider,
    pub writes: Vec<ChunkRangeWrite>,
}

pub struct FileReconstructor {
    pub(crate) client: Arc<dyn Client + Send + Sync>,
    #[allow(clippy::type_complexity)]
    chunk_cache: Option<Arc<dyn ChunkCache>>,
    range_download_single_flight: RangeDownloadSingleFlight,
}

impl std::fmt::Debug for FileReconstructor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileReconstructor")
            .field("chunk_cache", &self.chunk_cache.is_some())
            .finish_non_exhaustive()
    }
}

impl FileReconstructor {
    pub fn new(
        client: Arc<dyn Client + Send + Sync>,
        cache_config: Option<&CacheConfig>,
        range_download_single_flight: RangeDownloadSingleFlight,
    ) -> Arc<Self> {
        let chunk_cache = if let Some(cache_config) = cache_config {
            if cache_config.cache_size == 0 {
                info!("Chunk cache size set to 0, disabling chunk cache");
                None
            } else {
                info!(cache.dir=?cache_config.cache_directory, cache.size=cache_config.cache_size,"Using disk cache");
                chunk_cache::get_cache(cache_config)
                    .log_error("failed to initialize cache, not using cache")
                    .ok()
            }
        } else {
            None
        };

        Arc::new(Self {
            client,
            chunk_cache,
            range_download_single_flight,
        })
    }

    pub fn from_client(client: Arc<dyn Client + Send + Sync>, cache_config: Option<&CacheConfig>) -> Arc<Self> {
        let chunk_cache = if let Some(cache_config) = cache_config {
            if cache_config.cache_size == 0 {
                info!("Chunk cache size set to 0, disabling chunk cache");
                None
            } else {
                info!(cache.dir=?cache_config.cache_directory, cache.size=cache_config.cache_size,"Using disk cache");
                chunk_cache::get_cache(cache_config)
                    .log_error("failed to initialize cache, not using cache")
                    .ok()
            }
        } else {
            None
        };

        Arc::new(Self {
            client,
            chunk_cache,
            range_download_single_flight: Arc::new(Group::new()),
        })
    }

    #[cfg(not(target_family = "wasm"))]
    pub async fn get_reconstruction(
        self: &Arc<Self>,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>> {
        self.client.get_reconstruction(file_id, bytes_range).await
    }

    #[cfg(not(target_family = "wasm"))]
    pub async fn batch_get_reconstruction(
        self: &Arc<Self>,
        file_ids: &[MerkleHash],
    ) -> Result<BatchQueryReconstructionResponse> {
        self.client.batch_get_reconstruction(file_ids).await
    }

    #[cfg(not(target_family = "wasm"))]
    pub async fn get_one_fetch_term_data(
        self: &Arc<Self>,
        hash: MerkleHash,
        fetch_term: CASReconstructionFetchInfo,
    ) -> Result<TermDownloadOutput> {
        // Check cache first
        if let Some(cache) = &self.chunk_cache {
            let key = Key {
                prefix: "default".to_string(),
                hash,
            };
            if let Ok(Some(cached)) = cache.get(&key, &fetch_term.range).await.log_error("cache error") {
                info!(%hash, range=?fetch_term.range, "Cache hit");
                return Ok(cached.into());
            } else {
                info!(%hash, range=?fetch_term.range, "Cache miss");
            }
        }

        // Use single-flight to deduplicate concurrent requests
        let fetch_term_clone = fetch_term.clone();
        let client = self.client.clone();
        let download_range_result = self
            .range_download_single_flight
            .work_dump_caller_info(&fetch_term.url, async move {
                client
                    .download_fetch_term_data(hash, fetch_term_clone)
                    .await
                    .map(DownloadRangeResult::Data)
            })
            .await?;

        let DownloadRangeResult::Data(term_download_output) = download_range_result else {
            return Err(CasClientError::PresignedUrlExpirationError);
        };

        // Write to cache
        if let Some(cache) = &self.chunk_cache {
            let key = Key {
                prefix: "default".to_string(),
                hash,
            };
            if let Err(err) = cache
                .put(&key, &fetch_term.range, &term_download_output.chunk_byte_indices, &term_download_output.data)
                .await
            {
                info!(%hash, range=?fetch_term.range, ?err, "Writing to local cache failed, continuing");
            } else {
                info!(%hash, range=?fetch_term.range, "Cache write successful");
            }
        }

        Ok(term_download_output)
    }

    #[cfg(not(target_family = "wasm"))]
    pub async fn download_term_with_retry(
        self: &Arc<Self>,
        fetch_term_download: &FetchTermDownloadInfo,
    ) -> Result<TermDownloadResult<TermDownloadOutput>> {
        let instant = Instant::now();
        let mut n_retries_on_403 = 0;

        let key = (fetch_term_download.hash.into(), fetch_term_download.range);
        let data = loop {
            let (fetch_info, v) = fetch_term_download.fetch_info.find(key).await?;

            let range_data = self.get_one_fetch_term_data(fetch_term_download.hash, fetch_info.clone()).await;

            if let Err(CasClientError::PresignedUrlExpirationError) = range_data {
                fetch_term_download.fetch_info.refresh(v).await?;
                n_retries_on_403 += 1;
                continue;
            }

            break range_data?;
        };

        Ok(TermDownloadResult {
            payload: data,
            duration: instant.elapsed(),
            n_retries_on_403,
        })
    }

    #[cfg(not(target_family = "wasm"))]
    pub async fn process_sequential_term(
        self: &Arc<Self>,
        sequential_term: &SequentialTermDownload,
    ) -> Result<TermDownloadResult<Vec<u8>>> {
        let TermDownloadResult {
            payload:
                TermDownloadOutput {
                    data,
                    chunk_byte_indices,
                    chunk_range,
                },
            duration,
            n_retries_on_403,
        } = sequential_term.download.run(self).await?;

        let start_idx = (sequential_term.term.range.start - chunk_range.start) as usize;
        let end_idx = (sequential_term.term.range.end - chunk_range.start) as usize;

        let start_byte_index = chunk_byte_indices[start_idx] as usize;
        let end_byte_index = chunk_byte_indices[end_idx] as usize;
        debug_assert!(start_byte_index < data.len());
        debug_assert!(end_byte_index <= data.len());
        debug_assert!(start_byte_index < end_byte_index);
        let data_slice = &data[start_byte_index..end_byte_index];

        let start = sequential_term.skip_bytes as usize;
        let end = start + sequential_term.take as usize;
        let final_term_data = &data_slice[start..end];

        Ok(TermDownloadResult {
            payload: final_term_data.to_vec(),
            duration,
            n_retries_on_403,
        })
    }

    #[cfg(not(target_family = "wasm"))]
    pub async fn download_and_write_term(
        self: &Arc<Self>,
        task: FetchTermDownloadOnceAndWriteEverywhereUsed,
    ) -> Result<TermDownloadResult<u64>> {
        let download_result = self.download_term_with_retry(&task.download).await?;
        let TermDownloadOutput {
            data,
            chunk_byte_indices,
            chunk_range,
        } = download_result.payload;
        // Note: For LocalClient, chunk_byte_indices may have a different length due to how it's computed
        // This assertion is only valid for RemoteClient downloads
        #[cfg(not(test))]
        debug_assert_eq!(chunk_byte_indices.len(), (chunk_range.end - chunk_range.start + 1) as usize);
        if !chunk_byte_indices.is_empty() {
            debug_assert_eq!(*chunk_byte_indices.last().expect("checked len is something") as usize, data.len());
        }

        let mut total_written = 0;
        for write in task.writes {
            debug_assert!(write.chunk_range.start >= chunk_range.start);
            debug_assert!(write.chunk_range.end > write.chunk_range.start);
            debug_assert!(
                write.chunk_range.start < chunk_range.end,
                "{} < {} ;;; write {:?} term {:?}",
                write.chunk_range.start,
                chunk_range.end,
                write.chunk_range,
                chunk_range
            );
            debug_assert!(write.chunk_range.end <= chunk_range.end);

            let start_chunk_offset_index = write.chunk_range.start - chunk_range.start;
            let end_chunk_offset_index = write.chunk_range.end - chunk_range.start;
            let start_chunk_offset = chunk_byte_indices[start_chunk_offset_index as usize] as usize;
            let end_chunk_offset = chunk_byte_indices[end_chunk_offset_index as usize] as usize;
            let data_sub_range = &data[start_chunk_offset..end_chunk_offset];
            debug_assert_eq!(data_sub_range.len(), write.unpacked_length as usize);

            debug_assert!(data_sub_range.len() as u64 >= write.skip_bytes + write.take);
            let data_sub_range_sliced =
                &data_sub_range[(write.skip_bytes as usize)..((write.skip_bytes + write.take) as usize)];

            let mut writer = task.output.get_writer_at(write.writer_offset)?;
            writer.write_all(data_sub_range_sliced)?;
            writer.flush()?;
            total_written += write.take;
        }

        Ok(TermDownloadResult {
            payload: total_written,
            duration: download_result.duration,
            n_retries_on_403: download_result.n_retries_on_403,
        })
    }

    #[cfg(not(target_family = "wasm"))]
    pub async fn map_fetch_info_into_download_tasks(
        self: &Arc<Self>,
        segment: Arc<FetchInfo>,
        terms: Vec<CASReconstructionTerm>,
        offset_into_first_range: u64,
        base_write_negative_offset: u64,
        output_provider: &SeekingOutputProvider,
    ) -> Result<Vec<FetchTermDownloadOnceAndWriteEverywhereUsed>> {
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
            let take = (term.unpacked_length as u64 - skip_bytes).min(seg_len - total_taken);
            let write_term = ChunkRangeWrite {
                chunk_range: term.range,
                unpacked_length: term.unpacked_length,
                skip_bytes,
                take,
                writer_offset: initial_writer_offset + total_taken,
            };

            let task = fetch_info_term_map
                .entry((term.hash.into(), individual_fetch_info.range))
                .or_insert_with(|| FetchTermDownloadOnceAndWriteEverywhereUsed {
                    download: FetchTermDownloadInfo {
                        hash: term.hash.into(),
                        range: individual_fetch_info.range,
                        fetch_info: segment.clone(),
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

    pub async fn get_file_with_sequential_writer(
        self: &Arc<Self>,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        mut writer: SequentialOutput,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64> {
        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        info!(
            call_id,
            %hash,
            ?byte_range,
            "Starting reconstruct_file_to_writer_segmented",
        );

        let (task_tx, mut task_rx) = mpsc::unbounded_channel::<DownloadQueueItem<SequentialTermDownload>>();
        let (running_downloads_tx, mut running_downloads_rx) =
            mpsc::unbounded_channel::<JoinHandle<Result<(TermDownloadResult<Vec<u8>>, OwnedSemaphorePermit)>>>();

        let file_reconstruct_range = byte_range.unwrap_or_else(FileRange::full);
        let total_len = file_reconstruct_range.length();

        task_tx.send(DownloadQueueItem::Metadata(FetchInfo::new(*hash, file_reconstruct_range, self.clone())))?;

        let reconstruction_client = self.clone();
        let download_scheduler = DownloadSegmentLengthTuner::from_configurable_constants();
        let download_scheduler_clone = download_scheduler.clone();

        let download_concurrency_limiter =
            XetRuntime::current().global_semaphore(*DOWNLOAD_CHUNK_RANGE_CONCURRENCY_LIMITER);

        info!(concurrency_limit = xet_config().client.num_concurrent_range_gets, "Starting segmented download");

        let queue_dispatcher: JoinHandle<Result<()>> = tokio::spawn(async move {
            let mut remaining_total_len = total_len;
            while let Some(item) = task_rx.recv().await {
                match item {
                    DownloadQueueItem::End => {
                        debug!(call_id, "download queue emptied");
                        drop(running_downloads_tx);
                        break;
                    },
                    DownloadQueueItem::DownloadTask(term_download) => {
                        let permit = download_concurrency_limiter.clone().acquire_owned().await?;
                        debug!(call_id, "spawning 1 download task");
                        let reconstruction_client = reconstruction_client.clone();
                        let future: JoinHandle<Result<(TermDownloadResult<Vec<u8>>, OwnedSemaphorePermit)>> =
                            tokio::spawn(async move {
                                let data = reconstruction_client.process_sequential_term(&term_download).await?;
                                Ok((data, permit))
                            });
                        running_downloads_tx.send(future)?;
                    },
                    DownloadQueueItem::Metadata(fetch_info) => {
                        let segment_size = download_scheduler_clone.next_segment_size()?;
                        debug!(call_id, segment_size, "querying file info");
                        let (segment, maybe_remainder) = fetch_info.take_segment(segment_size);

                        let Some((offset_into_first_range, terms)) = segment.query().await? else {
                            task_tx.send(DownloadQueueItem::End)?;
                            continue;
                        };

                        let segment = Arc::new(segment);
                        let mut remaining_segment_len = segment_size;
                        debug!(call_id, num_tasks = terms.len(), "enqueueing download tasks");
                        let mut download_task_map = HashMap::new();
                        for (i, term) in terms.into_iter().enumerate() {
                            let skip_bytes = if i == 0 { offset_into_first_range } else { 0 };
                            let take = remaining_total_len
                                .min(remaining_segment_len)
                                .min(term.unpacked_length as u64 - skip_bytes);
                            let (individual_fetch_info, _) = segment.find((term.hash, term.range)).await?;
                            let download = download_task_map
                                .entry((term.hash, individual_fetch_info.range))
                                .or_insert_with(|| {
                                    Arc::new(FetchTermDownload::new(FetchTermDownloadInfo {
                                        hash: term.hash.into(),
                                        range: individual_fetch_info.range,
                                        fetch_info: segment.clone(),
                                    }))
                                })
                                .clone();

                            let download_task = SequentialTermDownload {
                                download,
                                term,
                                skip_bytes,
                                take,
                            };

                            remaining_total_len -= take;
                            remaining_segment_len -= take;
                            debug!(call_id, ?download_task, "enqueueing task");
                            task_tx.send(DownloadQueueItem::DownloadTask(download_task))?;
                        }

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

        let mut total_written = 0;
        while let Some(result) = running_downloads_rx.recv().await {
            match result.await {
                Ok(Ok((mut download_result, permit))) => {
                    let data = take(&mut download_result.payload);
                    writer.write_all(&data).await?;
                    drop(permit);

                    if let Some(updater) = progress_updater.as_ref() {
                        updater.update(data.len() as u64).await;
                    }

                    total_written += data.len() as u64;

                    download_scheduler.tune_on(download_result)?;
                },
                Ok(Err(e)) => Err(e)?,
                Err(e) => Err(anyhow!("{e:?}"))?,
            }
        }
        writer.flush().await?;

        queue_dispatcher.await??;

        info!(
            call_id,
            %hash,
            ?byte_range,
            "Completed reconstruct_file_to_writer_segmented"
        );

        Ok(total_written)
    }

    pub async fn get_file_with_parallel_writer(
        self: &Arc<Self>,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        writer: &SeekingOutputProvider,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64> {
        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        info!(
            call_id,
            %hash,
            ?byte_range,
            "Starting reconstruct_file_to_writer_segmented_parallel_write"
        );

        let (task_tx, mut task_rx) =
            mpsc::unbounded_channel::<DownloadQueueItem<FetchTermDownloadOnceAndWriteEverywhereUsed>>();
        let mut running_downloads = JoinSet::<Result<TermDownloadResult<u64>>>::new();

        let file_reconstruct_range = byte_range.unwrap_or_else(FileRange::full);
        let base_write_negative_offset = file_reconstruct_range.start;

        task_tx.send(DownloadQueueItem::Metadata(FetchInfo::new(*hash, file_reconstruct_range, self.clone())))?;

        let download_scheduler = DownloadSegmentLengthTuner::from_configurable_constants();

        let download_concurrency_limiter =
            XetRuntime::current().global_semaphore(*DOWNLOAD_CHUNK_RANGE_CONCURRENCY_LIMITER);

        let process_result = move |result: TermDownloadResult<u64>,
                                   total_written: &mut u64,
                                   download_scheduler: &DownloadSegmentLengthTuner|
              -> Result<u64> {
            let write_len = result.payload;
            *total_written += write_len;

            download_scheduler.tune_on(result)?;
            Ok(write_len)
        };

        let mut total_written = 0;
        while let Some(item) = task_rx.recv().await {
            while let Some(result) = running_downloads.try_join_next() {
                let write_len = process_result(result??, &mut total_written, &download_scheduler)?;
                if let Some(updater) = progress_updater.as_ref() {
                    updater.update(write_len).await;
                }
            }

            match item {
                DownloadQueueItem::End => {
                    debug!(call_id, "download queue emptied");
                    break;
                },
                DownloadQueueItem::DownloadTask(term_download) => {
                    let permit = download_concurrency_limiter.clone().acquire_owned().await?;
                    debug!(call_id, "spawning 1 download task");
                    let reconstruction_client = self.clone();
                    running_downloads.spawn(async move {
                        let data = reconstruction_client.download_and_write_term(term_download).await?;
                        drop(permit);
                        Ok(data)
                    });
                },
                DownloadQueueItem::Metadata(fetch_info) => {
                    let segment_size = download_scheduler.next_segment_size()?;
                    debug!(call_id, segment_size, "querying file info");
                    let (segment, maybe_remainder) = fetch_info.take_segment(segment_size);

                    let Some((offset_into_first_range, terms)) = segment.query().await? else {
                        task_tx.send(DownloadQueueItem::End)?;
                        continue;
                    };

                    let segment = Arc::new(segment);

                    let tasks = self
                        .map_fetch_info_into_download_tasks(
                            segment.clone(),
                            terms,
                            offset_into_first_range,
                            base_write_negative_offset,
                            writer,
                        )
                        .await?;

                    debug!(call_id, num_tasks = tasks.len(), "enqueueing download tasks");
                    for task_def in tasks {
                        task_tx.send(DownloadQueueItem::DownloadTask(task_def))?;
                    }

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

        info!(
            call_id,
            %hash,
            ?byte_range,
            "Completed reconstruct_file_to_writer_segmented_parallel_write"
        );

        Ok(total_written)
    }
}
