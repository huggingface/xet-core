//! The FileReconstructorV1 struct and its implementation.
//!
//! This module provides the main file reconstruction functionality, wrapping
//! an underlying client that provides the basic reconstruction data operations.

use std::collections::HashMap;
use std::mem::take;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::anyhow;
use cas_types::{CASReconstructionTerm, ChunkRange, FileRange};
use chunk_cache::{CacheConfig, ChunkCache};
use error_printer::ErrorPrinter;
use lazy_static::lazy_static;
use merklehash::MerkleHash;
use progress_tracking::item_tracking::SingleItemProgressUpdater;
use tokio::io::AsyncWriteExt;
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, event, instrument};
use utils::singleflight::Group;
use xet_runtime::{GlobalSemaphoreHandle, XetRuntime, global_semaphore_handle, xet_config};

use super::download_utils::{
    ChunkRangeWrite, DownloadQueueItem, DownloadSegmentLengthTuner, FetchInfo, FetchTermDownload,
    FetchTermDownloadInner, FetchTermDownloadOnceAndWriteEverywhereUsed, RangeDownloadSingleFlight,
    SequentialTermDownload, TermDownloadResult,
};
use super::output_provider::{SeekingOutputProvider, SequentialOutput};
use crate::error::Result;
use crate::{Client, INFORMATION_LOG_LEVEL};

lazy_static! {
    static ref DOWNLOAD_CHUNK_RANGE_CONCURRENCY_LIMITER: GlobalSemaphoreHandle =
        global_semaphore_handle!(xet_config().client.num_concurrent_range_gets);
    static ref FN_CALL_ID: AtomicU64 = AtomicU64::new(1);
}

/// FileReconstructorV1 provides file reconstruction functionality.
///
/// This struct wraps a client that implements the `Client` trait and provides
/// file reconstruction with optional caching.
///
/// Main methods:
/// - `get_file_with_sequential_writer`: Reconstructs a file with sequential writing
/// - `get_file_with_parallel_writer`: Reconstructs a file with parallel writing
pub struct FileReconstructorV1 {
    client: Arc<dyn Client>,
    chunk_cache: Option<Arc<dyn ChunkCache>>,
    range_download_single_flight: RangeDownloadSingleFlight,
}

impl FileReconstructorV1 {
    /// Create a new FileReconstructorV1 wrapping the given client.
    ///
    /// If `cache_config` is provided and valid, downloaded chunks will be cached
    /// to avoid redundant downloads.
    pub fn new(client: Arc<dyn Client>, cache_config: &Option<CacheConfig>) -> Self {
        let chunk_cache = if let Some(cache_config) = cache_config {
            if cache_config.cache_size == 0 {
                event!(INFORMATION_LOG_LEVEL, "Chunk cache size set to 0, disabling chunk cache");
                None
            } else {
                event!(INFORMATION_LOG_LEVEL, cache.dir=?cache_config.cache_directory, cache.size=cache_config.cache_size, "Using disk cache");
                chunk_cache::get_cache(cache_config)
                    .log_error("failed to initialize cache, not using cache")
                    .ok()
            }
        } else {
            None
        };

        Self {
            client,
            chunk_cache,
            range_download_single_flight: Arc::new(Group::new()),
        }
    }

    /// Get the underlying client.
    pub fn client(&self) -> &Arc<dyn Client> {
        &self.client
    }

    /// Get the chunk cache, if configured.
    pub fn chunk_cache(&self) -> Option<Arc<dyn ChunkCache>> {
        self.chunk_cache.clone()
    }

    /// Reconstruct a file using sequential writing.
    ///
    /// This method downloads the file in segments, writing each segment sequentially
    /// to the provided output. This is ideal when the external storage uses HDDs
    /// where sequential writes are more efficient.
    #[instrument(skip_all, name = "FileReconstructorV1::get_file_with_sequential_writer", fields(file.hash = file_hash.hex()))]
    pub async fn get_file_with_sequential_writer(
        &self,
        file_hash: &MerkleHash,
        byte_range: Option<FileRange>,
        mut writer: SequentialOutput,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64> {
        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            %file_hash,
            ?byte_range,
            "Starting get_file_with_sequential_writer",
        );

        let data_provider = self.client.clone();
        let chunk_cache = self.chunk_cache.clone();
        let range_download_single_flight = self.range_download_single_flight.clone();

        // Use an unlimited queue size, as queue size is inherently bounded by degree of concurrency.
        let (task_tx, mut task_rx) = mpsc::unbounded_channel::<DownloadQueueItem<SequentialTermDownload>>();
        let (running_downloads_tx, mut running_downloads_rx) =
            mpsc::unbounded_channel::<JoinHandle<Result<(TermDownloadResult<Vec<u8>>, OwnedSemaphorePermit)>>>();

        // derive the actual range to reconstruct
        let file_reconstruct_range = byte_range.unwrap_or_else(FileRange::full);
        let total_len = file_reconstruct_range.length();

        // kick-start the download by enqueue the fetch info task.
        task_tx.send(DownloadQueueItem::Metadata(FetchInfo::new(*file_hash, file_reconstruct_range)))?;

        // Start the queue processing logic
        let download_scheduler = DownloadSegmentLengthTuner::from_configurable_constants();
        let download_scheduler_clone = download_scheduler.clone();

        let download_concurrency_limiter =
            XetRuntime::current().global_semaphore(*DOWNLOAD_CHUNK_RANGE_CONCURRENCY_LIMITER);

        event!(
            INFORMATION_LOG_LEVEL,
            concurrency_limit = xet_config().client.num_concurrent_range_gets,
            "Starting segmented download"
        );

        let client_for_dispatch = data_provider.clone();
        let chunk_cache_for_dispatch = chunk_cache.clone();
        let range_download_single_flight_for_dispatch = range_download_single_flight.clone();
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
                        let client = client_for_dispatch.clone();
                        let future: JoinHandle<Result<(TermDownloadResult<Vec<u8>>, OwnedSemaphorePermit)>> =
                            tokio::spawn(async move {
                                let data = term_download.run(client).await?;
                                Ok((data, permit))
                            });
                        running_downloads_tx.send(future)?;
                    },
                    DownloadQueueItem::Metadata(fetch_info) => {
                        let segment_size = download_scheduler_clone.next_segment_size()?;
                        debug!(call_id, segment_size, "querying file info");
                        let (segment, maybe_remainder) = fetch_info.take_segment(segment_size);

                        let Some((offset_into_first_range, terms)) = segment.query(&client_for_dispatch).await? else {
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
                                    Arc::new(FetchTermDownload::new(FetchTermDownloadInner {
                                        hash: term.hash.into(),
                                        range: individual_fetch_info.range,
                                        fetch_info: segment.clone(),
                                        chunk_cache: chunk_cache_for_dispatch.clone(),
                                        range_download_single_flight: range_download_single_flight_for_dispatch.clone(),
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

        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            %file_hash,
            ?byte_range,
            "Completed get_file_with_sequential_writer"
        );

        Ok(total_written)
    }

    /// Reconstruct a file using parallel writing.
    ///
    /// This method downloads the file in segments, writing segments in parallel
    /// to the provided output. This is ideal when the external storage is fast
    /// at seeks, e.g. RAM or SSDs.
    #[instrument(skip_all, name = "FileReconstructorV1::get_file_with_parallel_writer", fields(file.hash = file_hash.hex()))]
    pub async fn get_file_with_parallel_writer(
        &self,
        file_hash: &MerkleHash,
        byte_range: Option<FileRange>,
        writer: &SeekingOutputProvider,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64> {
        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            %file_hash,
            ?byte_range,
            "Starting get_file_with_parallel_writer"
        );

        let data_provider = self.client.clone();
        let chunk_cache = self.chunk_cache.clone();
        let range_download_single_flight = self.range_download_single_flight.clone();

        // Use the unlimited queue, as queue size is inherently bounded by degree of concurrency.
        let (task_tx, mut task_rx) =
            mpsc::unbounded_channel::<DownloadQueueItem<FetchTermDownloadOnceAndWriteEverywhereUsed>>();
        let mut running_downloads = JoinSet::<Result<TermDownloadResult<u64>>>::new();

        // derive the actual range to reconstruct
        let file_reconstruct_range = byte_range.unwrap_or_else(FileRange::full);
        let base_write_negative_offset = file_reconstruct_range.start;

        // kick-start the download by enqueue the fetch info task.
        task_tx.send(DownloadQueueItem::Metadata(FetchInfo::new(*file_hash, file_reconstruct_range)))?;

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
        let client_for_downloads = data_provider.clone();
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
                    debug!(call_id, "download queue emptied");
                    break;
                },
                DownloadQueueItem::DownloadTask(term_download) => {
                    let permit = download_concurrency_limiter.clone().acquire_owned().await?;
                    debug!(call_id, "spawning 1 download task");
                    let client = client_for_downloads.clone();
                    running_downloads.spawn(async move {
                        let data = term_download.run(client).await?;
                        drop(permit);
                        Ok(data)
                    });
                },
                DownloadQueueItem::Metadata(fetch_info) => {
                    let segment_size = download_scheduler.next_segment_size()?;
                    debug!(call_id, segment_size, "querying file info");
                    let (segment, maybe_remainder) = fetch_info.take_segment(segment_size);

                    let Some((offset_into_first_range, terms)) = segment.query(&client_for_downloads).await? else {
                        task_tx.send(DownloadQueueItem::End)?;
                        continue;
                    };

                    let segment = Arc::new(segment);

                    let tasks = map_fetch_info_into_download_tasks(
                        segment.clone(),
                        terms,
                        offset_into_first_range,
                        base_write_negative_offset,
                        writer,
                        chunk_cache.clone(),
                        range_download_single_flight.clone(),
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

        // For empty files, ensure the output file is created even if no data was written
        if total_written == 0 {
            let mut empty_writer = writer.get_writer_at(0)?;
            empty_writer.flush()?;
        }

        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            %file_hash,
            ?byte_range,
            "Completed get_file_with_parallel_writer"
        );

        Ok(total_written)
    }
}

/// Helper function to map fetch info into download tasks for parallel writing.
async fn map_fetch_info_into_download_tasks(
    segment: Arc<FetchInfo>,
    terms: Vec<CASReconstructionTerm>,
    offset_into_first_range: u64,
    base_write_negative_offset: u64,
    output_provider: &SeekingOutputProvider,
    chunk_cache: Option<Arc<dyn ChunkCache>>,
    range_download_single_flight: RangeDownloadSingleFlight,
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
                download: FetchTermDownloadInner {
                    hash: term.hash.into(),
                    range: individual_fetch_info.range,
                    fetch_info: segment.clone(),
                    chunk_cache: chunk_cache.clone(),
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
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::Result;
    use cas_object::CompressionScheme;
    use cas_object::test_utils::*;
    use cas_types::{
        CASReconstructionFetchInfo, CASReconstructionTerm, ChunkRange, HttpRange, QueryReconstructionResponse,
    };
    use http::header::RANGE;
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use xet_runtime::{XetRuntime, xet_config};

    use super::*;
    use crate::RemoteClient;
    use crate::file_reconstruction_v1::buffer_provider::ThreadSafeBuffer;

    #[derive(Clone)]
    struct TestCase {
        file_hash: MerkleHash,
        reconstruction_response: QueryReconstructionResponse,
        file_range: FileRange,
        expected_data: Vec<u8>,
        expect_error: bool,
    }

    const NUM_CHUNKS: u32 = 128;

    const CHUNK_SIZE: u32 = 64 * 1024;

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
        assert_eq!(
            FIRST_SEGMENT_SIZE,
            xet_config().client.num_range_in_segment_base as u64 * *deduplication::constants::MAX_XORB_BYTES as u64
        );

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
                .path(format!("/v1/reconstructions/{}", test_case.file_hash))
                .matches(mock_no_match_range_header!(HttpRange::from(FIRST_SEGMENT_FILE_RANGE)));
            then.status(416);
        });
        let _mock_fi_200 = server.mock(|when, then| {
            let w = when.method(GET).path(format!("/v1/reconstructions/{}", test_case.file_hash));
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
        assert_eq!(
            FIRST_SEGMENT_SIZE,
            xet_config().client.num_range_in_segment_base as u64 * *deduplication::constants::MAX_XORB_BYTES as u64
        );

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
                .path(format!("/v1/reconstructions/{}", test_case.file_hash))
                .matches(mock_no_match_range_header!(HttpRange::from(FIRST_SEGMENT_FILE_RANGE)));
            then.status(416);
        });
        let _mock_fi_200 = server.mock(|when, then| {
            let w = when.method(GET).path(format!("/v1/reconstructions/{}", test_case.file_hash));
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
                .path(format!("/v1/reconstructions/{}", test_case.file_hash))
                .matches(mock_no_match_range_header!(HttpRange::from(FIRST_SEGMENT_FILE_RANGE)));
            then.status(416);
        });
        let _mock_fi_200 = server.mock(|when, then| {
            let w = when.method(GET).path(format!("/v1/reconstructions/{}", test_case.file_hash));
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
                .path(format!("/v1/reconstructions/{}", test_case.file_hash))
                .matches(mock_no_match_range_header!(HttpRange::from(FIRST_SEGMENT_FILE_RANGE)));
            then.status(416);
        });
        let _mock_fi_200 = server.mock(|when, then| {
            let w = when.method(GET).path(format!("/v1/reconstructions/{}", test_case.file_hash));
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
        let threadpool = XetRuntime::new()?;

        // test reconstruct and sequential write
        let test = test_case.clone();
        let client = RemoteClient::new(endpoint, &None, "", false, "");
        let reconstructor = FileReconstructorV1::new(client, &None);
        let buf = ThreadSafeBuffer::default();
        let provider = SequentialOutput::from(buf.clone());
        let resp = threadpool.external_run_async_task(async move {
            reconstructor
                .get_file_with_sequential_writer(&test.file_hash, Some(test.file_range), provider, None)
                .await
        })?;

        assert_eq!(test.expect_error, resp.is_err(), "{:?}", resp.err());
        if !test.expect_error {
            assert_eq!(test.expected_data.len() as u64, resp?);
            assert_eq!(test.expected_data, buf.value());
        }

        // test reconstruct and parallel write
        let test = test_case;
        let client = RemoteClient::new(endpoint, &None, "", false, "");
        let reconstructor = FileReconstructorV1::new(client, &None);
        let buf = ThreadSafeBuffer::default();
        let provider = SeekingOutputProvider::from(buf.clone());
        let resp = threadpool.external_run_async_task(async move {
            reconstructor
                .get_file_with_parallel_writer(&test.file_hash, Some(test.file_range), &provider, None)
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
