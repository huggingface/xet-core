use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use cas_object::error::CasObjectError;
use cas_types::{CASReconstructionFetchInfo, CASReconstructionTerm, ChunkRange, FileRange, HexMerkleHash, Key};
use chunk_cache::{CacheRange, ChunkCache};
use deduplication::constants::MAX_XORB_BYTES;
use derivative::Derivative;
use error_printer::ErrorPrinter;
use futures::TryStreamExt;
use http::header::RANGE;
use http::StatusCode;
use merklehash::MerkleHash;
use reqwest_middleware::ClientWithMiddleware;
use tokio_retry::strategy::ExponentialBackoff;
use tracing::{debug, error, info, trace, warn};
use url::Url;
use utils::singleflight::Group;

use crate::error::{CasClientError, Result};
use crate::http_client::{Api, BASE_RETRY_DELAY_MS, BASE_RETRY_MAX_DURATION_MS, NUM_RETRIES};
use crate::remote_client::{get_reconstruction_with_endpoint_and_client, PREFIX_DEFAULT};
use crate::OutputProvider;

utils::configurable_constants! {
    // Env (HF_XET_NUM_RANGE_IN_SEGMENT_BASE) base value for the approx number of ranges in the initial
    // segment size used to download, where a segment is a range of a file that is downloaded
    // setting this value to 0 causes no segments to be downloaded, this will cause downloads to fail/hang
    ref NUM_RANGE_IN_SEGMENT_BASE: usize = GlobalConfigMode::HighPerformanceOption {
        standard: 16, // 16 * ~64MB -> ~1GB initial segment size
        high_performance: 128, // 128 * ~64MB -> ~8GB initial segment size
    };

    // Env (HF_XET_NUM_RANGE_IN_SEGMENT_DELTA) delta value for the approx number of ranges in a segment size
    // used to increase/decrease the segment size by this many approximate ranges
    // setting this value to 0 causes no segment size change, i.e. will remain constant
    ref NUM_RANGE_IN_SEGMENT_DELTA: usize = 1; // increase/decrease segment size by 1 approx range ~64MB

    // Env (HF_XET_NUM_RANGE_IN_SEGMENT_MAX) max value for the approx number of ranges in a segment size
    // setting this value to 0 will be ignored and the max size will be set to usize::MAX
    ref NUM_RANGE_IN_SEGMENT_MAX: usize = 400; // * ~64MB -> max at 25GB segment
}

#[derive(Clone, Debug)]
pub(crate) enum DownloadRangeResult {
    Data(TermDownloadOutput),
    // This is a workaround to propagate the underlying request error as
    // the underlying reqwest_middleware::Error doesn't impl Clone.
    // Otherwise, if two download tasks with the same key in the single flight
    // group failed, the caller will get the origin error but the waiter will
    // get a `WaiterInternalError` wrapping a copy of the error message ("{e:?}"),
    // making it impossible to be examined programmatically.
    Forbidden,
}
pub(crate) type RangeDownloadSingleFlight = Arc<Group<DownloadRangeResult, CasClientError>>;

#[derive(Debug)]
pub(crate) struct FetchInfo {
    file_hash: MerkleHash,
    pub(crate) file_range: FileRange,
    endpoint: String,
    client: Arc<ClientWithMiddleware>, // only used for fetching file info
    inner: RwLock<HashMap<HexMerkleHash, Vec<CASReconstructionFetchInfo>>>,
    version: tokio::sync::Mutex<u32>,
}

impl FetchInfo {
    pub fn new(
        file_hash: MerkleHash,
        file_range: FileRange,
        endpoint: String,
        client: Arc<ClientWithMiddleware>,
    ) -> Self {
        Self {
            file_hash,
            file_range,
            endpoint,
            client,
            inner: Default::default(),
            version: tokio::sync::Mutex::new(0),
        }
    }

    // consumes self and split the file range into a segment of size `segment_size`
    // and a remainder.
    pub fn take_segment(self, segment_size: u64) -> (Self, Option<Self>) {
        let (first_segment, remainder) = self.file_range.take_segment(segment_size);

        (
            FetchInfo::new(self.file_hash, first_segment, self.endpoint.clone(), self.client.clone()),
            remainder.map(|r| FetchInfo::new(self.file_hash, r, self.endpoint, self.client)),
        )
    }

    pub async fn find(&self, key: (HexMerkleHash, ChunkRange)) -> Result<(CASReconstructionFetchInfo, u32)> {
        let v = *self.version.lock().await;

        let (hash, range) = key;
        let map = self.inner.read()?;
        let hash_fetch_info = map
            .get(&hash)
            .ok_or(CasClientError::InvalidArguments)
            .log_error("invalid response from CAS server: failed to get term hash in fetch info")?;
        let fetch_term = hash_fetch_info
            .iter()
            .find(|fterm| fterm.range.start <= range.start && fterm.range.end >= range.end)
            .ok_or(CasClientError::InvalidArguments)
            .log_error("invalid response from CAS server: failed to match hash in fetch_info")?
            .clone();

        Ok((fetch_term, v))
    }

    pub async fn query(&self) -> Result<Option<(u64, Vec<CASReconstructionTerm>)>> {
        let Some(manifest) = get_reconstruction_with_endpoint_and_client(
            &self.endpoint,
            &self.client,
            &self.file_hash,
            Some(self.file_range),
        )
        .await?
        else {
            return Ok(None);
        };

        *self.inner.write()? = manifest.fetch_info;

        Ok(Some((manifest.offset_into_first_range, manifest.terms)))
    }

    pub async fn refresh(&self, vhint: u32) -> Result<()> {
        // Our term download tasks run in concurrent, so at this point
        // it's possible that
        // 1. some other TermDownload is also calling refersh();
        // 2. some other TermDownload called refresh and the fetch info is already new, but the term calling into this
        // refresh() didn't see the update yet.

        // Mutex on `version` ensures only one refresh happens at a time.
        let mut v = self.version.lock().await;
        // Version check ensures we don't refresh again if some other
        // TermDownload already refreshed recently.
        if *v > vhint {
            // Already refreshed.
            return Ok(());
        }

        self.query().await?;

        *v += 1;

        Ok(())
    }
}

/// Helper object containing the structs needed when downloading a term in parallel
/// during reconstruction.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct FetchTermDownload {
    pub hash: MerkleHash,
    pub range: ChunkRange,
    #[derivative(Debug = "ignore")]
    pub fetch_info: Arc<FetchInfo>, // utility to get URL to download this term
    #[derivative(Debug = "ignore")]
    pub chunk_cache: Option<Arc<dyn ChunkCache>>,
    #[derivative(Debug = "ignore")]
    pub client: Arc<ClientWithMiddleware>, // only used for downloading range
    #[derivative(Debug = "ignore")]
    pub range_download_single_flight: RangeDownloadSingleFlight,
}

#[derive(Debug)]
pub(crate) struct SequentialTermDownload {
    pub term: CASReconstructionTerm,
    pub download: FetchTermDownload,
    pub skip_bytes: u64, // number of bytes to skip at the front
    pub take: u64,       /* number of bytes to take after skipping bytes,
                          * effectively taking [skip_bytes..skip_bytes+take]
                          * out of the downloaded range */
}

impl SequentialTermDownload {
    pub async fn run(self) -> Result<TermDownloadResult<Vec<u8>>> {
        let TermDownloadResult {
            payload:
                TermDownloadOutput {
                    data,
                    chunk_byte_indices,
                    chunk_range,
                },
            duration,
            n_retries_on_403,
        } = self.download.run().await?;

        // if the requested range is smaller than the fetched range, trim it down to the right data
        // the requested range cannot be larger than the fetched range.
        // "else" case data matches exact, save some work, return whole data.
        let start_idx = (self.term.range.start - chunk_range.start) as usize;
        let end_idx = (self.term.range.end - chunk_range.start) as usize;

        let start_byte_index = chunk_byte_indices[start_idx] as usize;
        let end_byte_index = chunk_byte_indices[end_idx] as usize;
        debug_assert!(start_byte_index < data.len());
        debug_assert!(end_byte_index <= data.len());
        debug_assert!(start_byte_index < end_byte_index);
        let data_slice = &data[start_byte_index..end_byte_index];

        // extract just the actual range data out of the term download output
        let start = self.skip_bytes as usize;
        let end = start + self.take as usize;
        let final_term_data = &data_slice[start..end];

        Ok(TermDownloadResult {
            payload: final_term_data.to_vec(),
            duration,
            n_retries_on_403,
        })
    }
}

#[derive(Debug)]
pub(crate) struct TermDownloadResult<T> {
    pub payload: T,         // download result
    pub duration: Duration, // duration to download
    pub n_retries_on_403: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct TermDownloadOutput {
    pub data: Vec<u8>,
    pub chunk_byte_indices: Vec<u32>,
    pub chunk_range: ChunkRange,
}

impl From<CacheRange> for TermDownloadOutput {
    fn from(CacheRange { data, offsets, range }: CacheRange) -> Self {
        Self {
            data,
            chunk_byte_indices: offsets,
            chunk_range: range,
        }
    }
}

impl FetchTermDownload {
    // Download and return results, retry on 403
    pub async fn run(self) -> Result<TermDownloadResult<TermDownloadOutput>> {
        let instant = Instant::now();
        let mut n_retries_on_403 = 0;

        let key = (self.hash.into(), self.range);
        let data = loop {
            let (fetch_info, v) = self.fetch_info.find(key).await?;

            let range_data = get_one_fetch_term_data(
                self.hash,
                fetch_info,
                self.client.clone(),
                self.chunk_cache.clone(),
                self.range_download_single_flight.clone(),
            )
            .await;

            if let Err(CasClientError::PresignedUrlExpirationError) = range_data {
                self.fetch_info.refresh(v).await?;
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
}

#[derive(Debug)]
pub(crate) struct ChunkRangeWrite {
    pub chunk_range: ChunkRange,
    pub unpacked_length: u32,
    pub skip_bytes: u64, // number of bytes to skip at the front
    pub take: u64,       // number of bytes to take after skipping bytes,
    pub writer_offset: u64,
}

/// Helper object containing the structs needed when downloading and writing a term in parallel
/// during reconstruction.
#[derive(Debug)]
pub(crate) struct FetchTermDownloadOnceAndWriteEverywhereUsed {
    pub download: FetchTermDownload,
    // pub write_offset: u64, // start position of the writer to write to
    pub output: OutputProvider,
    pub writes: Vec<ChunkRangeWrite>,
}

impl FetchTermDownloadOnceAndWriteEverywhereUsed {
    /// Download the term and write it to the underlying storage, retry on 403
    pub async fn run(self) -> Result<TermDownloadResult<u64>> {
        let download_result = self.download.run().await?;
        let TermDownloadOutput {
            data,
            chunk_byte_indices,
            chunk_range,
        } = download_result.payload;
        debug_assert_eq!(chunk_byte_indices.len(), (chunk_range.end - chunk_range.start + 1) as usize);
        debug_assert_eq!(*chunk_byte_indices.last().expect("checked len is something") as usize, data.len());

        // write out the data
        let mut total_written = 0;
        for write in self.writes {
            debug_assert!(write.chunk_range.start >= chunk_range.start);
            debug_assert!(write.chunk_range.end > chunk_range.start);
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

            let mut writer = self.output.get_writer_at(write.writer_offset)?;
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
}

pub(crate) enum DownloadQueueItem<T> {
    End,
    DownloadTask(T),
    Metadata(FetchInfo),
}

/// A utility to tune the segment size for downloading terms in a reconstruction.
/// Yields a segment size based on an approximate number of ranges in a segment,
pub struct DownloadSegmentLengthTuner {
    n_range_in_segment: Mutex<usize>, // number of range in a segment to fetch file reconstruction info
    max_segments: usize,
    delta: usize,
}

impl DownloadSegmentLengthTuner {
    pub fn new(n_range_in_segment_base: usize, max_segments: usize, delta: usize) -> Arc<Self> {
        Arc::new(Self {
            n_range_in_segment: Mutex::new(n_range_in_segment_base),
            max_segments,
            delta,
        })
    }

    pub fn from_configurable_constants() -> Arc<Self> {
        if *NUM_RANGE_IN_SEGMENT_BASE == 0 {
            warn!(
                "NUM_RANGE_IN_SEGMENT_BASE is set to 0, which means no segments will be downloaded.
                   This is likely a misconfiguration. Please check your environment variables."
            );
        }
        let max_num_segments = if *NUM_RANGE_IN_SEGMENT_MAX == 0 {
            usize::MAX
        } else {
            *NUM_RANGE_IN_SEGMENT_MAX
        };

        Self::new(*NUM_RANGE_IN_SEGMENT_BASE, max_num_segments, *NUM_RANGE_IN_SEGMENT_DELTA)
    }

    pub fn next_segment_size(&self) -> Result<u64> {
        Ok(*self.n_range_in_segment.lock()? as u64 * *MAX_XORB_BYTES as u64)
    }

    pub fn tune_on<T>(&self, metrics: TermDownloadResult<T>) -> Result<()> {
        let mut num_range_in_segment = self.n_range_in_segment.lock()?;
        debug_assert!(*num_range_in_segment <= self.max_segments);
        if metrics.n_retries_on_403 > 0 {
            if *num_range_in_segment > 1 {
                let delta = NUM_RANGE_IN_SEGMENT_DELTA.min(*num_range_in_segment - 1);
                info!("detected retries on 403, shrinking segment size by {delta} ranges");
                *num_range_in_segment -= delta;
            } else {
                info!(
                    "detected retries on 403, but segment size is already at minimum (1 range), not shrinking further"
                );
            }
        } else if *num_range_in_segment != self.max_segments {
            // TODO: check download speed and consider if we should increase or decrease
            let delta = NUM_RANGE_IN_SEGMENT_DELTA.min(self.max_segments - *num_range_in_segment);
            debug!("expanding segment size by {delta} approx ranges");
            *num_range_in_segment += delta;
        }

        Ok(())
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
pub(crate) async fn get_one_fetch_term_data(
    hash: MerkleHash,
    fetch_term: CASReconstructionFetchInfo,
    http_client: Arc<ClientWithMiddleware>,
    chunk_cache: Option<Arc<dyn ChunkCache>>,
    range_download_single_flight: RangeDownloadSingleFlight,
) -> Result<TermDownloadOutput> {
    debug!("getting {hash} {fetch_term:?}");
    if fetch_term.range.end < fetch_term.range.start {
        return Err(CasClientError::InvalidRange);
    }

    // check disk cache for the exact range we want for the reconstruction term
    if let Some(cache) = &chunk_cache {
        let key = Key {
            prefix: PREFIX_DEFAULT.to_string(),
            hash,
        };
        if let Ok(Some(cached)) = cache.get(&key, &fetch_term.range).await.log_error("cache error") {
            return Ok(cached.into());
        }
    }

    // fetch the range from blob store and deserialize the chunks
    // then put into the cache if used
    let download_range_result = range_download_single_flight
        .work_dump_caller_info(&fetch_term.url, download_fetch_term_data(hash.into(), fetch_term.clone(), http_client))
        .await?;

    let DownloadRangeResult::Data(term_download_output) = download_range_result else {
        return Err(CasClientError::PresignedUrlExpirationError);
    };

    // now write it to cache, the whole fetched term
    if let Some(cache) = chunk_cache {
        let key = Key {
            prefix: PREFIX_DEFAULT.to_string(),
            hash,
        };
        if let Err(e) = cache
            .put(&key, &fetch_term.range, &term_download_output.chunk_byte_indices, &term_download_output.data)
            .await
        {
            info!("Writing to local cache failed, continuing. Error: {}", e);
        }
    }

    Ok(term_download_output)
}

struct ChunkRangeDeserializeFromBytesStreamRetryCondition;

impl tokio_retry::Condition<CasClientError> for ChunkRangeDeserializeFromBytesStreamRetryCondition {
    fn should_retry(&mut self, err: &CasClientError) -> bool {
        // we only care about retrying some error yielded by trying to deserialize the stream
        let CasClientError::CasObjectError(CasObjectError::InternalIOError(cas_object_io_err)) = err else {
            return false;
        };
        let Some(inner) = cas_object_io_err.get_ref() else {
            return false;
        };
        let Some(inner_reqwest_err) = inner.downcast_ref::<reqwest::Error>() else {
            return false;
        };
        // errors that indicate reading the body failed
        inner_reqwest_err.is_body()
            || inner_reqwest_err.is_decode()
            || inner_reqwest_err.is_timeout()
            || inner_reqwest_err.is_request()
    }
}

/// use the provided http_client to make requests to S3/blob store using the url and url_range
/// parts of a CASReconstructionFetchInfo. The url_range part is used directly in a http Range header
/// value (see fn `range_header`).
async fn download_fetch_term_data(
    hash: HexMerkleHash,
    fetch_term: CASReconstructionFetchInfo,
    http_client: Arc<ClientWithMiddleware>,
) -> Result<DownloadRangeResult> {
    trace!("{hash},{},{}", fetch_term.range.start, fetch_term.range.end);

    let url = Url::parse(fetch_term.url.as_str())?;

    tokio_retry::RetryIf::spawn(
        ExponentialBackoff::from_millis(BASE_RETRY_DELAY_MS)
            .max_delay(Duration::from_millis(BASE_RETRY_MAX_DURATION_MS))
            .take(NUM_RETRIES as usize),
        || async {
            let response = match http_client
                .get(url.clone())
                .header(RANGE, fetch_term.url_range.range_header())
                .with_extension(Api("s3::get_range"))
                .send()
                .await
                .map_err(CasClientError::from)
                .log_error("error downloading range")?
                .error_for_status()
            {
                Ok(response) => response,
                Err(e) => return match e.status() {
                    Some(StatusCode::FORBIDDEN) => {
                        info!("error code {} for hash {hash}, will re-fetch reconstruction", StatusCode::FORBIDDEN,);
                        Ok(DownloadRangeResult::Forbidden)
                    },
                    _ => Err(e.into()),
                }
                .log_error("error code"),
            };

            if let Some(content_length) = response.content_length() {
                let expected_len = fetch_term.url_range.length();
                if content_length != expected_len {
                    error!("got back a smaller byte range ({content_length}) than requested ({expected_len}) from s3");
                    return Err(CasClientError::InvalidRange);
                }
            }

            let (data, chunk_byte_indices) = cas_object::deserialize_async::deserialize_chunks_from_stream(
                response.bytes_stream().map_err(std::io::Error::other),
            )
            .await?;
            Ok(DownloadRangeResult::Data(TermDownloadOutput {
                data,
                chunk_byte_indices,
                chunk_range: fetch_term.range,
            }))
        },
        ChunkRangeDeserializeFromBytesStreamRetryCondition,
    )
    .await
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use cas_types::{HttpRange, QueryReconstructionResponse};
    use httpmock::prelude::*;
    use tokio::task::JoinSet;
    use tokio::time::sleep;

    use super::*;
    use crate::{build_http_client, RetryConfig};

    #[tokio::test]
    async fn test_fetch_info_query_and_find() -> Result<()> {
        // Arrange test data
        let file_range = FileRange::new(100, 200);

        // fetch info of xorb with hash "0...1" and two coalesced ranges
        let xorb1: HexMerkleHash = MerkleHash::from_hex(&format!("{:0>64}", "1"))?.into();
        let x1range = vec![
            CASReconstructionFetchInfo {
                range: ChunkRange::new(5, 20),
                url: "".to_owned(),
                url_range: HttpRange::new(34, 400),
            },
            CASReconstructionFetchInfo {
                range: ChunkRange::new(40, 87),
                url: "".to_owned(),
                url_range: HttpRange::new(589, 1034),
            },
        ];

        // fetch info of xorb with hash "0...2" and two coalesced ranges
        let xorb2: HexMerkleHash = MerkleHash::from_hex(&format!("{:0>64}", "2"))?.into();
        let x2range = vec![
            CASReconstructionFetchInfo {
                range: ChunkRange::new(2, 20),
                url: "".to_owned(),
                url_range: HttpRange::new(56, 400),
            },
            CASReconstructionFetchInfo {
                range: ChunkRange::new(23, 96),
                url: "".to_owned(),
                url_range: HttpRange::new(1560, 10348),
            },
        ];

        // Arrange server
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET)
                .path(format!("/reconstruction/{}", MerkleHash::default()))
                .header(RANGE.as_str(), HttpRange::from(file_range).range_header());
            let response = QueryReconstructionResponse {
                offset_into_first_range: 0,
                terms: Default::default(),
                fetch_info: HashMap::from([(xorb1, x1range.clone()), (xorb2, x2range.clone())]),
            };
            then.status(200).json_body_obj(&response);
        });

        let fetch_info = FetchInfo::new(
            MerkleHash::default(),
            file_range,
            server.base_url(),
            Arc::new(build_http_client(RetryConfig::default(), "")?),
        );

        fetch_info.query().await?;

        // Test find fetch info
        let test_range1 = ChunkRange::new(6, 8);
        let (fi, _) = fetch_info.find((xorb1, test_range1)).await?;
        assert_eq!(x1range[0], fi);
        let test_range2 = ChunkRange::new(20, 40);
        let ret = fetch_info.find((xorb1, test_range2)).await;
        assert!(ret.is_err());
        let test_range3 = ChunkRange::new(40, 88);
        let ret = fetch_info.find((xorb1, test_range3)).await;
        assert!(ret.is_err());

        let (fi, _) = fetch_info.find((xorb2, test_range1)).await?;
        assert_eq!(x2range[0], fi);
        let test_range4 = x2range[1].range;
        let (fi, _) = fetch_info.find((xorb2, test_range4)).await?;
        assert_eq!(x2range[1], fi);

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_info_concurrent_refresh() -> Result<()> {
        let file_range_to_refresh = FileRange::new(100, 200);

        // Arrange server
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/reconstruction/{}", MerkleHash::default()))
                .header(RANGE.as_str(), HttpRange::from(file_range_to_refresh).range_header());
            let response = QueryReconstructionResponse {
                offset_into_first_range: 0,
                terms: Default::default(),
                fetch_info: Default::default(),
            };
            then.status(200).json_body_obj(&response);
        });

        let fetch_info = Arc::new(FetchInfo::new(
            MerkleHash::default(),
            file_range_to_refresh,
            server.base_url(),
            Arc::new(build_http_client(RetryConfig::default(), "")?),
        ));

        // Spawn multiple tasks each calling into refresh with a different delay in
        // [0, 1000] ms, where the refresh itself takes 100 ms to finish. This means
        // some are refreshing at the same time, and for the rest when they call
        // refresh() the operation was done recently.
        let mut tasks = JoinSet::<Result<()>>::new();
        for i in 0..100 {
            let fi = fetch_info.clone();
            tasks.spawn(async move {
                let v = 0;
                sleep(Duration::from_millis(i * 10)).await;
                Ok(fi.refresh(v).await?)
            });
        }

        tasks.join_all().await;

        // Assert that only one refresh happened.
        assert_eq!(*fetch_info.version.lock().await, 1);
        assert_eq!(mock.hits(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_term_download_and_refresh() -> Result<()> {
        // Arrange test data
        let file_range = FileRange::new(0, 200);
        let server = MockServer::start();

        // fetch info fo xorb with hash "0...1" and two coalesced ranges
        let xorb1: HexMerkleHash = MerkleHash::from_hex(&format!("{:0>64}", "1"))?.into();
        let x1range = vec![CASReconstructionFetchInfo {
            range: ChunkRange::new(5, 20),
            url: server.url(format!("/get_xorb/{xorb1}/")),
            url_range: HttpRange::new(34, 400),
        }];

        // Arrange server
        let mock_fi = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/reconstruction/{}", MerkleHash::default()))
                .header(RANGE.as_str(), HttpRange::from(file_range).range_header());
            let response = QueryReconstructionResponse {
                offset_into_first_range: 0,
                terms: vec![CASReconstructionTerm {
                    hash: xorb1,
                    unpacked_length: 100,
                    range: ChunkRange::new(6, 7),
                }],
                fetch_info: HashMap::from([(xorb1, x1range.clone())]),
            };
            then.status(200).json_body_obj(&response).delay(Duration::from_millis(100));
        });

        // Test download once and get 403
        let mock_data = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/get_xorb/{xorb1}/"))
                .header(RANGE.as_str(), x1range[0].url_range.range_header());
            then.status(403).delay(Duration::from_millis(100));
        });

        let fetch_info = FetchInfo::new(
            MerkleHash::default(),
            file_range,
            server.base_url(),
            Arc::new(build_http_client(RetryConfig::default(), "")?),
        );

        let (offset_info_first_range, terms) = fetch_info.query().await?.unwrap();

        let download_task = SequentialTermDownload {
            download: FetchTermDownload {
                hash: xorb1.into(),
                range: x1range[0].range,
                fetch_info: Arc::new(fetch_info),
                chunk_cache: None,
                client: Arc::new(build_http_client(RetryConfig::default(), "")?),
                range_download_single_flight: Arc::new(Group::new()),
            },
            term: terms[0].clone(),
            skip_bytes: offset_info_first_range,
            take: file_range.length(),
        };

        let handle = tokio::spawn(async move { download_task.run().await });

        // Wait for the download_task to refresh and retry for a couple of times.
        tokio::time::sleep(Duration::from_secs(1)).await;

        // download task will not return if keep hitting 403
        handle.abort();

        assert!(mock_fi.hits() >= 2);
        assert!(mock_data.hits() >= 2);

        Ok(())
    }
}
