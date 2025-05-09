use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use cas_types::{CASReconstructionFetchInfo, CASReconstructionTerm, ChunkRange, FileRange, HexMerkleHash, Key};
use chunk_cache::ChunkCache;
use deduplication::constants::MAX_XORB_BYTES;
use derivative::Derivative;
use error_printer::ErrorPrinter;
use futures::TryStreamExt;
use http::header::RANGE;
use http::StatusCode;
use merklehash::MerkleHash;
use reqwest_middleware::ClientWithMiddleware;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, error, info, trace};
use url::Url;
use utils::singleflight::Group;

use crate::error::{CasClientError, Result};
use crate::http_client::Api;
use crate::remote_client::{get_reconstruction_with_endpoint_and_client, PREFIX_DEFAULT};
use crate::OutputProvider;

#[derive(Clone, Debug)]
pub(crate) enum DownloadRangeResult {
    Data(Vec<u8>, Vec<u32>),
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
    file_range: FileRange,
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
pub(crate) struct TermDownload {
    pub term: CASReconstructionTerm,
    pub skip_bytes: u64, // number of bytes to skip at the front
    pub take: u64,       // number of bytes to take after skipping bytes,
    // effectively taking [skip_bytes..skip_bytes+take]
    // out of the downloaded range
    pub fetch_info: Arc<FetchInfo>, // utility to get URL to download this term
    #[derivative(Debug = "ignore")]
    pub chunk_cache: Option<Arc<dyn ChunkCache>>,
    pub client: Arc<ClientWithMiddleware>, // only used for downloading range
    pub range_download_single_flight: RangeDownloadSingleFlight,
}

#[derive(Debug)]
pub(crate) struct TermDownloadResult<T> {
    pub data: T,            // download result
    pub duration: Duration, // duration to download
    pub n_retries_on_403: u32,
}

impl TermDownload {
    // Download and return results, retry on 403
    pub async fn run(self) -> Result<TermDownloadResult<Vec<u8>>> {
        let instant = Instant::now();
        let mut n_retries_on_403 = 0;

        let key = (self.term.hash, self.term.range);
        let mut data = loop {
            let (fetch_info, v) = self.fetch_info.find(key).await?;

            let range_data = get_one_term(
                self.client.clone(),
                self.chunk_cache.clone(),
                self.term.clone(),
                fetch_info,
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

        let skip_bytes = self.skip_bytes.try_into().log_error("incorrect offset into range")?;
        let take = self.take.try_into().log_error("incorrect take bytes")?;
        if skip_bytes > 0 {
            data = data[skip_bytes..skip_bytes + take].to_vec()
        } else {
            data.truncate(take);
        }

        Ok(TermDownloadResult {
            data,
            duration: instant.elapsed(),
            n_retries_on_403,
        })
    }
}

/// Helper object containing the structs needed when downloading and writing a term in parallel
/// during reconstruction.
#[derive(Debug)]
pub(crate) struct TermDownloadAndWrite {
    pub download: TermDownload,
    pub write_offset: u64, // start position of the writer to write to
    pub output: OutputProvider,
}

impl TermDownloadAndWrite {
    /// Download the term and write it to the underlying storage, retry on 403
    pub async fn run(self) -> Result<TermDownloadResult<usize>> {
        let download_result = self.download.run().await?;

        // write out the term
        let mut writer = self.output.get_writer_at(self.write_offset)?;
        writer.write_all(&download_result.data)?;
        writer.flush()?;

        Ok(TermDownloadResult {
            data: download_result.data.len(),
            duration: download_result.duration,
            n_retries_on_403: download_result.n_retries_on_403,
        })
    }
}

pub(crate) enum DownloadQueueItem<T> {
    End,
    Term(T),
    Metadata(FetchInfo),
}

pub struct DownloadScheduler {
    n_range_in_segment: Mutex<usize>, // number of range in a segment to fetch file reconstruction info
    n_concurrent_download_task: Arc<Semaphore>,
}

impl DownloadScheduler {
    pub fn new(n_concurrent_range_get: usize) -> Arc<Self> {
        Arc::new(Self {
            n_range_in_segment: Mutex::new(n_concurrent_range_get),
            n_concurrent_download_task: Arc::new(Semaphore::new(n_concurrent_range_get)),
        })
    }

    pub async fn download_permit(&self) -> Result<OwnedSemaphorePermit> {
        self.n_concurrent_download_task
            .clone()
            .acquire_owned()
            .await
            .map_err(CasClientError::from)
    }

    pub fn next_segment_size(&self) -> Result<u64> {
        Ok(*self.n_range_in_segment.lock()? as u64 * *MAX_XORB_BYTES as u64)
    }

    pub fn tune_on<T>(&self, metrics: TermDownloadResult<T>) -> Result<()> {
        if metrics.n_retries_on_403 > 0 {
            info!("detected retries on 403, shrinking segment size by one range");
            let mut num_range_in_segment = self.n_range_in_segment.lock()?;
            if *num_range_in_segment > 1 {
                *num_range_in_segment -= 1;
            }
            self.n_concurrent_download_task.forget_permits(1);
        } else {
            // TODO: check download speed and consider if we should increase or decrease
            debug!("expanding segment size by one range");
            *self.n_range_in_segment.lock()? += 1;
            self.n_concurrent_download_task.add_permits(1);
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
pub(crate) async fn get_one_term(
    http_client: Arc<ClientWithMiddleware>,
    chunk_cache: Option<Arc<dyn ChunkCache>>,
    term: CASReconstructionTerm,
    fetch_term: CASReconstructionFetchInfo,
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
            return Ok(cached.data.to_vec());
        }
    }

    // fetch the range from blob store and deserialize the chunks
    // then put into the cache if used
    let download_range_result = range_download_single_flight
        .work_dump_caller_info(&fetch_term.url, download_range(http_client, fetch_term.clone(), term.hash))
        .await?;

    let DownloadRangeResult::Data(mut data, chunk_byte_indices) = download_range_result else {
        return Err(CasClientError::PresignedUrlExpirationError);
    };

    // now write it to cache, the whole fetched term
    if let Some(cache) = chunk_cache {
        let key = Key {
            prefix: PREFIX_DEFAULT.to_string(),
            hash: term.hash.into(),
        };
        if let Err(e) = cache.put(&key, &fetch_term.range, &chunk_byte_indices, &data) {
            info!("Writing to local cache failed, continuing. Error: {}", e);
        }
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

/// use the provided http_client to make requests to S3/blob store using the url and url_range
/// parts of a CASReconstructionFetchInfo. The url_range part is used directly in a http Range header
/// value (see fn `range_header`).
async fn download_range(
    http_client: Arc<ClientWithMiddleware>,
    fetch_term: CASReconstructionFetchInfo,
    hash: HexMerkleHash,
) -> Result<DownloadRangeResult> {
    trace!("{hash},{},{}", fetch_term.range.start, fetch_term.range.end);

    let url = Url::parse(fetch_term.url.as_str())?;
    let response = match http_client
        .get(url)
        .header(RANGE, fetch_term.url_range.range_header())
        .with_extension(Api("s3::get_range"))
        .send()
        .await
        .map_err(CasClientError::from)
        .log_error("error downloading range")?
        .error_for_status()
    {
        Ok(response) => response,
        Err(e) => {
            return match e.status() {
                Some(StatusCode::FORBIDDEN) => {
                    info!("error code {} for hash {hash}, will re-fetch reconstruction", StatusCode::FORBIDDEN,);
                    Ok(DownloadRangeResult::Forbidden)
                },
                _ => Err(e.into()),
            }
            .log_error("error code")
        },
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
    Ok(DownloadRangeResult::Data(data, chunk_byte_indices))
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

        let download_task = TermDownload {
            term: terms[0].clone(),
            skip_bytes: offset_info_first_range,
            take: file_range.length(),
            fetch_info: Arc::new(fetch_info),
            chunk_cache: None,
            client: Arc::new(build_http_client(RetryConfig::default(), "")?),
            range_download_single_flight: Arc::new(Group::new()),
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
