use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use cas_types::{CASReconstructionFetchInfo, CASReconstructionTerm, ChunkRange, FileRange, HexMerkleHash};
use chunk_cache::CacheRange;
use deduplication::constants::MAX_XORB_BYTES;
use error_printer::ErrorPrinter;
use merklehash::MerkleHash;
use tracing::{debug, info, warn};
use utils::singleflight::Group;
use xet_runtime::xet_config;

use crate::error::{CasClientError, Result};
use crate::file_reconstruction::file_reconstror::FileReconstructor;

#[derive(Clone, Debug)]
pub enum DownloadRangeResult {
    Data(TermDownloadOutput),
    Forbidden,
}
pub type RangeDownloadSingleFlight = Arc<Group<DownloadRangeResult, CasClientError>>;

pub struct FetchInfo {
    file_hash: MerkleHash,
    pub(crate) file_range: FileRange,
    reconstruction_client: Arc<FileReconstructor>,
    #[allow(clippy::type_complexity)]
    inner: RwLock<HashMap<HexMerkleHash, Vec<CASReconstructionFetchInfo>>>,
    version: tokio::sync::Mutex<u32>,
}

impl std::fmt::Debug for FetchInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchInfo")
            .field("file_hash", &self.file_hash)
            .field("file_range", &self.file_range)
            .finish_non_exhaustive()
    }
}

impl FetchInfo {
    pub fn new(file_hash: MerkleHash, file_range: FileRange, reconstruction_client: Arc<FileReconstructor>) -> Self {
        Self {
            file_hash,
            file_range,
            reconstruction_client,
            inner: Default::default(),
            version: tokio::sync::Mutex::new(0),
        }
    }

    pub fn take_segment(self, segment_size: u64) -> (Self, Option<Self>) {
        let (first_segment, remainder) = self.file_range.take_segment(segment_size);

        (
            FetchInfo::new(self.file_hash, first_segment, self.reconstruction_client.clone()),
            remainder.map(|r| FetchInfo::new(self.file_hash, r, self.reconstruction_client.clone())),
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
        let Some(manifest) = self
            .reconstruction_client
            .get_reconstruction(&self.file_hash, Some(self.file_range))
            .await?
        else {
            return Ok(None);
        };

        *self.inner.write()? = manifest.fetch_info;

        Ok(Some((manifest.offset_into_first_range, manifest.terms)))
    }

    pub async fn refresh(&self, vhint: u32) -> Result<()> {
        let mut v = self.version.lock().await;
        if *v > vhint {
            return Ok(());
        }

        self.query().await?;

        *v += 1;

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct FetchTermDownloadInfo {
    pub hash: MerkleHash,
    pub range: ChunkRange,
    pub fetch_info: Arc<FetchInfo>,
}

#[derive(Debug)]
pub struct FetchTermDownload {
    fetch_term_download: FetchTermDownloadInfo,
    cell: tokio::sync::OnceCell<Result<TermDownloadResult<TermDownloadOutput>>>,
}

impl FetchTermDownload {
    pub fn new(fetch_term_download: FetchTermDownloadInfo) -> Self {
        Self {
            fetch_term_download,
            cell: tokio::sync::OnceCell::new(),
        }
    }

    // This struct is now just a data container with caching.
    // Use FileReconstructor::download_term_with_retry() to perform the download.
    pub async fn run(
        &self,
        reconstruction_client: &Arc<FileReconstructor>,
    ) -> Result<TermDownloadResult<TermDownloadOutput>> {
        let reconstruction_client = reconstruction_client.clone();
        let fetch_term_download = self.fetch_term_download.clone();
        self.cell
            .get_or_init(|| async move { reconstruction_client.download_term_with_retry(&fetch_term_download).await })
            .await
            .clone()
    }
}

#[derive(Debug)]
pub struct SequentialTermDownload {
    pub term: CASReconstructionTerm,
    pub download: Arc<FetchTermDownload>,
    pub skip_bytes: u64,
    pub take: u64,
}

impl SequentialTermDownload {
    // This struct is now just a data container.
    // Use FileReconstructor::process_sequential_term() to process the download.
}

#[derive(Debug)]
pub struct TermDownloadResult<T> {
    pub payload: T,
    pub duration: Duration,
    pub n_retries_on_403: u32,
}

impl<T: Clone> Clone for TermDownloadResult<T> {
    fn clone(&self) -> Self {
        Self {
            payload: self.payload.clone(),
            duration: self.duration,
            n_retries_on_403: self.n_retries_on_403,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TermDownloadOutput {
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

pub enum DownloadQueueItem<T> {
    End,
    DownloadTask(T),
    Metadata(FetchInfo),
}

pub struct DownloadSegmentLengthTuner {
    n_range_in_segment: Mutex<usize>,
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
        if xet_config().client.num_range_in_segment_base == 0 {
            warn!(
                "NUM_RANGE_IN_SEGMENT_BASE is set to 0, which means no segments will be downloaded.
                   This is likely a misconfiguration. Please check your environment variables."
            );
        }
        let max_num_segments = if xet_config().client.num_range_in_segment_max == 0 {
            usize::MAX
        } else {
            xet_config().client.num_range_in_segment_max
        };

        Self::new(
            xet_config().client.num_range_in_segment_base,
            max_num_segments,
            xet_config().client.num_range_in_segment_delta,
        )
    }

    pub fn next_segment_size(&self) -> Result<u64> {
        Ok(*self.n_range_in_segment.lock()? as u64 * *MAX_XORB_BYTES as u64)
    }

    pub fn tune_on<T>(&self, metrics: TermDownloadResult<T>) -> Result<()> {
        let mut num_range_in_segment = self.n_range_in_segment.lock()?;
        debug_assert!(*num_range_in_segment <= self.max_segments);

        info!(retried_on_403=metrics.n_retries_on_403, duration=?metrics.duration, "Download metrics");

        if metrics.n_retries_on_403 > 0 {
            if *num_range_in_segment > 1 {
                let delta = xet_config().client.num_range_in_segment_delta.min(*num_range_in_segment - 1);
                info!("detected retries on 403, shrinking segment size by {delta} ranges");
                *num_range_in_segment -= delta;
            } else {
                info!(
                    "detected retries on 403, but segment size is already at minimum (1 range), not shrinking further"
                );
            }
        } else if *num_range_in_segment != self.max_segments {
            let delta = xet_config()
                .client
                .num_range_in_segment_delta
                .min(self.max_segments - *num_range_in_segment);
            debug!("expanding segment size by {delta} approx ranges");
            *num_range_in_segment += delta;
        }

        Ok(())
    }
}
