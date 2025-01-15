use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use cas_client::{Client, UploadMetrics};
use mdb_shard::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo};
use mdb_shard::ShardFileManager;
use merkledb::aggregate_hashes::cas_node_hash;
use merklehash::MerkleHash;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tracing::info;
use utils::progress::ProgressUpdater;
use utils::ThreadPool;

use crate::data_processing::CASDataAggregator;
use crate::errors::DataProcessingError::*;
use crate::errors::*;

const DEFAULT_NETWORK_STAT_REPORT_INTERVAL_SEC: u32 = 2; // 2 s

#[async_trait]
pub(crate) trait XorbUpload {
    /// Register a block of data ready for upload and dedup, return the hash of the produced xorb.
    async fn register_new_cas_block(&self, cas_data: CASDataAggregator) -> Result<MerkleHash>;
    /// Flush all xorbs that are pending to be sent to remote.
    async fn flush(&self) -> Result<()>;
}

struct NetworkStatCheckPoint {
    n_bytes: u64,
    start: Instant,
}

impl Default for NetworkStatCheckPoint {
    fn default() -> Self {
        Self {
            n_bytes: 0,
            start: Instant::now(),
        }
    }
}

struct NetworkStat {
    accumulated: NetworkStatCheckPoint,
    last_check_point: NetworkStatCheckPoint,
    report_interval: Duration,
}

impl NetworkStat {
    fn new(report_interval_sec: u32) -> Self {
        Self {
            accumulated: Default::default(),
            last_check_point: Default::default(),
            report_interval: Duration::from_secs(report_interval_sec.into()),
        }
    }

    fn update_and_report(&mut self, metrics: &UploadMetrics, what: &str) {
        self.accumulated.n_bytes += metrics.n_bytes as u64;
        let now = Instant::now();
        if now.duration_since(self.last_check_point.start) >= self.report_interval {
            Self::report_rate(&format!("{what} accumulated"), self.accumulated.n_bytes, self.accumulated.start, now);
            Self::report_rate(
                &format!("{what} instantaneous"),
                self.accumulated.n_bytes - self.last_check_point.n_bytes,
                self.last_check_point.start,
                now,
            );
            self.last_check_point = NetworkStatCheckPoint {
                n_bytes: self.accumulated.n_bytes,
                start: Instant::now(),
            };
        }
    }

    fn report_rate(what: &str, n_bytes: u64, start: Instant, end: Instant) {
        const RATE_UNIT: [(f64, &str); 3] = [(1e9, "Gbps"), (1e6, "Mbps"), (1e3, "Kbps")];

        let duration = end.duration_since(start);

        if n_bytes == 0 {
            info!("{what} rate: 0 bps");
        }

        let bps = n_bytes as f64 * 8. / duration.as_secs_f64();

        for (base, unit) in RATE_UNIT {
            let curr = bps / base;
            if curr > 1. {
                info!("{what} rate: {curr:.2} {unit}");
                return;
            }
        }

        info!("{what} rate: {bps:.2} bps");
    }
}

/// Helper to parallelize xorb upload and registration.
/// Calls to registering xorbs return immediately after computing a xorb hash so callers
/// can continue with other work, and xorb data is queued internally to be uploaded and registered.
///
/// It is critical to call [`flush`] before `ParallelXorbUploader` is dropped. Dropping will
/// cancel all ongoing transfers automatically.
pub(crate) struct ParallelXorbUploader {
    // Configurations
    cas_prefix: String,

    // Utils
    shard_manager: Arc<ShardFileManager>,
    cas: Arc<dyn Client + Send + Sync>,

    // Internal worker
    upload_tasks: Mutex<JoinSet<Result<UploadMetrics>>>,

    // Rate limiter
    rate_limiter: Arc<Semaphore>,

    // Theadpool
    threadpool: Arc<ThreadPool>,

    // Network metrics
    egress_stat: Mutex<NetworkStat>,

    // Upload Progress
    upload_progress_updater: Option<Arc<dyn ProgressUpdater>>,
}

impl ParallelXorbUploader {
    pub async fn new(
        cas_prefix: &str,
        shard_manager: Arc<ShardFileManager>,
        cas: Arc<dyn Client + Send + Sync>,
        rate_limiter: Arc<Semaphore>,
        threadpool: Arc<ThreadPool>,
        upload_progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Arc<Self> {
        Arc::new(ParallelXorbUploader {
            cas_prefix: cas_prefix.to_owned(),
            shard_manager,
            cas,
            upload_tasks: Mutex::new(JoinSet::new()),
            rate_limiter,
            threadpool,
            egress_stat: Mutex::new(NetworkStat::new(DEFAULT_NETWORK_STAT_REPORT_INTERVAL_SEC)), // report every 2 s
            upload_progress_updater,
        })
    }

    async fn status_is_ok(&self) -> Result<()> {
        let mut upload_tasks = self.upload_tasks.lock().await;
        while let Some(result) = upload_tasks.try_join_next() {
            let metrics = result??;
            let mut egress_rate = self.egress_stat.lock().await;
            egress_rate.update_and_report(&metrics, "Xorb upload");
        }

        Ok(())
    }
}

#[async_trait]
impl XorbUpload for ParallelXorbUploader {
    async fn register_new_cas_block(&self, cas_data: CASDataAggregator) -> Result<MerkleHash> {
        self.status_is_ok().await?;

        let xorb_data_len = cas_data.data.len();

        let cas_hash = cas_node_hash(&cas_data.chunks[..]);

        // Rate limiting, the acquired permit is dropped after the task completes.
        // The chosen Semaphore is fair, meaning xorbs added first will be scheduled to upload first.
        let permit = self
            .rate_limiter
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| UploadTaskError(e.to_string()))?;

        let item = (cas_hash, cas_data.data, cas_data.chunks);
        let shard_manager = self.shard_manager.clone();
        let cas = self.cas.clone();
        let cas_prefix = self.cas_prefix.clone();

        let mut upload_tasks = self.upload_tasks.lock().await;
        let upload_progress_updater = self.upload_progress_updater.clone();
        upload_tasks.spawn_on(
            async move {
                let ret = upload_and_register_xorb(item, shard_manager, cas, cas_prefix).await;
                if let Some(updater) = upload_progress_updater {
                    updater.update(xorb_data_len as u64);
                }
                drop(permit);
                ret
            },
            &self.threadpool.handle(),
        );

        // Now register any new files as needed.
        for (mut fi, chunk_hash_indices) in cas_data.pending_file_info {
            for i in chunk_hash_indices {
                debug_assert_eq!(fi.segments[i].cas_hash, MerkleHash::default());
                fi.segments[i].cas_hash = cas_hash;
            }

            self.shard_manager.add_file_reconstruction_info(fi).await?;
        }

        Ok(cas_hash)
    }

    /// Flush makes sure all xorbs added to queue before this call are sent successfully
    /// to remote. This function can be called multiple times and should be called at
    /// least once before `ParallelXorbUploader` is dropped.
    async fn flush(&self) -> Result<()> {
        let mut upload_tasks = self.upload_tasks.lock().await;

        while let Some(result) = upload_tasks.join_next().await {
            let metrics = result??;
            let mut egress_rate = self.egress_stat.lock().await;
            egress_rate.update_and_report(&metrics, "Xorb upload");
        }

        Ok(())
    }
}

type XorbUploadValueType = (MerkleHash, Vec<u8>, Vec<(MerkleHash, usize)>);

async fn upload_and_register_xorb(
    item: XorbUploadValueType,
    shard_manager: Arc<ShardFileManager>,
    cas: Arc<dyn Client + Send + Sync>,
    cas_prefix: String,
) -> Result<UploadMetrics> {
    let (cas_hash, data, chunks) = item;

    let raw_bytes_len = data.len();
    // upload xorb
    let metrics = {
        let mut pos = 0;
        let chunk_and_boundaries = chunks
            .iter()
            .map(|(hash, len)| {
                pos += *len;
                (*hash, pos as u32)
            })
            .collect();
        cas.put(&cas_prefix, &cas_hash, data, chunk_and_boundaries).await?
    };

    // register for dedup
    // This should happen after uploading xorb above succeeded so not to
    // leave invalid information in the local shard to dedup other xorbs.
    {
        let metadata = CASChunkSequenceHeader::new(cas_hash, chunks.len(), raw_bytes_len);

        let mut pos = 0;
        let chunks: Vec<_> = chunks
            .iter()
            .map(|(h, len)| {
                let result = CASChunkSequenceEntry::new(*h, *len, pos);
                pos += *len;
                result
            })
            .collect();
        let cas_info = MDBCASInfo { metadata, chunks };

        shard_manager.add_cas_block(cas_info).await?;
    }

    Ok(metrics)
}
