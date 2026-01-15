use std::mem::{swap, take};
use std::sync::Arc;

use cas_client::{Client, RemoteClient};
use cas_object::SerializedCasObject;
use deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS};
use deduplication::{DataAggregator, DeduplicationMetrics, RawXorbData};
use mdb_shard::shard_in_memory::MDBInMemoryShard;
use mdb_shard::MDBShardInfo;
use merklehash::{HashedWrite, MerkleHash};
use tokio::sync::Mutex;

use super::configurations::TranslatorConfig;
use super::errors::*;
use crate::wasm_file_cleaner::SingleFileCleaner;
use crate::wasm_timer::ConsoleTimer;
use crate::xorb_uploader::{XorbUploader, XorbUploaderSpawnParallel};

static UPLOAD_CONCURRENCY: usize = 5;

/// Manages the translation of files between the
/// MerkleDB / pointer file format and the materialized version.
///
/// This class handles the clean operations.  It's meant to be a single atomic session
/// that succeeds or fails as a unit;  i.e. all files get uploaded on finalization, and all shards
/// and xorbs needed to reconstruct those files are properly uploaded and registered.
pub struct FileUploadSession {
    /// The configuration settings, if needed.
    pub(crate) config: Arc<TranslatorConfig>,

    pub(crate) client: Arc<dyn Client + Send + Sync>,
    pub(crate) session_shard: Mutex<MDBInMemoryShard>,
    xorb_uploader: Mutex<Option<Box<dyn XorbUploader + Send>>>,

    /// Deduplicated data shared across files.
    current_session_data: Mutex<DataAggregator>,
}

impl FileUploadSession {
    pub fn new(config: Arc<TranslatorConfig>) -> Self {
        let client = RemoteClient::new(
            &config.data_config.endpoint,
            &config.data_config.auth,
            &config.session_id,
            false,
            &config.data_config.user_agent,
        );

        let xorb_uploader =
            Box::new(XorbUploaderSpawnParallel::new(client.clone(), &config.data_config.prefix, UPLOAD_CONCURRENCY));

        Self {
            session_shard: Mutex::new(MDBInMemoryShard::default()),
            xorb_uploader: Mutex::new(Some(xorb_uploader)),
            config,
            client,
            current_session_data: Mutex::new(DataAggregator::default()),
        }
    }

    pub fn start_clean(self: &Arc<Self>, file_id: u64, sha256: Option<MerkleHash>) -> SingleFileCleaner {
        SingleFileCleaner::new(self.clone(), file_id, sha256, true)
    }

    pub(crate) async fn register_single_file_clean_completion(
        self: &Arc<Self>,
        mut file_data: DataAggregator,
        _dedup_metrics: &DeduplicationMetrics,
    ) -> Result<()> {
        // Merge in the remaining file data; uploading a new xorb if need be.
        {
            let mut current_session_data = self.current_session_data.lock().await;

            // Do we need to cut one of these to a xorb?
            if current_session_data.num_bytes() + file_data.num_bytes() > *MAX_XORB_BYTES
                || current_session_data.num_chunks() + file_data.num_chunks() > *MAX_XORB_CHUNKS
            {
                // Cut the larger one as a xorb, uploading it and registering the files.
                if current_session_data.num_bytes() > file_data.num_bytes() {
                    swap(&mut *current_session_data, &mut file_data);
                }

                // Actually upload this outside the lock
                drop(current_session_data);

                self.process_aggregated_data_as_xorb(file_data).await?;
            } else {
                current_session_data.merge_in(file_data);
            }
        }

        Ok(())
    }

    /// Process the aggregated data, uploading the data as a xorb and registering the files
    async fn process_aggregated_data_as_xorb(self: &Arc<Self>, data_agg: DataAggregator) -> Result<()> {
        let (xorb, new_files) = data_agg.finalize();

        // Add the xorb info to the current shard.
        if xorb.num_bytes() > 0 {
            self.session_shard.lock().await.add_cas_block(xorb.cas_info.clone())?;
        }
        self.register_new_xorb_for_upload(xorb).await?;

        for (_, fi, _) in new_files {
            self.session_shard.lock().await.add_file_reconstruction_info(fi)?;
        }

        Ok(())
    }

    pub(crate) async fn register_new_xorb_for_upload(self: &Arc<Self>, xorb: RawXorbData) -> Result<()> {
        // No need to process an empty xorb.
        if xorb.num_bytes() == 0 {
            return Ok(());
        }

        let compression_scheme = self.config.data_config.compression;
        let use_footer = self.client.use_xorb_footer();
        let cas_object = SerializedCasObject::from_xorb(xorb, compression_scheme, use_footer)?;

        let Some(ref mut xorb_uploader) = *self.xorb_uploader.lock().await else {
            return Err(DataProcessingError::internal("register xorb after drop"));
        };
        xorb_uploader.upload_xorb(cas_object).await?;

        Ok(())
    }

    pub async fn finalize(self: Arc<Self>) -> Result<()> {
        // Register the remaining xorbs for upload.
        let data_agg = take(&mut *self.current_session_data.lock().await);
        self.process_aggregated_data_as_xorb(data_agg).await?;

        // Finalize the xorb uploads.
        if let Some(mut xorb_uploader) = self.xorb_uploader.lock().await.take() {
            xorb_uploader.finalize().await?;
        }

        // Now that all the tasks there are completed, there shouldn't be any other references to this session
        // hanging around; i.e. the self in this shession should be used as if it's consuming the class, as it
        // effectively empties all the states.
        debug_assert_eq!(Arc::strong_count(&self), 1);

        // Upload the current shards in the session.
        self.upload_and_register_session_shards().await?;

        Ok(())
    }

    async fn upload_and_register_session_shards(&self) -> Result<()> {
        log::info!("uploading shard");
        let shard = self.session_shard.lock().await;

        let serialized_shard = Vec::with_capacity(shard.shard_file_size() as usize);
        let mut hashed_write = HashedWrite::new(serialized_shard);

        MDBShardInfo::serialize_from(&mut hashed_write, &shard, None)?;

        let shard_hash = hashed_write.hash();
        let shard_data = hashed_write.into_inner();

        log::info!("shard hash: {shard_hash}, {} bytes", shard_data.len());

        let _timer = ConsoleTimer::new("upload shard");
        let permit = self.client.acquire_upload_permit().await?;
        self.client
            .upload_shard(shard_data.into(), permit)
            .await?;

        Ok(())
    }
}
