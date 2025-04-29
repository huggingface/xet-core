use std::sync::Arc;

use cas_client::{Client, OutputProvider};
use cas_types::FileRange;
use merklehash::MerkleHash;
use utils::progress::ProgressUpdater;
use xet_threadpool::ThreadPool;

use crate::configurations::TranslatorConfig;
use crate::errors::*;
use crate::prometheus_metrics;
use crate::remote_client_interface::create_remote_client;

/// Manages the download of files based on a hash or pointer file.
///
/// This class handles the clean operations.  It's meant to be a single atomic session
/// that succeeds or fails as a unit;  i.e. all files get uploaded on finalization, and all shards
/// and xorbs needed to reconstruct those files are properly uploaded and registered.
pub struct FileDownloader {
    /* ----- Configurations ----- */
    config: Arc<TranslatorConfig>,
    client: Arc<dyn Client + Send + Sync>,
}

/// Smudge operations
impl FileDownloader {
    pub async fn new(config: Arc<TranslatorConfig>, threadpool: Arc<ThreadPool>) -> Result<Self> {
        let client = create_remote_client(&config, threadpool.clone(), false)?;

        Ok(Self { config, client })
    }

    pub async fn smudge_file_from_hash(
        &self,
        file_id: &MerkleHash,
        output: &OutputProvider,
        range: Option<FileRange>,
        progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Result<u64> {
        // Currently, this works by always directly querying the remote server.
        let n_bytes = self.client.get_file(file_id, range, output, progress_updater).await?;

        prometheus_metrics::FILTER_BYTES_SMUDGED.inc_by(n_bytes);

        Ok(n_bytes)
    }
}
