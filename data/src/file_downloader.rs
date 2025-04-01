use std::io::Write;
use std::sync::Arc;

use cas_client::Client;
use cas_types::FileRange;
use merklehash::MerkleHash;
use utils::progress::{ItemProgressUpdater, SimpleProgressUpdater, TrackingProgressUpdater};
use xet_threadpool::ThreadPool;

use crate::configurations::TranslatorConfig;
use crate::errors::*;
use crate::remote_client_interface::create_remote_client;
use crate::{prometheus_metrics, PointerFile};

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

    pub async fn smudge_file_from_pointer(
        &self,
        pointer: &PointerFile,
        writer: &mut Box<dyn Write + Send>,
        range: Option<FileRange>,
        progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
    ) -> Result<u64> {
        self.smudge_file_from_hash(&pointer.hash()?, pointer.path().into(), writer, range, progress_updater)
            .await
    }

    pub async fn smudge_file_from_hash(
        &self,
        file_id: &MerkleHash,
        file_name: Arc<str>,
        writer: &mut Box<dyn Write + Send>,
        range: Option<FileRange>,
        progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
    ) -> Result<u64> {
        let file_progress_tracker =
            progress_updater.map(|p| ItemProgressUpdater::new(p, file_name, None) as Arc<dyn SimpleProgressUpdater>);

        // Currently, this works by always directly querying the remote server.
        let n_bytes = self.client.get_file(file_id, range, writer, file_progress_tracker).await?;

        prometheus_metrics::FILTER_BYTES_SMUDGED.inc_by(n_bytes);

        Ok(n_bytes)
    }
}
