use std::borrow::Cow;
use std::sync::Arc;

use cas_client::{Client, SeekingOutputProvider, SequentialOutput};
use cas_types::FileRange;
use merklehash::MerkleHash;
use progress_tracking::item_tracking::ItemProgressUpdater;
use tracing::{info, instrument};
use ulid::Ulid;

use crate::configurations::{SessionContext, TranslatorConfig};
use crate::errors::*;
use crate::prometheus_metrics;
use crate::remote_client_interface::create_remote_client;

/// Manages the download of files based on a hash or pointer file.
///
/// This class handles the clean operations.  It's meant to be a single atomic session
/// that succeeds or fails as a unit; i.e., all files get uploaded on finalization, and all shards
/// and xorbs needed to reconstruct those files are properly uploaded and registered.
pub struct FileDownloader {
    client: Arc<dyn Client>,
}

/// Smudge operations
impl FileDownloader {
    pub async fn new(session: SessionContext) -> Result<Self> {
        let config = TranslatorConfig::new(session)?;
        let session_id = config
            .session
            .session_id
            .as_ref()
            .map(Cow::Borrowed)
            .unwrap_or_else(|| Cow::Owned(Ulid::new().to_string()));
        let client = create_remote_client(&config, &session_id, false)?;

        Ok(Self { client })
    }

    #[instrument(skip_all, name = "FileDownloader::smudge_file_from_hash", fields(hash=file_id.hex()
    ))]
    pub async fn smudge_file_from_hash(
        &self,
        file_id: &MerkleHash,
        file_name: Arc<str>,
        output: SeekingOutputProvider,
        range: Option<FileRange>,
        progress_updater: Option<Arc<ItemProgressUpdater>>,
    ) -> Result<u64> {
        let file_progress_tracker = progress_updater.map(|p| ItemProgressUpdater::item_tracker(&p, file_name, None));

        let n_bytes = self
            .client
            .clone()
            .get_file_with_parallel_writer(file_id, range, output, file_progress_tracker)
            .await?;

        prometheus_metrics::FILTER_BYTES_SMUDGED.inc_by(n_bytes);

        Ok(n_bytes)
    }

    #[instrument(skip_all, name = "FileDownloader::smudge_file_from_hash", fields(hash=file_id.hex()
    ))]
    pub async fn smudge_file_from_hash_sequential(
        &self,
        file_id: &MerkleHash,
        file_name: Arc<str>,
        output: SequentialOutput,
        range: Option<FileRange>,
        progress_updater: Option<Arc<ItemProgressUpdater>>,
    ) -> Result<u64> {
        let file_progress_tracker = progress_updater.map(|p| ItemProgressUpdater::item_tracker(&p, file_name, None));

        // Currently, this works by always directly querying the remote server.
        info!("Using sequential writer for smudge");
        let n_bytes = self
            .client
            .clone()
            .get_file_with_sequential_writer(file_id, range, output, file_progress_tracker)
            .await?;

        prometheus_metrics::FILTER_BYTES_SMUDGED.inc_by(n_bytes);

        Ok(n_bytes)
    }
}
