use std::borrow::Cow;
use std::sync::Arc;

use cas_client::Client;
use cas_types::FileRange;
use file_reconstruction::{DataOutput, FileReconstructor};
use merklehash::MerkleHash;
use progress_tracking::item_tracking::ItemProgressUpdater;
use tracing::instrument;
use ulid::Ulid;

use crate::configurations::TranslatorConfig;
use crate::errors::*;
use crate::prometheus_metrics;
use crate::remote_client_interface::create_remote_client;

/// Manages the download of files based on a hash or pointer file.
pub struct FileDownloader {
    config: Arc<TranslatorConfig>,
    client: Arc<dyn Client>,
}

impl FileDownloader {
    pub async fn new(config: Arc<TranslatorConfig>) -> Result<Self> {
        let session_id = config
            .session_id
            .as_ref()
            .map(Cow::Borrowed)
            .unwrap_or_else(|| Cow::Owned(Ulid::new().to_string()));
        let client = create_remote_client(&config, &session_id, false)?;
        Ok(Self { config, client })
    }

    #[instrument(skip_all, name = "FileDownloader::smudge_file_from_hash", fields(hash=file_id.hex()))]
    pub async fn smudge_file_from_hash(
        &self,
        file_id: &MerkleHash,
        file_name: Arc<str>,
        output: DataOutput,
        range: Option<FileRange>,
        progress_updater: Option<Arc<ItemProgressUpdater>>,
    ) -> Result<u64> {
        let file_progress_tracker = progress_updater.map(|p| ItemProgressUpdater::item_tracker(&p, file_name, None));

        let mut reconstructor =
            FileReconstructor::new(&self.client, *file_id, output).with_cache(&self.config.data_config.cache_config);

        if let Some(range) = range {
            reconstructor = reconstructor.with_byte_range(range);
        }

        if let Some(tracker) = file_progress_tracker {
            reconstructor = reconstructor.with_progress_updater(tracker);
        }

        let n_bytes = reconstructor.run().await?;
        prometheus_metrics::FILTER_BYTES_SMUDGED.inc_by(n_bytes);

        Ok(n_bytes)
    }
}
