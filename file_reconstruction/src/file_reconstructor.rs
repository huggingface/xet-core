use std::path::Path;
use std::sync::Arc;

use cas_client::Client;
use cas_types::FileRange;
use merklehash::MerkleHash;
use progress_tracking::item_tracking::SingleItemProgressUpdater;
use xet_runtime::xet_config;

use crate::FileReconstructionError;
use crate::data_writer::{DataOutput, DataWriter, new_data_writer};
use crate::error::Result;
use crate::reconstruction_metadata::ReconstructionTermManager;

pub struct FileReconstructor {
    client: Arc<dyn Client>,
    file_hash: MerkleHash,
    byte_range: Option<FileRange>,
    output_mode: Option<DataOutput>,
    progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    config: xet_config::ReconstructionConfig,
}

impl FileReconstructor {
    pub fn new(client: &Arc<dyn Client>, file_hash: MerkleHash) -> Self {
        Self {
            client: client.clone(),
            file_hash,
            byte_range: None,
            output_mode: None,
            progress_updater: None,
            config: xet_config().reconstruction.clone(),
        }
    }

    pub fn with_byte_range(self, byte_range: FileRange) -> Self {
        Self {
            byte_range: Some(byte_range),
            ..self
        }
    }

    pub fn with_output_file(self, _output_path: impl AsRef<Path>) -> Self {
        todo!()
    }

    pub fn with_progress_updater(self, progress_updater: Arc<SingleItemProgressUpdater>) -> Self {
        Self {
            progress_updater: Some(progress_updater),
            ..self
        }
    }

    pub async fn run(self) -> Result<()> {
        let reconstructor = FileReconstructionImpl::from_builder(self)?;
        reconstructor.run().await
    }
}

/// Now the real process here.
pub(crate) struct FileReconstructionImpl {
    client: Arc<dyn Client>,
    file_hash: MerkleHash,
    byte_range: FileRange,
    term_manager: Arc<ReconstructionTermManager>,
    data_writer: Arc<dyn DataWriter>,
    progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    config: xet_config::ReconstructionConfig,
}

impl FileReconstructionImpl {
    pub(crate) fn from_builder(builder: FileReconstructor) -> Result<Self> {
        let FileReconstructor {
            client,
            file_hash,
            byte_range,
            output_mode,
            progress_updater,
            config,
        } = builder;

        let byte_range = byte_range.unwrap_or_else(FileRange::full);
        let term_manager = ReconstructionTermManager::new(client.clone(), file_hash, byte_range);

        let data_output = output_mode.ok_or_else(|| {
            FileReconstructionError::ConfigurationError(
                "Output mode must be specified for FileReconstructor".to_string(),
            )
        })?;

        let data_writer = new_data_writer(data_output, &config, 0, 0)?;

        Ok(Self {
            client,
            file_hash,
            byte_range,
            term_manager,
            data_writer,
            progress_updater,
            config,
        })
    }

    pub(crate) async fn run(self) -> Result<()> {
        // First, start fetching the data.
        // Prefetch the first terms.
        self.term_manager
            .prefetch_next_term_data(self.config.initial_reconstruction_fetch_size.as_u64())
            .await;

        // TODO: Implement the actual reconstruction logic here

        // Finish writing
        self.data_writer.finish().await?;

        Ok(())
    }
}
