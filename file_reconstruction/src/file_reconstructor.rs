use std::sync::Arc;

use cas_client::Client;
use cas_types::FileRange;
use merklehash::MerkleHash;
use progress_tracking::item_tracking::SingleItemProgressUpdater;
use xet_runtime::xet_config;

use crate::data_writer::{DataOutput, new_data_writer};
use crate::error::Result;
use crate::reconstruction_terms::ReconstructionTermManager;

pub struct FileReconstruction {
    client: Arc<dyn Client>,
    file_hash: MerkleHash,
    byte_range: Option<FileRange>,
    output: DataOutput,
    progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    config: Arc<xet_config::ReconstructionConfig>,
}

impl FileReconstruction {
    pub fn new(client: &Arc<dyn Client>, file_hash: MerkleHash, output: DataOutput) -> Self {
        Self {
            client: client.clone(),
            file_hash,
            byte_range: None,
            output,
            progress_updater: None,
            config: Arc::new(xet_config().reconstruction.clone()),
        }
    }

    pub fn with_byte_range(self, byte_range: FileRange) -> Self {
        Self {
            byte_range: Some(byte_range),
            ..self
        }
    }

    pub fn with_progress_updater(self, progress_updater: Arc<SingleItemProgressUpdater>) -> Self {
        Self {
            progress_updater: Some(progress_updater),
            ..self
        }
    }

    pub fn with_config(self, config: xet_config::ReconstructionConfig) -> Self {
        Self {
            config: Arc::new(config),
            ..self
        }
    }

    pub async fn run(self) -> Result<()> {
        let Self {
            client,
            file_hash,
            byte_range,
            output,
            progress_updater,
            config,
        } = self;

        let byte_range = byte_range.unwrap_or_else(FileRange::full);

        let term_manager = ReconstructionTermManager::new(config.clone(), client.clone(), file_hash, byte_range).await;

        let data_writer = new_data_writer(output, &config)?;


        for term_idx in 0.. {

            // Get the next term
            let Some(file_term) = term_manager.get_file_term



        while let Some(file_term) = term_manager.next_file_term().await? {
            // Get the data recepticle for this next term.
            let data_receptacle = data_writer.next_data_receptacle(file_term.range, None).await?;

            // Now, check the

            // Now, acquire the download permit.
            let data = file_term.data.clone();
            data_receptacle.write(data).await?;

            if let Some(progress_updater) = progress_updater.as_ref() {
                progress_updater.update(data.len() as u64).await;
            }
        }

        data_writer.finish().await?;

        Ok(())
    }
}
