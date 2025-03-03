use std::io::Write;
use std::sync::Arc;

use cas_client::Client;
use cas_types::FileRange;
use merklehash::MerkleHash;
use utils::progress::ProgressUpdater;
use xet_threadpool::ThreadPool;

use crate::remote_client_interface::create_remote_client;
use crate::configurations::TranslatorConfig;
use crate::errors::*;
use crate::PointerFile;

/// Manages the download of files based on a hash or pointer file.
///
/// This class handles the clean operations.  It's meant to be a single atomic session
/// that succeeds or fails as a unit;  i.e. all files get uploaded on finalization, and all shards
/// and xorbs needed to reconstruct those files are properly uploaded and registered.
pub struct FileDownloader {
    /* ----- Configurations ----- */
    config: TranslatorConfig,
    client: Arc<dyn Client + Send + Sync>,
}

/// Smudge operations
impl FileDownloader {
    pub async fn new(config: TranslatorConfig, threadpool: Arc<ThreadPool>) -> Result<Self> {
        let client = create_remote_client(&config, threadpool.clone(), false)?;

        Ok(Self { config, client })
    }

    pub async fn smudge_file_from_pointer(
        &self,
        pointer: &PointerFile,
        writer: &mut Box<dyn Write + Send>,
        range: Option<FileRange>,
        progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Result<()> {
        self.smudge_file_from_hash(&pointer.hash()?, writer, range, progress_updater)
            .await
    }

    pub async fn smudge_file_from_hash(
        &self,
        file_id: &MerkleHash,
        writer: &mut Box<dyn Write + Send>,
        range: Option<FileRange>,
        progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Result<()> {
        // Currently, this works by always directly querying the remote server.
        self.client.get_file(file_id, range, writer, progress_updater).await?;
        Ok(())
    }
}
