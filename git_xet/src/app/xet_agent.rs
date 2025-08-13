use std::io::Write;
use std::sync::Arc;

use async_trait::async_trait;
use data::FileUploadSession;
use data::data_client::{clean_file, default_config};
use progress_tracking::{ProgressUpdate, TrackingProgressUpdater};

use crate::constants::GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM;
use crate::errors::{GitXetError, Result, internal};
use crate::lfs_agent_protocol::errors::bad_syntax;
use crate::lfs_agent_protocol::*;

const XET_ACCESS_TOKEN_HEADER: &str = "X-Xet-Access-Token";
const XET_TOKEN_EXPIRATION_HEADER: &str = "X-Xet-Token-Expiration";

struct XetProgressUpdaterWrapper<W: Write + Send + Sync + 'static> {
    updater: ProgressUpdater<W>,
}

#[async_trait]
impl<W: Write + Send + Sync + 'static> TrackingProgressUpdater for XetProgressUpdaterWrapper<W> {
    async fn register_updates(&self, updates: ProgressUpdate) {
        let _ = self.updater.update_bytes_so_far(updates.total_bytes_completed);
    }
}

#[derive(Default)]
pub struct XetAgent {}

impl TransferAgent for XetAgent {
    async fn init_upload(&mut self, _: &InitRequestInner) -> Result<()> {
        Ok(())
    }

    async fn init_download(&mut self, _: &InitRequestInner) -> Result<()> {
        Err(GitXetError::NotSupported(format!(
            "custom transfer for download is not available; if you think this is an error, consider 
            upgrade {GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM} or contact Xet Team at Hugging Face."
        )))
    }

    async fn upload_one<W: Write + Send + Sync + 'static>(
        &mut self,
        req: &TransferRequest,
        progress_updater: ProgressUpdater<W>,
    ) -> Result<()> {
        // From git-lfs:
        // > First worker is the only one allowed to start immediately.
        // > The rest wait until successful response from 1st worker to
        // > make sure only 1 login prompt is presented if necessary.
        //
        // Xet upload doesn't invoke interactive login, so we send a response right away
        // with positive progress to trigger simultaneous uploads.
        //
        // For reference see https://github.com/git-lfs/git-lfs/blob/2c7de1f90cbe13bf9c1ed43b84dda88bb32f2ba4/tq/adapterbase.go#L156
        // and https://github.com/git-lfs/git-lfs/blob/2c7de1f90cbe13bf9c1ed43b84dda88bb32f2ba4/tq/custom.go#L304
        progress_updater.update_bytes_so_far(1)?;

        let xet_updater = XetProgressUpdaterWrapper {
            updater: progress_updater,
        };

        let cas_url = req.action.href.clone();
        let token = req.action.header[XET_ACCESS_TOKEN_HEADER].clone();
        let token_expiry: u64 = req.action.header[XET_TOKEN_EXPIRATION_HEADER].parse().map_err(internal)?;
        let config = default_config(cas_url, None, Some((token, token_expiry)), None)?;
        let session = FileUploadSession::new(config, Some(Arc::new(xet_updater))).await?;

        let Some(file_path) = &req.path else {
            return Err(GitXetError::GitLFSProtocolError(bad_syntax("file path not provided for upload request")));
        };

        clean_file(session.clone(), file_path).await?;

        // git-lfs only waits 30s for the agent to finish after sending a termination event,
        // so we need to actually upload the shard after each file upload to have the files registered.
        // See https://github.com/git-lfs/git-lfs/blob/2c7de1f90cbe13bf9c1ed43b84dda88bb32f2ba4/tq/custom.go#L233
        session.finalize().await?;

        Ok(())
    }

    async fn download_one<W: Write + Send + Sync + 'static>(
        &mut self,
        _req: &TransferRequest,
        _progress_updater: ProgressUpdater<W>,
    ) -> Result<std::path::PathBuf> {
        todo!()
    }

    async fn terminate(&mut self) -> Result<()> {
        Ok(())
    }
}
