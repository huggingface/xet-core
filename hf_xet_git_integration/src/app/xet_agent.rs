use data::FileUploadSession;
use data::data_client::{clean_file, default_config};

use crate::errors::{GitXetError, Result, bad_protocol, internal};
use crate::lfs_agent_protocol::*;

const XET_ACCESS_TOKEN_HEADER: &str = "X-Xet-Access-Token";
const XET_TOKEN_EXPIRATION_HEADER: &str = "X-Xet-Token-Expiration";

#[derive(Default)]
pub struct XetAgent {}

impl TransferAgent for XetAgent {
    async fn init_upload(&mut self, req: &definitions::InitRequestInner) -> Result<()> {
        Ok(())
    }

    async fn init_download(&mut self, req: &definitions::InitRequestInner) -> Result<()> {
        Ok(())
    }

    async fn upload_one<W: std::io::Write>(
        &mut self,
        req: &definitions::TransferRequest,
        _progress_updater: ProgressUpdater<'_, W>,
    ) -> Result<()> {
        let cas_url = req.action.href.clone();
        let token = req.action.header[XET_ACCESS_TOKEN_HEADER].clone();
        let token_expiry: u64 = req.action.header[XET_TOKEN_EXPIRATION_HEADER].parse().map_err(internal)?;
        let config = default_config(cas_url, None, Some((token, token_expiry)), None)?;
        let session = FileUploadSession::new(config, None).await?;

        let Some(file_path) = &req.path else {
            return Err(bad_protocol("file path not provided for upload request"));
        };

        clean_file(session.clone(), file_path).await?;

        // It seems git-lfs only waits 30s for the agent to finish after sending a termination event,
        // so we need to actually upload the shard after each file upload to have the files registered.
        // See https://github.com/git-lfs/git-lfs/blob/2c7de1f90cbe13bf9c1ed43b84dda88bb32f2ba4/tq/custom.go#L233
        session.finalize().await?;

        Ok(())
    }

    async fn download_one<W: std::io::Write>(
        &mut self,
        _req: &definitions::TransferRequest,
        _progress_updater: ProgressUpdater<'_, W>,
    ) -> Result<std::path::PathBuf> {
        todo!()
    }

    async fn terminate(&mut self) -> Result<()> {
        Ok(())
    }
}
