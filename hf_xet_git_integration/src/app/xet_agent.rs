use std::sync::Arc;

use data::data_client::{clean_file, default_config};
use data::{FileDownloader, FileUploadSession};

use crate::errors::{GitXetError, Result, bad_protocol, internal};
use crate::lfs_agent_protocol::*;

const XET_ACCESS_TOKEN_HEADER: &str = "X-Xet-Access-Token";
const XET_TOKEN_EXPIRATION_HEADER: &str = "X-Xet-Token-Expiration";

#[derive(Default)]
pub struct XetAgent {
    upload_session: Option<Arc<FileUploadSession>>,
    download_session: Option<FileDownloader>,
}

impl TransferAgent for XetAgent {
    async fn init_upload(&mut self, req: &definitions::InitRequestInner) -> Result<()> {
        if req.concurrent {
            return Err(GitXetError::NotSupported("concurrent transfer not supported".to_owned()));
        }

        Ok(())
    }

    async fn init_download(&mut self, req: &definitions::InitRequestInner) -> Result<()> {
        if req.concurrent {
            return Err(GitXetError::NotSupported("concurrent transfer not supported".to_owned()));
        }

        Ok(())
    }

    async fn upload_one<'a, W: std::io::Write>(
        &mut self,
        req: &definitions::TransferRequest,
        progress_updater: ProgressUpdater<'a, W>,
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

        // It seems git-lfs doesn't wait for the agent to finish after sending a termination event,
        // so we need to actually upload the shard after each file upload to have the files registered.
        session.finalize().await?;

        Ok(())
    }

    async fn download_one<'a, W: std::io::Write>(
        &mut self,
        req: &definitions::TransferRequest,
        progress_updater: ProgressUpdater<'a, W>,
    ) -> Result<std::path::PathBuf> {
        todo!()
    }

    async fn terminate(&mut self) -> Result<()> {
        Ok(())
    }
}
