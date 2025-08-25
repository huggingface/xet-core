use std::io::Write;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use data::FileUploadSession;
use data::data_client::{advanced_config, clean_file};
use progress_tracking::{ProgressUpdate, TrackingProgressUpdater};

use crate::auth::Operation;
use crate::constants::{
    GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM, HF_ENDPOINT_ENV, XET_ACCESS_TOKEN_HEADER, XET_TOKEN_EXPIRATION_HEADER,
};
use crate::errors::{GitXetError, Result, internal, not_supported};
use crate::git_repo::GitRepo;
use crate::git_url::{GitUrl, Scheme};
use crate::hub_client::HubClientTokenRefresher;
use crate::lfs_agent_protocol::errors::bad_syntax;
use crate::lfs_agent_protocol::*;

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
pub struct XetAgent {
    repo: OnceLock<GitRepo>,
    remote_url: Option<GitUrl>,
    hf_endpoint: Option<String>,
}

impl TransferAgent for XetAgent {
    async fn init_upload(&mut self, req: &InitRequestInner) -> Result<()> {
        let repo = GitRepo::open_from_cur_dir()?;
        let remote_url = match repo.remote_name_to_url(&req.remote) {
            Ok(url) => url, // the provided `remote` is a remote name
            Err(_) => {
                // the provided `remote` is likely a remote URL, try parse it
                GitUrl::from_str(&req.remote)?
            },
        };

        let hf_endpoint = if !matches!(remote_url.scheme(), Scheme::Http | Scheme::Https) && remote_url.port().is_some()
        {
            let endpoint = std::env::var(HF_ENDPOINT_ENV);
            match endpoint {
                Ok(e) => Some(e),
                Err(_) => {
                    return Err(GitXetError::GitConfigError(
                        "This repository has a non-standard Hugging Face remote URL, please specify the Hugging Face server 
                        endpoint using environment variable 'HF_ENDPOINT'"
                            .to_owned(),
                    ));
                },
            }
        } else {
            None
        };

        self.repo.get_or_init(|| repo);
        self.remote_url = Some(remote_url);
        self.hf_endpoint = hf_endpoint;

        Ok(())
    }

    async fn init_download(&mut self, _: &InitRequestInner) -> Result<()> {
        Err(not_supported(format!(
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
        let repo = self.repo.get().unwrap(); // protocol state guarantees self.repo is set.
        let token_refresher = HubClientTokenRefresher::new(
            repo,
            self.remote_url.clone(),
            self.hf_endpoint.clone(),
            Operation::Upload,
            "",
        )?;
        let config = advanced_config(
            cas_url,
            None,
            Some((token, token_expiry)),
            Some(Arc::new(token_refresher)),
            false, /* upload one file at a time so no need for the heavy progress aggregator */
        )?;
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
