use std::io::Write;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use data::FileUploadSession;
use data::data_client::{clean_file, default_config};
use hub_client::Operation;
use progress_tracking::{ProgressUpdate, TrackingProgressUpdater};
use utils::auth::TokenRefresher;

use crate::constants::{
    HF_ENDPOINT_ENV, XET_ACCESS_TOKEN_HEADER, XET_CAS_URL, XET_SESSION_ID, XET_TOKEN_EXPIRATION_HEADER,
};
use crate::errors::{GitXetError, Result};
use crate::git_repo::GitRepo;
use crate::git_url::{GitUrl, Scheme};
use crate::lfs_agent_protocol::{
    GitLFSProtocolError, InitRequestInner, ProgressUpdater, TransferAgent, TransferRequest,
};
use crate::token_refresher::DirectRefreshRouteTokenRefresher;

// This implements a Git LFS custom transfer agent that uploads and downloads files using the Xet protocol.
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
            Some(std::env::var(HF_ENDPOINT_ENV).map_err(|_| {
                GitXetError::config_error(
                    r#"This repository has a non-standard Hugging Face remote URL, 
                please specify the Hugging Face server endpoint using environment variable "HF_ENDPOINT""#,
                )
            })?)
        } else {
            None
        };

        self.repo.get_or_init(|| repo);
        self.remote_url = Some(remote_url);
        self.hf_endpoint = hf_endpoint;

        Ok(())
    }

    async fn init_download(&mut self, _: &InitRequestInner) -> Result<()> {
        Err(GitXetError::not_supported(
            "custom transfer for download is not implemented yet. Downloads should operate through standard git-lfs download protocol.
            If you encounter errors downloading, contact Xet Team at Hugging Face.",
        ))
    }

    async fn upload_one<W: Write + Send + Sync + 'static>(
        &mut self,
        req: &TransferRequest,
        progress_updater: ProgressUpdater<W>,
    ) -> Result<()> {
        // Get the token refresher set up before the dummy progress update below,
        // so that if the internal git credential helper needs to prompt the user for credential,
        // only one prompt is presented.
        let repo = self.repo.get().unwrap(); // protocol state guarantees self.repo is set.

        let session_id = req.action.header.get(XET_SESSION_ID).map(|s| s.as_str()).unwrap_or_default();
        let token_refresher: Arc<dyn TokenRefresher> = Arc::new(DirectRefreshRouteTokenRefresher::new(
            repo,
            self.remote_url.clone(),
            &req.action.href,
            Operation::Upload,
            session_id,
        )?);
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

        let cas_url = req
            .action
            .header
            .get(XET_CAS_URL)
            .ok_or_else(|| GitXetError::internal("Hugging Face Hub didn't provide a CAS URL"))?
            .clone();
        let token = req
            .action
            .header
            .get(XET_ACCESS_TOKEN_HEADER)
            .ok_or_else(|| GitXetError::internal("Hugging Face Hub didn't provide a CAS access token"))?
            .clone();
        let token_expiry: u64 = req
            .action
            .header
            .get(XET_TOKEN_EXPIRATION_HEADER)
            .ok_or_else(|| GitXetError::internal("Hugging Face Hub didn't provide a CAS access token expiration"))?
            .parse()
            .map_err(GitXetError::internal)?;

        let config = default_config(cas_url, None, Some((token, token_expiry)), Some(token_refresher))?
            .disable_progress_aggregation()
            .with_session_id(session_id); // upload one file at a time so no need for the heavy progress aggregator
        let session = FileUploadSession::new(config.into(), Some(Arc::new(xet_updater))).await?;

        let Some(file_path) = &req.path else {
            return Err(GitLFSProtocolError::bad_syntax("file path not provided for upload request").into());
        };

        clean_file(session.clone(), file_path).await?;

        // We need to actually upload the shard after each file upload to have the files registered, because
        //
        // 1. LFS custom transfer protocol is sequential: git-lfs waits for the upload/download result of the one file
        //    before sending the request to process the next one;
        // 2. git-lfs doesn't tell agents how many files to upload/download at the initiation phase;
        // 3. After sending a termination signal, git-lfs waits for 30s and sends SIGKILL to the agent. SIGKILL is not
        //    like SIGINT, it can't be intercepted or ignored by a process.
        // 4. Xet system is not a real-time system that guarantees response within any duration. Batching and thus
        //    effectively delaying shard upload means we risk data loss.
        //
        // See https://github.com/git-lfs/git-lfs/blob/2c7de1f90cbe13bf9c1ed43b84dda88bb32f2ba4/tq/custom.go#L233
        session.finalize().await?;

        Ok(())
    }

    async fn download_one<W: Write + Send + Sync + 'static>(
        &mut self,
        _req: &TransferRequest,
        _progress_updater: ProgressUpdater<W>,
    ) -> Result<std::path::PathBuf> {
        unimplemented!()
    }

    async fn terminate(&mut self) -> Result<()> {
        Ok(())
    }
}

struct XetProgressUpdaterWrapper<W: Write + Send + Sync + 'static> {
    updater: ProgressUpdater<W>,
}

#[async_trait]
impl<W: Write + Send + Sync + 'static> TrackingProgressUpdater for XetProgressUpdaterWrapper<W> {
    async fn register_updates(&self, updates: ProgressUpdate) {
        let _ = self.updater.update_bytes_so_far(updates.total_bytes_completed);
    }
}
