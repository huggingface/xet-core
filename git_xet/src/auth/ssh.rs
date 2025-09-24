use std::sync::Arc;

use async_trait::async_trait;
use hub_client::{CredentialHelper, HubClientError, Operation, Result};
#[cfg(unix)]
use openssh::{KnownHosts, Session};
use reqwest::header;
use reqwest_middleware::RequestBuilder;
use serde::Deserialize;

use crate::git_url::GitUrl;

#[derive(Deserialize)]
struct GitLFSAuthentationResponseHeader {
    #[serde(rename = "Authorization")]
    authorization: String,
}

// This struct represents the JSON format of the `git-lfs-authenticate` command response over an
// SSH channel to the remote Git server. For details see `crate::auth.rs`.
#[derive(Deserialize)]
#[allow(unused)]
struct GitLFSAuthenticateResponse {
    header: GitLFSAuthentationResponseHeader,
    href: String,
    expires_in: u32,
}

// This credential helper calls a remote command `git-lfs-authenticate` over an SSH channel
// to the remote Git server.
// We can't cache the authorization token from ssh authentication because
// it has a shorter TTL than that of a Xet CAS JWT.
pub struct SSHCredentialHelper {
    remote_url: GitUrl,
    operation: Operation,
}

impl SSHCredentialHelper {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(remote_url: &GitUrl, operation: Operation) -> Arc<Self> {
        Arc::new(Self {
            remote_url: remote_url.clone(),
            operation,
        })
    }

    #[cfg(unix)]
    async fn authenticate(&self) -> Result<GitLFSAuthenticateResponse> {
        let host_url = self.remote_url.host_url().map_err(HubClientError::credential_helper_error)?;
        let full_repo_path = self.remote_url.full_repo_path();
        let session = Session::connect(&host_url, KnownHosts::Add)
            .await
            .map_err(HubClientError::credential_helper_error)?;

        let output = session
            .command("git-lfs-authenticate")
            .arg(full_repo_path)
            .arg(self.operation.as_str())
            .output()
            .await
            .map_err(HubClientError::credential_helper_error)?;

        serde_json::from_slice(&output.stdout).map_err(HubClientError::credential_helper_error)
    }

    #[cfg(not(unix))]
    async fn authenticate(&self) -> Result<GitLFSAuthenticateResponse> {
        unimplemented!()
    }
}

#[async_trait]
impl CredentialHelper for SSHCredentialHelper {
    async fn fill_credential(&self, req: RequestBuilder) -> anyhow::Result<RequestBuilder> {
        let authenticated = self.authenticate().await?;
        Ok(req.header(header::AUTHORIZATION, authenticated.header.authorization))
    }

    fn whoami(&self) -> &str {
        "ssh"
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use hub_client::Operation;

    use super::SSHCredentialHelper;
    use crate::git_url::GitUrl;

    #[tokio::test]
    #[ignore = "need ssh server"]
    async fn test_ssh_cred_helper_local() -> Result<()> {
        let remote_url = "ssh://git@localhost:2222/datasets/test/td";
        let parsed_url: GitUrl = remote_url.parse()?;
        let ssh_helper = SSHCredentialHelper::new(&parsed_url, Operation::Download);

        let response = ssh_helper.authenticate().await?;

        assert!(response.header.authorization.starts_with("Basic"));
        assert_eq!(response.href, "http://localhost:5564/datasets/test/td.git/info/lfs");
        assert!(response.expires_in > 0);

        Ok(())
    }

    #[tokio::test]
    #[ignore = "need ssh key"]
    async fn test_ssh_cred_helper_remote() -> Result<()> {
        let remote_url = "ssh://git@hf.co/seanses/tm"; // it seems that ssh port is not open on "huggingface.co"
        let parsed_url: GitUrl = remote_url.parse()?;
        let ssh_helper = SSHCredentialHelper::new(&parsed_url, Operation::Upload);

        let response = ssh_helper.authenticate().await?;

        assert!(response.header.authorization.starts_with("Basic"));
        assert_eq!(response.href, "https://huggingface.co/seanses/tm.git/info/lfs");
        assert!(response.expires_in > 0);

        Ok(())
    }
}
