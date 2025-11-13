use std::sync::Arc;

use async_trait::async_trait;
use hub_client::{CredentialHelper, Operation};
use reqwest::header;
use reqwest_middleware::RequestBuilder;
use serde::{Deserialize, Serialize};

use crate::errors::{GitXetError, Result};
use crate::git_repo::GitRepo;
use crate::git_url::GitUrl;
use crate::utils::process_wrapping::run_program_captured_with_input_and_output;
use crate::utils::ssh_connect::{SSHMetadata, get_sshcmd_and_args};

#[derive(Deserialize, Serialize)]
pub struct GitLFSAuthentationResponseHeader {
    #[serde(rename = "Authorization")]
    pub authorization: String,
}

// This struct represents the JSON format of the `git-lfs-authenticate` command response over an
// SSH channel to the remote Git server. For details see `crate::auth.rs`.
#[derive(Deserialize, Serialize)]
#[allow(unused)]
pub struct GitLFSAuthenticateResponse {
    pub header: GitLFSAuthentationResponseHeader,
    pub href: String,
    pub expires_in: u32,
}

// This credential helper calls a remote command `git-lfs-authenticate` over an SSH channel
// to the remote Git server.
// We can't cache the authorization token from ssh authentication because
// it has a shorter TTL than that of a Xet CAS JWT.
pub struct SSHCredentialHelper {
    remote_url: GitUrl,
    repo: GitRepo,
    operation: Operation,
}

impl SSHCredentialHelper {
    pub fn new(remote_url: &GitUrl, repo: &GitRepo, operation: Operation) -> Arc<Self> {
        Arc::new(Self {
            remote_url: remote_url.clone(),
            repo: repo.clone(),
            operation,
        })
    }

    async fn authenticate(&self) -> Result<GitLFSAuthenticateResponse> {
        let meta = SSHMetadata {
            user_and_host: self.remote_url.user_and_host()?,
            port: self.remote_url.port(),
            arg_list: vec![
                "git-lfs-authenticate".into(),
                self.remote_url.full_repo_path(),
                self.operation.as_str().into(),
            ],
        };

        // Access to "ssh" and "sh" is provided by the `git` -> `git-lfs` -> `git-xet`, see
        // git_xet/tests/test_ssh.rs for details.
        let (program, args) = get_sshcmd_and_args(&meta, &self.repo)?;

        let (output, _err) =
            run_program_captured_with_input_and_output(program, self.repo.git_path()?, args)?.wait_with_output()?;

        let response: GitLFSAuthenticateResponse = serde_json::from_slice(&output).map_err(GitXetError::internal)?;

        Ok(response)
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
    use crate::git_repo::GitRepo;
    use crate::git_url::GitUrl;
    use crate::test_utils::TestRepo;

    #[tokio::test]
    #[ignore = "need ssh server"]
    async fn test_ssh_cred_helper_local() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let repo = GitRepo::open(test_repo.path())?;
        let remote_url = "ssh://git@localhost:2222/datasets/test/td";
        let parsed_url: GitUrl = remote_url.parse()?;
        let ssh_helper = SSHCredentialHelper::new(&parsed_url, &repo, Operation::Download);

        let response = ssh_helper.authenticate().await?;

        assert!(response.header.authorization.starts_with("Basic"));
        assert_eq!(response.href, "http://localhost:5564/datasets/test/td.git/info/lfs");
        assert!(response.expires_in > 0);

        Ok(())
    }

    #[tokio::test]
    #[ignore = "need ssh key"]
    async fn test_ssh_cred_helper_remote() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let repo = GitRepo::open(test_repo.path())?;
        let remote_url = "ssh://git@hf.co/seanses/tm"; // it seems that ssh port is not open on "huggingface.co"
        let parsed_url: GitUrl = remote_url.parse()?;
        let ssh_helper = SSHCredentialHelper::new(&parsed_url, &repo, Operation::Upload);

        let response = ssh_helper.authenticate().await?;

        assert!(response.header.authorization.starts_with("Basic"));
        assert_eq!(response.href, "https://huggingface.co/seanses/tm.git/info/lfs");
        assert!(response.expires_in > 0);

        Ok(())
    }
}
