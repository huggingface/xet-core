use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use reqwest::header;
use reqwest_middleware::RequestBuilder;
use serde::{Deserialize, Serialize};
use tracing::debug;
use xet_client::error::ClientError;
use xet_client::hub_client::{CredentialHelper, Operation};

use super::Interaction;
use crate::errors::{GitExtensionError, Result};
use crate::git_repo::GitRepo;
use crate::git_url::GitUrl;
use crate::utils::process_wrapping::{check_command_output, run_program_captured_with_input_and_output};
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

/// How long a non-interactive lookup waits for `git-lfs-authenticate` before giving up. `ConnectTimeout` in batch
/// mode bounds only the connection and key exchange; this also bounds a stalled remote command or a key waiting for
/// agent confirmation or a hardware touch.
const NON_INTERACTIVE_TIMEOUT: Duration = Duration::from_secs(5);

// This credential helper calls a remote command `git-lfs-authenticate` over an SSH channel
// to the remote Git server.
//
// The returned authorization is cached in memory until the `expires_in` the server reports, so CAS token refreshes do
// not reconnect. Concurrent requests wait for one SSH call. The authorization is user-scoped; it is never
// written to disk or logged.
pub struct SSHCredentialHelper {
    remote_url: GitUrl,
    repo: GitRepo,
    operation: Operation,
    interaction: Interaction,
    cached: Mutex<Option<CachedAuthorization>>,
}

enum CachedAuthorization {
    Valid {
        authorization: String,
        expires_at: Instant,
    },
    /// A non-interactive lookup failed; later requests go without credentials instead of waiting on SSH again.
    Unavailable,
}

impl SSHCredentialHelper {
    /// With [`Interaction::Never`], SSH runs in batch mode, so it never prompts for a passphrase or
    /// host key, and is killed if it does not answer within `NON_INTERACTIVE_TIMEOUT`. A failed
    /// authentication sends the request without credentials instead of failing it, and so do all later
    /// requests through this helper. The server then treats them as anonymous, which suffices for
    /// public repositories.
    pub fn new(remote_url: &GitUrl, repo: &GitRepo, operation: Operation, interaction: Interaction) -> Arc<Self> {
        Arc::new(Self {
            remote_url: remote_url.clone(),
            repo: repo.clone(),
            operation,
            interaction,
            cached: Mutex::new(None),
        })
    }

    /// Returns the authorization header value, or `Ok(None)` if a non-interactive lookup failed and the
    /// request should go without credentials.
    fn authorization(&self) -> Result<Option<String>> {
        // Held across the SSH call so concurrent requests reuse one result. No await happens under it.
        let mut cached = self
            .cached
            .lock()
            .map_err(|_| GitExtensionError::internal("SSH cache lock poisoned"))?;
        match cached.as_ref() {
            Some(CachedAuthorization::Valid {
                authorization,
                expires_at,
            }) if Instant::now() < *expires_at => return Ok(Some(authorization.clone())),
            Some(CachedAuthorization::Unavailable) => return Ok(None),
            _ => {},
        }
        let response = match self.authenticate() {
            Ok(response) => response,
            Err(error) if matches!(self.interaction, Interaction::Never) => {
                debug!(%error, "SSH authentication unavailable; sending requests without credentials");
                *cached = Some(CachedAuthorization::Unavailable);
                return Ok(None);
            },
            Err(error) => return Err(error),
        };
        *cached = Some(CachedAuthorization::Valid {
            authorization: response.header.authorization.clone(),
            expires_at: Instant::now() + Duration::from_secs(response.expires_in.into()),
        });
        Ok(Some(response.header.authorization))
    }

    fn authenticate(&self) -> Result<GitLFSAuthenticateResponse> {
        let meta = SSHMetadata {
            user_and_host: self.remote_url.user_and_host()?,
            port: self.remote_url.port(),
            arg_list: vec![
                "git-lfs-authenticate".into(),
                self.remote_url.full_repo_path(),
                self.operation.as_str().into(),
            ],
            batch: matches!(self.interaction, Interaction::Never),
        };

        // On Windows, access to "ssh" and "sh" is provided by the `git` -> `git-lfs` -> `git-xet` call chain, see
        // git_xet/tests/test_ssh.rs for details.
        let (program, args) = get_sshcmd_and_args(&meta, &self.repo)?;

        let command = run_program_captured_with_input_and_output(program, self.repo.git_path()?, args)?;
        let (output, _err) = match self.interaction {
            Interaction::Allowed => command.wait_with_output()?,
            Interaction::Never => {
                let output = command.wait_with_status_timeout(NON_INTERACTIVE_TIMEOUT)?.ok_or_else(|| {
                    GitExtensionError::internal(format!(
                        "git-lfs-authenticate did not answer within {NON_INTERACTIVE_TIMEOUT:?}"
                    ))
                })?;
                check_command_output(output)?
            },
        };

        let response: GitLFSAuthenticateResponse =
            serde_json::from_slice(&output).map_err(GitExtensionError::internal)?;

        Ok(response)
    }
}

#[async_trait]
impl CredentialHelper for SSHCredentialHelper {
    async fn fill_credential(&self, req: RequestBuilder) -> std::result::Result<RequestBuilder, ClientError> {
        match self.authorization().map_err(ClientError::credential_helper_error)? {
            Some(authorization) => Ok(req.header(header::AUTHORIZATION, authorization)),
            None => Ok(req),
        }
    }

    fn whoami(&self) -> &str {
        "ssh"
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use xet_client::hub_client::{CredentialHelper, Operation};

    use super::SSHCredentialHelper;
    use crate::auth::Interaction;
    use crate::git_repo::GitRepo;
    use crate::git_url::GitUrl;
    use crate::test_utils::TestRepo;

    // A fake `GIT_SSH` program: records its arguments, then answers like `git-lfs-authenticate`
    // with the `expires_in` in `<dir>/expires`, fails if that file holds `fail`, or hangs if it holds `hang`.
    #[cfg(unix)]
    fn fake_ssh(dir: &std::path::Path) -> anyhow::Result<xet_runtime::utils::EnvVarGuard> {
        use std::os::unix::fs::PermissionsExt;

        let script = dir.join("fake-ssh.sh");
        let d = dir.display();
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\necho \"$@\" >> '{d}/calls'\nexpires=$(cat '{d}/expires')\n[ \"$expires\" = fail ] && exit 1\n[ \"$expires\" = hang ] && exec sleep 30\n\
                 printf '{{\"header\":{{\"Authorization\":\"Basic abc\"}},\"href\":\"h\",\"expires_in\":%s}}' \"$expires\"\n"
            ),
        )?;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))?;
        Ok(xet_runtime::utils::EnvVarGuard::set("GIT_SSH", &script))
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial_test::serial(env_var_write_read)]
    async fn authorization_is_cached_until_near_expiry_and_batch_fails_open() -> Result<()> {
        use crate::test_utils::TestRepo;

        let test_repo = TestRepo::new("main")?;
        let dir = test_repo.path();
        let _ssh = fake_ssh(dir)?;
        let repo = GitRepo::open(dir)?;
        let url: GitUrl = "ssh://git@hf.co/datasets/org/repo".parse()?;
        let calls = || {
            std::fs::read_to_string(dir.join("calls"))
                .map(|c| c.lines().count())
                .unwrap_or(0)
        };

        // An hour-long authorization is fetched once.
        std::fs::write(dir.join("expires"), "3480")?;
        let helper = SSHCredentialHelper::new(&url, &repo, Operation::Download, Interaction::Never);
        for _ in 0..3 {
            assert_eq!(helper.authorization()?.as_deref(), Some("Basic abc"));
        }
        assert_eq!(calls(), 1);
        assert!(std::fs::read_to_string(dir.join("calls"))?.contains("BatchMode=yes"));

        // An already expired one is fetched every time.
        std::fs::write(dir.join("expires"), "0")?;
        let helper = SSHCredentialHelper::new(&url, &repo, Operation::Download, Interaction::Allowed);
        helper.authorization()?;
        helper.authorization()?;
        assert_eq!(calls(), 3);
        assert!(
            !std::fs::read_to_string(dir.join("calls"))?
                .lines()
                .last()
                .unwrap()
                .contains("BatchMode")
        );

        // Without SSH access, batch mode sends no credentials, and doesn't retry SSH for later requests; interactive
        // mode fails.
        std::fs::write(dir.join("expires"), "fail")?;
        let client = reqwest_middleware::ClientWithMiddleware::from(reqwest::Client::new());
        let never = SSHCredentialHelper::new(&url, &repo, Operation::Download, Interaction::Never);
        for _ in 0..2 {
            let request = never.fill_credential(client.get("http://localhost/")).await?.build()?;
            assert!(request.headers().get(reqwest::header::AUTHORIZATION).is_none());
        }
        assert_eq!(calls(), 4);
        let allowed = SSHCredentialHelper::new(&url, &repo, Operation::Download, Interaction::Allowed);
        assert!(allowed.fill_credential(client.get("http://localhost/")).await.is_err());
        Ok(())
    }

    // `ConnectTimeout` doesn't cover a remote command that stalls after connecting, so batch mode must bound the whole
    // call itself rather than block the Git filter waiting on it.
    #[cfg(unix)]
    #[tokio::test]
    #[serial_test::serial(env_var_write_read)]
    async fn batch_gives_up_on_a_stalled_ssh_call() -> Result<()> {
        use std::time::{Duration, Instant};

        let test_repo = TestRepo::new("main")?;
        let dir = test_repo.path();
        let _ssh = fake_ssh(dir)?;
        std::fs::write(dir.join("expires"), "hang")?;
        let repo = GitRepo::open(dir)?;
        let url: GitUrl = "ssh://git@hf.co/datasets/org/repo".parse()?;
        let client = reqwest_middleware::ClientWithMiddleware::from(reqwest::Client::new());
        let never = SSHCredentialHelper::new(&url, &repo, Operation::Download, Interaction::Never);

        let start = Instant::now();
        let request = never.fill_credential(client.get("http://localhost/")).await?.build()?;
        assert!(request.headers().get(reqwest::header::AUTHORIZATION).is_none());
        assert!(start.elapsed() < Duration::from_secs(10));

        // The failure is remembered, so a later request doesn't wait again.
        let start = Instant::now();
        never.fill_credential(client.get("http://localhost/")).await?;
        assert!(start.elapsed() < Duration::from_secs(1));
        Ok(())
    }

    #[tokio::test]
    #[ignore = "need ssh server"]
    async fn test_ssh_cred_helper_local() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let repo = GitRepo::open(test_repo.path())?;
        let remote_url = "ssh://git@localhost:2222/datasets/test/td";
        let parsed_url: GitUrl = remote_url.parse()?;
        let ssh_helper = SSHCredentialHelper::new(&parsed_url, &repo, Operation::Download, Interaction::Allowed);

        let response = ssh_helper.authenticate()?;

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
        let ssh_helper = SSHCredentialHelper::new(&parsed_url, &repo, Operation::Upload, Interaction::Allowed);

        let response = ssh_helper.authenticate()?;

        assert!(response.header.authorization.starts_with("Basic"));
        assert_eq!(response.href, "https://huggingface.co/seanses/tm.git/info/lfs");
        assert!(response.expires_in > 0);

        Ok(())
    }
}
