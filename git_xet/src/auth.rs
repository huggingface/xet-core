use std::io::{BufRead, BufReader, Cursor, Write};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use netrc::Netrc;
use openssh::{KnownHosts, Session};
use reqwest::header;
use reqwest_middleware::RequestBuilder;
use serde::Deserialize;

use crate::constants::HF_TOKEN_ENV;
use crate::errors::*;
use crate::git_process_wrapping::run_git_captured_with_input_and_output;
use crate::git_repo::GitRepo;
use crate::git_url::{GitUrl, Scheme};

// The lfs.<url>.access configuration.
// If set to "basic" then credentials will be requested before making batch requests to this url,
// otherwise a public request will initially be attempted.
// If set to "none" then credentials are not needed. For this case we don't want to prompt the user
// for any crendential.
#[derive(Debug, PartialEq)]
pub enum AccessMode {
    None,
    Basic,
    Private,
    Negotiate,
    Empty,
}

impl FromStr for AccessMode {
    type Err = GitXetError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "none" => Ok(AccessMode::None),
            "basic" => Ok(AccessMode::Basic),
            "private" => Ok(AccessMode::Private),
            "negotiate" => Ok(AccessMode::Negotiate),
            "" => Ok(AccessMode::Empty),
            _ => Err(config_error(format!("invalid \"lfs.<url>.access\" type: {s}"))),
        }
    }
}

impl AccessMode {
    pub fn from_repo_and_remote_url(repo: &GitRepo, remote_url: &GitUrl) -> Result<Self> {
        let lfs_server = remote_url.to_default_lfs_endpoint()?;
        let repo_config = repo.config()?;
        let access_config = repo_config.get_str(&format!("lfs.{lfs_server}.access")).unwrap_or_default();
        Self::from_str(access_config)
    }
}

#[derive(Clone, Copy)]
#[allow(unused)]
pub enum Operation {
    Upload,
    Download,
}

impl Operation {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::Download => "download",
        }
    }

    pub fn token_type(&self) -> &'static str {
        match self {
            Self::Upload => "write",
            Self::Download => "read",
        }
    }
}

#[async_trait]
pub trait CredentialHelper: Send + Sync {
    async fn fill_creds(&self, req: RequestBuilder) -> Result<RequestBuilder>;
    #[cfg(test)]
    fn whoami(&self) -> &str;
}

struct NoopCredentialHelper {}

impl NoopCredentialHelper {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl CredentialHelper for NoopCredentialHelper {
    async fn fill_creds(&self, req: RequestBuilder) -> Result<RequestBuilder> {
        Ok(req)
    }

    #[cfg(test)]
    fn whoami(&self) -> &str {
        "noop"
    }
}

struct BasicJWTCredentialHelper {
    user: String,
    token: String,

    _whoami: &'static str,
}

impl BasicJWTCredentialHelper {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(user: String, token: String, whoami: &'static str) -> Arc<Self> {
        Arc::new(Self {
            user,
            token,
            _whoami: whoami,
        })
    }
}

#[async_trait]
impl CredentialHelper for BasicJWTCredentialHelper {
    async fn fill_creds(&self, req: RequestBuilder) -> Result<RequestBuilder> {
        Ok(req.basic_auth(self.user.clone(), Some(self.token.clone())))
    }

    #[cfg(test)]
    fn whoami(&self) -> &str {
        self._whoami
    }
}

struct BearerCredentialHelper {
    hf_token: String,

    _whoami: &'static str,
}

impl BearerCredentialHelper {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(hf_token: String, whoami: &'static str) -> Arc<Self> {
        Arc::new(Self {
            hf_token,
            _whoami: whoami,
        })
    }
}

#[async_trait]
impl CredentialHelper for BearerCredentialHelper {
    async fn fill_creds(&self, req: RequestBuilder) -> Result<RequestBuilder> {
        Ok(req.bearer_auth(&self.hf_token))
    }

    #[cfg(test)]
    fn whoami(&self) -> &str {
        self._whoami
    }
}

#[derive(Deserialize)]
struct GLFSARHeader {
    #[serde(rename = "Authorization")]
    authorization: String,
}

#[derive(Deserialize)]
#[allow(unused)]
struct GitLFSAuthenticateResponse {
    header: GLFSARHeader,
    href: String,
    expires_in: u32,
}

// We can't cache the authorization token from ssh authentication because
// it has a shorter TTL than that of a CAS JWT.
struct SSHCredentialHelper {
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

    async fn authenticate(&self) -> Result<GitLFSAuthenticateResponse> {
        let host_url = self.remote_url.host_url()?;
        let full_repo_path = self.remote_url.full_repo_path();
        let session = Session::connect(&host_url, KnownHosts::Add).await.map_err(internal)?;

        let output = session
            .command("git-lfs-authenticate")
            .arg(full_repo_path)
            .arg(self.operation.as_str())
            .output()
            .await
            .map_err(internal)?;

        serde_json::from_slice(&output.stdout).map_err(internal)
    }
}

#[async_trait]
impl CredentialHelper for SSHCredentialHelper {
    async fn fill_creds(&self, req: RequestBuilder) -> Result<RequestBuilder> {
        let authenticated = self.authenticate().await?;
        Ok(req.header(header::AUTHORIZATION, authenticated.header.authorization))
    }

    #[cfg(test)]
    fn whoami(&self) -> &str {
        "ssh"
    }
}

struct GitCredentialHelper {}

impl GitCredentialHelper {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(repo_path: impl AsRef<Path>, host_url: &str) -> Result<Arc<BearerCredentialHelper>> {
        let hf_token = Self::authenticate(repo_path.as_ref(), host_url)?;

        Ok(BearerCredentialHelper::new(hf_token, "git"))
    }

    fn authenticate(repo_path: &Path, host_url: &str) -> Result<String> {
        let mut cred_query = run_git_captured_with_input_and_output(repo_path, "credential", &["fill"])?;
        let mut writer = cred_query.stdin()?;
        write!(writer, "url={host_url}\n\n")?;
        drop(writer);

        let (response, _err) = cred_query.wait_with_output()?;

        let reader = BufReader::new(Cursor::new(response));

        for line in reader.lines() {
            let mut line = line?;
            line.retain(|c| !c.is_whitespace());

            if let Some(hf_token) = line.strip_prefix("password=") {
                if !hf_token.is_empty() {
                    return Ok(hf_token.to_owned());
                }
            }
        }

        Err(config_error(format!("failed to find authentication for {host_url}")))
    }
}

// getCreds determines the credential helper to fill authorization headers of a request if possible,
// from the following sources:
//
// 1. If access mode is "none", credential helper doesn't do anything and we don't prompt the user for
//  any credentials.
// 2. URL authentication on the Endpoint URL or the Git Remote URL.
// 3. HF token set by environment variable "HF_TOKEN".
// 4. Netrc based on the hostname.
// 5. If the Git remote URL has SSH scheme, use the SSHCredentialHelper.
// 6. Git Credential Helper, potentially prompting the user.
//
// There are two URLs in play, that make this a little confusing.
//
//  1. The LFS API URL, which should be something like "https://git.com/repo.git/info/lfs" This URL used for the
//     "lfs.URL.access" git config key, which determines what kind of auth the LFS server expects. Could be BasicAccess,
//     NegotiateAccess, or NoneAccess, in which the Git Credential Helper step is skipped. We do not want to prompt the
//     user for a password to fetch public repository data.
//  2. The Git Remote URL, which should be something like "https://git.com/repo.git" This URL is used for the Git
//     Credential Helper. This way existing https Git remote credentials can be re-used for LFS.
pub fn get_creds(repo: &GitRepo, remote_url: &GitUrl, operation: Operation) -> Result<Arc<dyn CredentialHelper>> {
    let access = AccessMode::from_repo_and_remote_url(repo, remote_url)?;
    let derived_host_url = remote_url.to_derived_http_host_url()?;

    // 1. check access mode
    if access == AccessMode::None {
        return Ok(NoopCredentialHelper::new());
    }

    // 2. check embedded authentication
    let credential = remote_url.credential();
    match credential {
        (Some(user), Some(token)) => return Ok(BasicJWTCredentialHelper::new(user, token, "url")),
        _ => (), // valid only when both user and token exist
    }

    // 3. check credential from environment
    if let Ok(token) = std::env::var(HF_TOKEN_ENV) {
        return Ok(BearerCredentialHelper::new(token, "env"));
    }

    // 4. check netrc file
    if let Ok(nrc) = Netrc::new() {
        let derived_host_name = derived_host_url.split("://").last();
        if let Some(host_name) = derived_host_name {
            for (host, auth) in nrc.hosts {
                if host.eq(host_name) {
                    return Ok(BasicJWTCredentialHelper::new(auth.login, auth.password, "netrc"));
                }
            }
        }
    }

    // 5. check remote URL scheme
    if matches!(remote_url.scheme(), Scheme::Ssh | Scheme::GitSsh) {
        return Ok(SSHCredentialHelper::new(remote_url, operation));
    }

    // 6. check Git credential helper
    Ok(GitCredentialHelper::new(repo.git_path()?, &derived_host_url)?)
}

#[cfg(test)]
mod test_access_mode {
    use anyhow::Result;

    use super::*;
    use crate::test_utils::test_repo::TestRepo;

    #[test]
    fn test_get_access() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let remote_url = "https://localhost/test/aaa.git";
        let remote_lfs_server = format!("{remote_url}/info/lfs");
        let config_key = format!("lfs.{remote_lfs_server}.access");
        let config_val = "basic";

        // 1. when no key is set
        let repo = GitRepo::open(&test_repo.repo_path)?;
        let remote_url: GitUrl = remote_url.parse()?;
        let access = AccessMode::from_repo_and_remote_url(&repo, &remote_url)?;
        assert_eq!(access, AccessMode::Empty);

        // 2. when key is set
        test_repo.set_config(&config_key, config_val)?;
        let access = AccessMode::from_repo_and_remote_url(&repo, &remote_url)?;
        assert_eq!(access, AccessMode::Basic);

        Ok(())
    }
}

#[cfg(test)]
mod test_cred_helpers {
    use anyhow::Result;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::auth::SSHCredentialHelper;
    use crate::git_url::GitUrl;
    use crate::test_utils::test_repo::TestRepo;

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

    #[test]
    fn test_git_cred_helper() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let creds_file = NamedTempFile::new()?;
        let creds_file_path = std::path::absolute(creds_file.path())?;
        let host_url = "http://localhost:1234";
        let username = "user";
        let password = "secr3t";

        // 1. set credential helper to store with a local file
        test_repo.set_config("credential.helper", &format!("store --file={}", creds_file_path.to_str().unwrap()))?;

        // 2. store a credential
        let mut cred_store = run_git_captured_with_input_and_output(&test_repo.repo_path, "credential", &["approve"])?;
        let mut writer = cred_store.stdin()?;
        write!(writer, "url={host_url}\nusername={username}\npassword={password}\n\n")?;
        drop(writer);
        cred_store.wait()?;

        // 3. test git credential helper
        let remote_url = format!("{host_url}/datasets/test/td");
        let parsed_url: GitUrl = remote_url.parse()?;
        let host_url = parsed_url.host_url()?;

        let git_cred_helper = GitCredentialHelper::new(&test_repo.repo_path, &host_url)?;
        assert_eq!(git_cred_helper.hf_token, password);

        Ok(())
    }

    #[test]
    #[ignore = "need manual interaction"]
    fn test_cred_helper_selection_error() -> Result<()> {
        // Test get error when failed to locate any credential source.
        // Make sure GIT_ASKPASS is not set and GIT_TERMINAL_PROMPT is not set or not set to 0.

        let test_repo = TestRepo::new("main")?;

        // 1. set http remote url
        test_repo.set_remote("origin", &format!("https://server.co/datasets/user/repo"))?;

        // 2. test
        let repo = GitRepo::open(&test_repo.repo_path)?;
        let remote_url = repo.remote_url()?;
        let operation = Operation::Upload;

        let cred_helper = get_creds(&repo, &remote_url, operation);
        // press ^D
        assert!(cred_helper.is_err());

        Ok(())
    }

    #[test]
    fn test_cred_helper_selection_git() -> Result<()> {
        // Test get GitCredentialHelper when a credential is cached in git credential helper.

        let test_repo = TestRepo::new("main")?;

        // 1. set http remote url
        test_repo.set_remote("origin", &format!("https://huggingface.co/datasets/user/repo"))?;

        // 2. set credential helper to store with a local file
        test_repo.set_config("credential.helper", "store --file=creds")?;

        // 3. store a credential
        let mut cred_store = run_git_captured_with_input_and_output(&test_repo.repo_path, "credential", &["approve"])?;
        let mut writer = cred_store.stdin()?;
        write!(writer, "protocol=https\nhost=huggingface.co\nusername=user\npassword=secr3t\n\n")?;
        drop(writer);
        cred_store.wait()?;

        // 4. test
        let repo = GitRepo::open(&test_repo.repo_path)?;
        let remote_url = repo.remote_url()?;
        let operation = Operation::Upload;

        let cred_helper = get_creds(&repo, &remote_url, operation)?;
        assert_eq!(cred_helper.whoami(), "git");

        Ok(())
    }
}
