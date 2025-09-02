use std::str::FromStr;
use std::sync::Arc;

use hub_client::{BasicJWTCredentialHelper, BearerCredentialHelper, CredentialHelper, NoopCredentialHelper, Operation};
use netrc::Netrc;

use crate::constants::HF_TOKEN_ENV;
use crate::errors::*;
use crate::git_repo::GitRepo;
use crate::git_url::{GitUrl, Scheme};

mod git;
mod ssh;

use git::GitCredentialHelper;
use ssh::SSHCredentialHelper;

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
        match s.to_ascii_lowercase().as_str() {
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

// Determines the credential helper to fill authorization headers of a request if possible,
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
pub fn get_credential(repo: &GitRepo, remote_url: &GitUrl, operation: Operation) -> Result<Arc<dyn CredentialHelper>> {
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
        #[cfg(unix)]
        return Ok(SSHCredentialHelper::new(remote_url, operation));
        #[cfg(not(unix))]
        return Err(not_supported(format!(
            "using {GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM} in a repository with SSH Git URL is under development; if you think this is an error, 
            consider upgrade {GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM} or contact Xet Team at Hugging Face."
        )));
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
    use std::io::Write;

    use anyhow::Result;
    use serial_test::serial;
    use tempfile::NamedTempFile;
    use utils::EnvVarGuard;

    use super::*;
    use crate::git_process_wrapping::run_git_captured_with_input_and_output;
    use crate::test_utils::test_repo::TestRepo;

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

        let cred_helper = get_credential(&repo, &remote_url, operation);
        // press ^D
        assert!(cred_helper.is_err());

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_cred_helper_selection_git() -> Result<()> {
        // Test get GitCredentialHelper when a credential is cached in git credential helper.
        let test_repo = TestRepo::new("main")?;
        let creds_file = NamedTempFile::new()?;
        let creds_file_path = std::path::absolute(creds_file.path())?;

        // 1. set http remote url
        test_repo.set_remote("origin", "https://huggingface.co/datasets/user/repo")?;

        // 2. set credential helper to store with a local file
        test_repo.set_config("credential.helper", &format!("store --file={}", creds_file_path.to_str().unwrap()))?;

        // 3. store a credential
        let mut cred_store = run_git_captured_with_input_and_output(&test_repo.repo_path, "credential", &["approve"])?;
        let mut writer = cred_store.stdin()?;
        write!(writer, "url=https://huggingface.co\nusername=user\npassword=secr3t\n\n")?;
        drop(writer);
        cred_store.wait()?;

        // 4. test
        let repo = GitRepo::open(&test_repo.repo_path)?;
        let remote_url = repo.remote_url()?;
        let operation = Operation::Upload;

        let cred_helper = get_credential(&repo, &remote_url, operation)?;
        assert_eq!(cred_helper.whoami(), "git");

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_cred_helper_selection_ssh() -> Result<()> {
        // Test get SSHCredentialHelper when the Git remote URL has SSH scheme.
        let test_repo = TestRepo::new("main")?;

        // 1. set ssh remote url
        test_repo.set_remote("origin", "git@hf.co:user/model-A")?;

        // 2. test
        let repo = GitRepo::open(&test_repo.repo_path)?;
        let remote_url = repo.remote_url()?;
        let operation = Operation::Upload;

        let cred_helper = get_credential(&repo, &remote_url, operation)?;
        assert_eq!(cred_helper.whoami(), "ssh");

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_cred_helper_selection_netrc() -> Result<()> {
        // Test get credential from a Netrc configuration when there's a host match.
        let test_repo = TestRepo::new("main")?;
        let netrc_file = NamedTempFile::new()?;
        let netrc_file_path = netrc_file.path();

        // 1. set http remote url
        test_repo.set_remote("origin", "https://huggingface.co/user/repo")?;

        // 2. store a credential in a Netrc file.
        let _env_guard = EnvVarGuard::set("NETRC", netrc_file_path);
        std::fs::write(netrc_file_path, "machine huggingface.co login user password secr3t")?;

        // 3. test
        let repo = GitRepo::open(&test_repo.repo_path)?;
        let remote_url = repo.remote_url()?;
        let operation = Operation::Upload;

        let cred_helper = get_credential(&repo, &remote_url, operation)?;
        assert_eq!(cred_helper.whoami(), "netrc");

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_cred_helper_selection_env() -> Result<()> {
        // Test get credential from env var "HF_TOKEN".
        let test_repo = TestRepo::new("main")?;

        // 1. set http remote url
        test_repo.set_remote("origin", "https://huggingface.co/user/repo")?;

        // 2. set env var token
        let _env_guard = EnvVarGuard::set(HF_TOKEN_ENV, "hf_abcde");

        // 3. test
        let repo = GitRepo::open(&test_repo.repo_path)?;
        let remote_url = repo.remote_url()?;
        let operation = Operation::Upload;

        let cred_helper = get_credential(&repo, &remote_url, operation)?;
        assert_eq!(cred_helper.whoami(), "env");

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_cred_helper_selection_url() -> Result<()> {
        // Test get embedded credential from Git remote URL.
        let test_repo = TestRepo::new("main")?;

        // 1. set http remote url
        test_repo.set_remote("origin", "https://user:hf_token@hf.co/user/repo")?;

        // 2. test
        let repo = GitRepo::open(&test_repo.repo_path)?;
        let remote_url = repo.remote_url()?;
        let operation = Operation::Upload;

        let cred_helper = get_credential(&repo, &remote_url, operation)?;
        assert_eq!(cred_helper.whoami(), "url");

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_cred_helper_selection_noop() -> Result<()> {
        // Test get NoopCredentialHelper when there's no need for credential.
        let test_repo = TestRepo::new("main")?;

        // 1. set http remote url
        test_repo.set_remote("origin", "https://huggingface.co/user/repo")?;

        // 2. set access mode to "none"
        test_repo.set_config(&format!("lfs.{}.access", "https://huggingface.co/user/repo.git/info/lfs"), "none")?;

        // 3. test
        let repo = GitRepo::open(&test_repo.repo_path)?;
        let remote_url = repo.remote_url()?;
        let operation = Operation::Upload;

        let cred_helper = get_credential(&repo, &remote_url, operation)?;
        assert_eq!(cred_helper.whoami(), "noop");

        Ok(())
    }
}
