//! Chooses the credential for Hub requests made on behalf of a repository.
//!
//! The credential is the identity Git itself uses for the remote, so uploads and `git push` act as
//! the same user. An SSH remote always uses SSH authentication (`git-lfs-authenticate`), as Git LFS
//! does. For an HTTP(S) remote, the first available source wins:
//!
//! 1. a token embedded in the remote URL (`https://user:token@host/...`);
//! 2. `.netrc`, matched by the remote's hostname, which Git also reads;
//! 3. Git's credential helpers. With [`Interaction::Allowed`], Git may prompt.
//!
//! [`Interaction::Allowed`] (pre-push) fails if none of these yields a credential.
//!
//! [`Interaction::Never`] (the clean filter) never prompts: SSH runs in batch mode, and helpers return
//! only stored credentials. A credential there improves deduplication but isn't required, so if an
//! HTTP(S) remote has none of the above, it falls back to:
//!
//! 4. the `HF_TOKEN` environment variable, or the legacy `HUGGING_FACE_HUB_TOKEN`;
//! 5. the Hugging Face token file written by `hf auth login`;
//!
//! and otherwise makes anonymous requests, which are enough to read public repositories. Steps 4 and 5
//! aren't tied to a host, so they apply only to Hub hosts (`huggingface.co`, `hf.co`, the staging Hub,
//! or the host of `HF_ENDPOINT`); a repository's `.gitattributes` can't send a Hub token elsewhere.
#![cfg_attr(not(test), expect(dead_code, reason = "used by the clean filter and pre-push"))]

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use git_extension_common::{GitCredentialHelper, GitRepo, GitUrl, Interaction, SSHCredentialHelper, Scheme};
use netrc::Netrc;
use url::Url;
use xet_client::error::ClientError;
use xet_client::hub_client::{BearerCredentialHelper, CredentialHelper, NoopCredentialHelper, Operation};

use crate::constants::{
    AUTH_GUIDANCE, HF_ENDPOINT_ENV, HF_HOME_ENV, HF_TOKEN_ENV, HF_TOKEN_PATH_ENV, HUB_HOSTS, LEGACY_HF_TOKEN_ENV,
    SSH_AUTH_GUIDANCE,
};
use crate::{GitHfError, Result};

/// Where a [`ResolvedCredential`] came from, for messages. Never includes the secret itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialSource {
    RemoteUrl,
    /// The environment variable that held the token.
    Environment(&'static str),
    TokenFile(PathBuf),
    Netrc,
    Ssh,
    GitCredentialHelper,
    Anonymous,
}

impl fmt::Display for CredentialSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RemoteUrl => write!(f, "the token in the remote URL"),
            Self::Environment(name) => write!(f, "{name}"),
            Self::TokenFile(path) => write!(f, "the token file {}", path.display()),
            Self::Netrc => write!(f, ".netrc"),
            Self::Ssh => write!(f, "SSH authentication"),
            Self::GitCredentialHelper => write!(f, "the Git credential helper"),
            Self::Anonymous => write!(f, "no credentials"),
        }
    }
}

/// A credential helper for Hub requests and the source it came from.
pub struct ResolvedCredential {
    helper: Arc<dyn CredentialHelper>,
    source: CredentialSource,
}

impl ResolvedCredential {
    pub fn helper(&self) -> Arc<dyn CredentialHelper> {
        self.helper.clone()
    }

    pub fn source(&self) -> &CredentialSource {
        &self.source
    }

    /// Explains a failed Hub token request caused by authentication or authorization, naming the
    /// repository, host, and credential source. Returns `None` for other failures, such as network
    /// errors. Uses the redacted remote URL, so no secret appears in the message.
    pub fn describe_failure(&self, error: &ClientError, remote_url: &GitUrl, operation: Operation) -> Option<String> {
        let status = error.status()?.as_u16();
        let repo = remote_url.full_repo_path();
        let host = remote_url.to_derived_http_host_url().unwrap_or_else(|_| remote_url.redacted());
        let access = match operation {
            Operation::Upload => "write",
            Operation::Download => "read",
        };
        let source = &self.source;
        let guidance = match remote_url.scheme() {
            Scheme::Ssh | Scheme::GitSsh => SSH_AUTH_GUIDANCE,
            _ => AUTH_GUIDANCE,
        };
        Some(match (status, source) {
            (401 | 404, CredentialSource::Anonymous) => {
                format!("{host} requires authentication for {repo}, and none was found; {guidance}")
            },
            (401, _) => format!("{host} rejected {source} for {repo}; {guidance}"),
            (403, _) => format!(
                "{source} lacks {access} access to {repo} on {host}; check the token's permissions{}",
                if matches!(operation, Operation::Upload) {
                    " and the branch's protection rules"
                } else {
                    ""
                }
            ),
            (404, _) => format!("{repo} was not found on {host}, or is not visible with {source}"),
            _ => return None,
        })
    }
}

/// Resolves the credential for Hub requests about `remote_url`'s repository. See the module docs for
/// the order of sources and the meaning of `interaction`.
pub fn resolve_credential(
    repo: &GitRepo,
    remote_url: &GitUrl,
    operation: Operation,
    interaction: Interaction,
) -> Result<ResolvedCredential> {
    let bearer = |token: String, source: CredentialSource, whoami| ResolvedCredential {
        helper: BearerCredentialHelper::new(token, whoami),
        source,
    };

    if matches!(remote_url.scheme(), Scheme::Ssh | Scheme::GitSsh) {
        return Ok(ResolvedCredential {
            helper: SSHCredentialHelper::new(remote_url, repo, operation, interaction),
            source: CredentialSource::Ssh,
        });
    }

    if let (Some(_user), Some(token)) = remote_url.credential() {
        return Ok(bearer(token, CredentialSource::RemoteUrl, "url"));
    }
    let host_url = remote_url.to_derived_http_host_url()?;
    if let Some(token) = netrc_token(&host_url) {
        return Ok(bearer(token, CredentialSource::Netrc, "netrc"));
    }
    if let Some(helper) = GitCredentialHelper::new(repo.git_path()?, &host_url, interaction)? {
        return Ok(ResolvedCredential {
            helper,
            source: CredentialSource::GitCredentialHelper,
        });
    }

    if matches!(interaction, Interaction::Allowed) {
        return Err(GitHfError::Auth(format!(
            "no credentials found for {} on {host_url}; {AUTH_GUIDANCE}",
            remote_url.full_repo_path()
        )));
    }
    if is_hub_host(&host_url) {
        // As in huggingface_hub, a blank `HF_TOKEN` falls through to the legacy name.
        for name in [HF_TOKEN_ENV, LEGACY_HF_TOKEN_ENV] {
            if let Some(token) = std::env::var(name).ok().filter(|token| !token.trim().is_empty()) {
                return Ok(bearer(token.trim().to_owned(), CredentialSource::Environment(name), "env"));
            }
        }
        if let Some(path) = token_file_path()
            && let Some(token) = read_token_file(&path)?
        {
            return Ok(bearer(token, CredentialSource::TokenFile(path), "token-file"));
        }
    }
    Ok(ResolvedCredential {
        helper: NoopCredentialHelper::new(),
        source: CredentialSource::Anonymous,
    })
}

/// The token file `huggingface_hub` uses: `$HF_TOKEN_PATH`, else `$HF_HOME/token`, else
/// `$XDG_CACHE_HOME/huggingface/token`, else `~/.cache/huggingface/token`. Like huggingface_hub, a
/// leading `~` in the resolved `HF_TOKEN_PATH` or `HF_HOME` is expanded; `$VAR` references are not.
fn token_file_path() -> Option<PathBuf> {
    let non_empty = |name| std::env::var_os(name).filter(|value| !value.is_empty()).map(PathBuf::from);
    let expand_tilde = |path: PathBuf| shellexpand::path::tilde(&path).into_owned();
    if let Some(path) = non_empty(HF_TOKEN_PATH_ENV) {
        return Some(expand_tilde(path));
    }
    let hf_home = non_empty(HF_HOME_ENV).or_else(|| {
        non_empty("XDG_CACHE_HOME")
            .or_else(|| dirs::home_dir().map(|home| home.join(".cache")))
            .map(|cache| cache.join("huggingface"))
    })?;
    Some(expand_tilde(hf_home).join("token"))
}

// A missing or blank file means no token; other read errors are reported.
fn read_token_file(path: &Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents.trim().to_owned()).filter(|token| !token.is_empty())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Whether `host_url`, such as `https://hf.co`, names a Hub host: one of [`HUB_HOSTS`], or the host
/// of `HF_ENDPOINT`. As in huggingface_hub, hosts are compared without ports.
fn is_hub_host(host_url: &str) -> bool {
    let Some(host) = hostname(host_url) else {
        return false;
    };
    HUB_HOSTS.contains(&host.as_str())
        || std::env::var(HF_ENDPOINT_ENV).is_ok_and(|endpoint| hostname(endpoint.trim()).as_ref() == Some(&host))
}

/// The host of a URL such as `https://user@host:port/path`, without user or port, lowercased for
/// HTTP(S). `None` if `url` doesn't parse or has no host.
fn hostname(url: &str) -> Option<String> {
    Url::parse(url).ok()?.host_str().map(str::to_owned)
}

// `.netrc` has no port, so like curl (used by Git) and `requests`, match the hostname alone, ignoring case.
fn netrc_token(host_url: &str) -> Option<String> {
    let host = hostname(host_url)?;
    Netrc::new()
        .ok()?
        .hosts
        .into_iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(&host))
        .map(|(_, auth)| auth.password)
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    use git_extension_common::process_wrapping::{run_git_captured, run_git_captured_with_input_and_output};
    use git_extension_common::test_utils::TestRepo;
    use serial_test::serial;
    use xet_client::hub_client::HubClient;
    use xet_runtime::core::XetContext;
    use xet_runtime::utils::EnvVarGuard;

    use super::*;

    const REMOTE: &str = "https://huggingface.co/datasets/org/repo";

    // Guards that keep every source but the one under test empty. TestRepo already isolates HOME
    // and system Git configuration.
    fn isolate(repo: &TestRepo) -> anyhow::Result<Vec<EnvVarGuard>> {
        let guards = vec![
            EnvVarGuard::unset(HF_TOKEN_ENV),
            EnvVarGuard::unset(LEGACY_HF_TOKEN_ENV),
            EnvVarGuard::unset(HF_TOKEN_PATH_ENV),
            EnvVarGuard::unset(HF_HOME_ENV),
            EnvVarGuard::unset(HF_ENDPOINT_ENV),
            EnvVarGuard::unset("XDG_CACHE_HOME"),
            EnvVarGuard::unset("NETRC"),
            EnvVarGuard::unset("GIT_ASKPASS"),
            EnvVarGuard::unset("SSH_ASKPASS"),
            // A test must never reach a real prompt, even when run from a terminal.
            EnvVarGuard::set("GIT_TERMINAL_PROMPT", "0"),
        ];
        // Reset the helper list so no inherited helper (such as a keychain) is consulted.
        run_git_captured(repo.path(), "config", ["credential.helper", ""])?;
        Ok(guards)
    }

    // Configures a store helper with one credential, and verifies it is the only helper.
    fn store_credential(repo: &TestRepo, password: &str) -> anyhow::Result<()> {
        // Git runs the helper through a shell, which would treat Windows backslashes as escapes.
        let store = repo.path().join("credential-store").to_string_lossy().replace('\\', "/");
        let helper = format!("store --file={store}");
        run_git_captured(repo.path(), "config", ["--add", "credential.helper", helper.as_str()])?;
        let (helpers, _) =
            run_git_captured_with_input_and_output(repo.path(), "config", ["--get-all", "credential.helper"])?
                .wait_with_output()?;
        assert_eq!(String::from_utf8(helpers)?, format!("\n{helper}\n"));
        let mut approve = run_git_captured_with_input_and_output(repo.path(), "credential", ["approve"])?;
        write!(approve.stdin()?, "url=https://huggingface.co\nusername=user\npassword={password}\n\n")?;
        approve.wait()?;
        Ok(())
    }

    fn resolve(repo: &TestRepo, url: &str, interaction: Interaction) -> Result<ResolvedCredential> {
        resolve_credential(&GitRepo::open(repo.path())?, &url.parse()?, Operation::Download, interaction)
    }

    #[test]
    #[serial(env_var_write_read)]
    fn sources_in_priority_order() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _isolation = isolate(&repo)?;
        store_credential(&repo, "from-helper")?;
        let netrc = repo.path().join("netrc");
        std::fs::write(&netrc, "machine huggingface.co login user password from-netrc\n")?;
        let _netrc = EnvVarGuard::set("NETRC", &netrc);
        let token_file = repo.path().join("token");
        std::fs::write(&token_file, " from-file \n")?;
        let _token_path = EnvVarGuard::set(HF_TOKEN_PATH_ENV, &token_file);
        let _token = EnvVarGuard::set(HF_TOKEN_ENV, "from-env");

        let url_with_token = "https://user:from-url@huggingface.co/datasets/org/repo";
        assert_eq!(resolve(&repo, url_with_token, Interaction::Never)?.source(), &CredentialSource::RemoteUrl);
        assert_eq!(resolve(&repo, REMOTE, Interaction::Never)?.source(), &CredentialSource::Netrc);
        let _no_netrc = EnvVarGuard::set("NETRC", repo.path().join("missing"));
        // A stored Git credential, which `git push` also uses, comes before the Hub tokens.
        assert_eq!(resolve(&repo, REMOTE, Interaction::Never)?.source(), &CredentialSource::GitCredentialHelper);
        run_git_captured(repo.path(), "config", ["--replace-all", "credential.helper", ""])?;
        assert_eq!(resolve(&repo, REMOTE, Interaction::Never)?.source(), &CredentialSource::Environment(HF_TOKEN_ENV));
        // A blank HF_TOKEN falls through to the legacy name.
        let _blank = EnvVarGuard::set(HF_TOKEN_ENV, " ");
        let _legacy = EnvVarGuard::set(LEGACY_HF_TOKEN_ENV, "from-legacy");
        assert_eq!(
            resolve(&repo, REMOTE, Interaction::Never)?.source(),
            &CredentialSource::Environment(LEGACY_HF_TOKEN_ENV)
        );
        let _no_legacy = EnvVarGuard::unset(LEGACY_HF_TOKEN_ENV);
        assert_eq!(resolve(&repo, REMOTE, Interaction::Never)?.source(), &CredentialSource::TokenFile(token_file));
        let _no_file = EnvVarGuard::unset(HF_TOKEN_PATH_ENV);
        assert_eq!(resolve(&repo, REMOTE, Interaction::Never)?.source(), &CredentialSource::Anonymous);
        Ok(())
    }

    // HF_TOKEN and the token file aren't tied to a host, so a remote on another host, which a
    // repository's own `.gitattributes` can make the clean filter contact, must not receive them.
    #[test]
    #[serial(env_var_write_read)]
    fn host_independent_tokens_only_go_to_hub_hosts() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _isolation = isolate(&repo)?;
        let token_file = repo.path().join("token");
        std::fs::write(&token_file, "from-file\n")?;
        let _token_path = EnvVarGuard::set(HF_TOKEN_PATH_ENV, &token_file);

        for url in [
            "https://example.com/datasets/org/repo",
            "http://example.com/datasets/org/repo",
            // Only exact Hub hosts match.
            "https://huggingface.co.example.com/datasets/org/repo",
        ] {
            assert_eq!(resolve(&repo, url, Interaction::Never)?.source(), &CredentialSource::Anonymous, "{url}");
            let _token = EnvVarGuard::set(HF_TOKEN_ENV, "from-env");
            assert_eq!(resolve(&repo, url, Interaction::Never)?.source(), &CredentialSource::Anonymous, "{url}");
        }
        // Sources bound to that host still apply.
        let netrc = repo.path().join("netrc");
        std::fs::write(&netrc, "machine example.com login user password from-netrc\n")?;
        let _netrc = EnvVarGuard::set("NETRC", &netrc);
        assert_eq!(
            resolve(&repo, "https://example.com/datasets/org/repo", Interaction::Never)?.source(),
            &CredentialSource::Netrc
        );

        // Hub hosts match regardless of case, and HF_ENDPOINT adds its host, whatever the port.
        let _token = EnvVarGuard::set(HF_TOKEN_ENV, "from-env");
        let _endpoint = EnvVarGuard::set(HF_ENDPOINT_ENV, "http://localhost:5564/");
        for url in [
            "https://hf.co/datasets/org/repo",
            "https://HuggingFace.co/datasets/org/repo",
            "https://hub-ci.huggingface.co/datasets/org/repo",
            "http://localhost:5564/datasets/org/repo",
            "http://localhost:8080/datasets/org/repo",
        ] {
            assert_eq!(
                resolve(&repo, url, Interaction::Never)?.source(),
                &CredentialSource::Environment(HF_TOKEN_ENV),
                "{url}"
            );
        }
        Ok(())
    }

    // An SSH remote uses SSH, the identity `git push` uses, whatever other credentials exist: a token
    // file or HF_TOKEN may hold a read token, and SSH only authenticates to the remote's own host.
    #[test]
    #[serial(env_var_write_read)]
    fn ssh_remotes_always_use_ssh() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _isolation = isolate(&repo)?;
        store_credential(&repo, "from-helper")?;
        let netrc = repo.path().join("netrc");
        std::fs::write(&netrc, "machine hf.co login user password from-netrc\n")?;
        let _netrc = EnvVarGuard::set("NETRC", &netrc);
        let token_file = repo.path().join("token");
        std::fs::write(&token_file, "from-file\n")?;
        let _token_path = EnvVarGuard::set(HF_TOKEN_PATH_ENV, &token_file);
        let _token = EnvVarGuard::set(HF_TOKEN_ENV, "from-env");

        for url in [
            "ssh://git@hf.co/datasets/org/repo",
            "git@hf.co:datasets/org/repo",
            "git@example.com:datasets/org/repo",
        ] {
            for interaction in [Interaction::Never, Interaction::Allowed] {
                assert_eq!(resolve(&repo, url, interaction)?.source(), &CredentialSource::Ssh, "{url}");
            }
        }
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn netrc_matches_the_hostname_without_port() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _isolation = isolate(&repo)?;
        let netrc = repo.path().join("netrc");
        std::fs::write(&netrc, "machine hub.example.com login user password from-netrc\n")?;
        let _netrc = EnvVarGuard::set("NETRC", &netrc);

        for url in [
            "https://hub.example.com/datasets/org/repo",
            "https://hub.example.com:8443/datasets/org/repo",
            "http://Hub.Example.com:5564/datasets/org/repo",
        ] {
            assert_eq!(resolve(&repo, url, Interaction::Never)?.source(), &CredentialSource::Netrc, "{url}");
        }
        assert_eq!(
            resolve(&repo, "https://example.com:8443/datasets/org/repo", Interaction::Never)?.source(),
            &CredentialSource::Anonymous
        );
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn token_file_follows_huggingface_hub_locations() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _isolation = isolate(&repo)?;
        let _home = EnvVarGuard::set("HOME", repo.path());
        // On Windows the home directory comes from the user profile, not HOME.
        let home = dirs::home_dir().unwrap();
        assert_eq!(token_file_path(), Some(home.join(".cache/huggingface/token")));
        let _xdg = EnvVarGuard::set("XDG_CACHE_HOME", repo.path().join("xdg"));
        assert_eq!(token_file_path(), Some(repo.path().join("xdg/huggingface/token")));
        let _hf_home = EnvVarGuard::set(HF_HOME_ENV, repo.path().join("hf"));
        assert_eq!(token_file_path(), Some(repo.path().join("hf/token")));
        let _hf_home_tilde = EnvVarGuard::set(HF_HOME_ENV, "~/tilde-hf");
        assert_eq!(token_file_path(), Some(home.join("tilde-hf/token")));
        let _path_tilde = EnvVarGuard::set(HF_TOKEN_PATH_ENV, "~/tilde-token");
        assert_eq!(token_file_path(), Some(home.join("tilde-token")));
        let _path = EnvVarGuard::set(HF_TOKEN_PATH_ENV, repo.path().join("explicit"));
        assert_eq!(token_file_path(), Some(repo.path().join("explicit")));
        std::fs::write(repo.path().join("explicit"), "\n")?;
        assert_eq!(read_token_file(&repo.path().join("explicit"))?, None);
        Ok(())
    }

    // Without a stored credential, a non-interactive lookup is anonymous, runs no askpass program,
    // and returns promptly even when a credential helper hangs.
    #[cfg(unix)]
    #[test]
    #[serial(env_var_write_read)]
    fn non_interactive_never_prompts_or_blocks() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let repo = TestRepo::new("main")?;
        let _isolation = isolate(&repo)?;
        let marker = repo.path().join("askpass-called");
        let askpass = repo.path().join("askpass.sh");
        std::fs::write(&askpass, format!("#!/bin/sh\ntouch '{}'\necho secret\n", marker.display()))?;
        std::fs::set_permissions(&askpass, std::fs::Permissions::from_mode(0o755))?;
        run_git_captured(repo.path(), "config", ["core.askPass", askpass.to_str().unwrap()])?;
        let _ssh_askpass = EnvVarGuard::set("SSH_ASKPASS", &askpass);
        // Interactive prompts are possible here: no terminal prompt guard for this lookup.
        let _prompt = EnvVarGuard::unset("GIT_TERMINAL_PROMPT");

        let resolved = resolve(&repo, REMOTE, Interaction::Never)?;
        assert_eq!(resolved.source(), &CredentialSource::Anonymous);
        assert!(!marker.exists());

        let hang = repo.path().join("hang.sh");
        std::fs::write(&hang, "#!/bin/sh\nsleep 30\n")?;
        std::fs::set_permissions(&hang, std::fs::Permissions::from_mode(0o755))?;
        let hang_helper = format!("!{}", hang.display());
        run_git_captured(repo.path(), "config", ["--add", "credential.helper", hang_helper.as_str()])?;
        let start = Instant::now();
        assert_eq!(resolve(&repo, REMOTE, Interaction::Never)?.source(), &CredentialSource::Anonymous);
        assert!(start.elapsed() < Duration::from_secs(10));
        Ok(())
    }

    // Pre-push uses only what `git push` can use: HF_TOKEN and the token file are never consulted.
    #[test]
    #[serial(env_var_write_read)]
    fn interactive_lookup_uses_only_git_sources() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _isolation = isolate(&repo)?;
        let token_file = repo.path().join("token");
        std::fs::write(&token_file, "from-file\n")?;
        let _token_path = EnvVarGuard::set(HF_TOKEN_PATH_ENV, &token_file);
        let _token = EnvVarGuard::set(HF_TOKEN_ENV, "from-env");

        // GIT_TERMINAL_PROMPT=0 stands in for a user who cannot answer the prompt.
        let error = resolve(&repo, REMOTE, Interaction::Allowed).err().unwrap().to_string();
        assert!(error.contains("datasets/org/repo") && error.contains("credential helper"), "{error}");

        let url_with_token = "https://user:from-url@huggingface.co/datasets/org/repo";
        assert_eq!(resolve(&repo, url_with_token, Interaction::Allowed)?.source(), &CredentialSource::RemoteUrl);
        store_credential(&repo, "stored-token")?;
        assert_eq!(resolve(&repo, REMOTE, Interaction::Allowed)?.source(), &CredentialSource::GitCredentialHelper);
        let netrc = repo.path().join("netrc");
        std::fs::write(&netrc, "machine huggingface.co login user password from-netrc\n")?;
        let _netrc = EnvVarGuard::set("NETRC", &netrc);
        assert_eq!(resolve(&repo, REMOTE, Interaction::Allowed)?.source(), &CredentialSource::Netrc);
        Ok(())
    }

    // Serves one fixed HTTP status for every request.
    fn status_server(status: u16) -> anyhow::Result<String> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut stream = stream;
                let mut buffer = [0; 4096];
                let _ = std::io::Read::read(&mut stream, &mut buffer);
                let _ = write!(stream, "HTTP/1.1 {status} X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
            }
        });
        Ok(endpoint)
    }

    #[test]
    #[serial(env_var_write_read)]
    fn describes_hub_auth_failures() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _isolation = isolate(&repo)?;
        let url: GitUrl = REMOTE.parse()?;
        let ctx = XetContext::default()?;
        let token = |source| ResolvedCredential {
            helper: BearerCredentialHelper::new("hf_secret".into(), "test"),
            source,
        };
        for (status, source, operation, expected) in [
            (401, CredentialSource::Anonymous, Operation::Download, "requires authentication"),
            (401, CredentialSource::Environment(HF_TOKEN_ENV), Operation::Download, "rejected HF_TOKEN"),
            (403, CredentialSource::Environment(HF_TOKEN_ENV), Operation::Upload, "lacks write access"),
            (404, CredentialSource::Environment(HF_TOKEN_ENV), Operation::Download, "was not found"),
        ] {
            let resolved = token(source);
            let hub = HubClient::new(
                ctx.clone(),
                &status_server(status)?,
                url.repo_info()?,
                Some("main".into()),
                "test",
                Some(resolved.helper()),
                None,
            )?;
            let error = ctx.runtime.handle().block_on(hub.get_cas_jwt(operation)).err().unwrap();
            let message = resolved.describe_failure(&error, &url, operation).unwrap();
            assert!(message.contains(expected) && message.contains("datasets/org/repo"), "{message}");
            assert!(!message.contains("hf_secret"));
        }
        Ok(())
    }
}
