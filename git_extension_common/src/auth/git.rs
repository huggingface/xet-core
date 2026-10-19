use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use xet_client::hub_client::BearerCredentialHelper;

use super::Interaction;
use crate::constants::GIT_EXECUTABLE;
use crate::errors::Result;
use crate::utils::process_wrapping::CapturedCommand;

/// How long a non-interactive lookup waits for a credential helper before giving up.
const NON_INTERACTIVE_TIMEOUT: Duration = Duration::from_secs(2);

// This implements the mechanism to get credential stored in configured git credential helpers, including
// git-credential-cache, git-credential-store, git-credential-libsecret (Linux), git-credential-osxkeychain (macOS),
// git-credential-wincred (Windows) and Git Credential Manager (cross platform, included in Git for Windows).
// All these can be accessed uniformly through command `git credential fill` with a defined input/output format like
// below.
// INPUT:
//      ```
//      protocol=https
//      host=hf.co
//      [blank line]
//      ```
// , which is equivalent to
//      ```
//      url=https://hf.co
//      [blank line]
//      ```
// OUTPUT:
//      ```
//      protocol=https
//      host=hf.co
//      username=bob
//      password=secr3t
//      ```
// See https://git-scm.com/docs/git-credential for details.
//
// If no saved credential is found for the queried host, this command will ask the user for credentials following the
// below strategies (https://git-scm.com/docs/gitcredentials)
// > 1. If the `GIT_ASKPASS` environment variable is set, the program specified by the variable is invoked. A suitable
// > prompt is provided to the program on the command line, and the user’s input is read from its standard output.
// > 2. Otherwise, if the `core.askPass` configuration variable is set, its value is used as above.
// > 3. Otherwise, if the `SSH_ASKPASS` environment variable is set, its value is used as above.
// > 4. Otherwise, the user is prompted on the terminal.
// In case 4, dedicated channels (file descriptors) are opened from "/dev/tty" (unix-like systems) or "CONIN$ and
// "CONOUT$" (Windows) so the process can still communicate with the parent process, i.e. git-xet over stdin (fd 0) and
// stdout (fd 1). See https://github.com/git/git/blob/2462961280690837670d997bde64bd4ebf8ae66d/compat/terminal.c#L427 for details.
pub struct GitCredentialHelper;

impl GitCredentialHelper {
    /// Asks Git's credential helpers for `host_url`'s credential and returns a helper that sends its
    /// password as a bearer token, or `Ok(None)` if no credential was produced.
    ///
    /// With [`Interaction::Never`], every prompt path is disabled: an empty `GIT_ASKPASS` stops Git
    /// from running `core.askPass` or `SSH_ASKPASS`, `GIT_TERMINAL_PROMPT=0` stops terminal prompts,
    /// and `GCM_INTERACTIVE=never` stops Git Credential Manager's UI. Only a stored credential is
    /// returned; a helper that does not answer within 2 seconds is killed. With
    /// [`Interaction::Allowed`], Git may prompt.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        repo_path: impl AsRef<Path>,
        host_url: &str,
        interaction: Interaction,
    ) -> Result<Option<Arc<BearerCredentialHelper>>> {
        let hf_token = Self::authenticate(repo_path.as_ref(), host_url, interaction)?;

        Ok(hf_token.map(|hf_token| BearerCredentialHelper::new(hf_token, "git")))
    }

    fn authenticate(repo_path: &Path, host_url: &str, interaction: Interaction) -> Result<Option<String>> {
        let mut command = Command::new(GIT_EXECUTABLE);
        command.current_dir(repo_path).args(["credential", "fill"]);
        if matches!(interaction, Interaction::Never) {
            command
                .env("GIT_ASKPASS", "")
                .env("GIT_TERMINAL_PROMPT", "0")
                .env("GCM_INTERACTIVE", "never");
        }
        let mut cred_query = CapturedCommand::new_with_piped_stdin(command)?;

        // writer is dropped at the end of the scope
        {
            let mut writer = cred_query.stdin()?;
            write!(writer, "url={host_url}\n\n")?;
        }

        let output = match interaction {
            Interaction::Allowed => cred_query.wait_with_status()?,
            Interaction::Never => match cred_query.wait_with_status_timeout(NON_INTERACTIVE_TIMEOUT)? {
                Some(output) => output,
                None => return Ok(None),
            },
        };
        if !output.status.success() {
            return Ok(None);
        }

        Ok(parse_password(&output.stdout))
    }
}

fn parse_password(output: &[u8]) -> Option<String> {
    String::from_utf8_lossy(output).lines().find_map(|line| {
        line.trim()
            .strip_prefix("password=")
            .map(str::trim)
            .filter(|password| !password.is_empty())
            .map(str::to_owned)
    })
}

#[cfg(test)]
mod test_cred_helpers {
    use std::io::Write;

    use anyhow::Result;
    use serial_test::serial;

    use super::GitCredentialHelper;
    use crate::auth::Interaction;
    use crate::git_url::GitUrl;
    use crate::test_utils::TestRepo;
    use crate::utils::process_wrapping::run_git_captured_with_input_and_output;

    #[test]
    #[serial(env_var_write_read)]
    fn test_git_cred_helper() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let host_url = "http://localhost:1234";
        let username = "user";
        let password = "secr3t";

        // 1. set credential helper to store with a local file
        test_repo.set_config("credential.helper", "store")?;

        // 2. store a credential
        let mut cred_store = run_git_captured_with_input_and_output(test_repo.path(), "credential-store", &["store"])?;
        {
            let mut writer = cred_store.stdin()?;
            write!(writer, "url={host_url}\nusername={username}\npassword={password}\n\n")?;
        }
        cred_store.wait()?;

        // 3. test git credential helper
        let remote_url = format!("{host_url}/datasets/test/td");
        let parsed_url: GitUrl = remote_url.parse()?;
        let host_url = parsed_url.host_url()?;

        let helper = GitCredentialHelper::new(test_repo.path(), &host_url, Interaction::Allowed)?.unwrap();
        assert_eq!(helper.hf_token, password);

        Ok(())
    }
}
