use std::io::{BufRead, BufReader, Cursor, Write};
use std::path::Path;
use std::sync::Arc;

use hub_client::BearerCredentialHelper;

use crate::errors::{GitXetError, Result};
use crate::git_process_wrapping::run_git_captured_with_input_and_output;

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
pub struct GitCredentialHelper {}

impl GitCredentialHelper {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(repo_path: impl AsRef<Path>, host_url: &str) -> Result<Arc<BearerCredentialHelper>> {
        let hf_token = Self::authenticate(repo_path.as_ref(), host_url)?;

        Ok(BearerCredentialHelper::new(hf_token, "git"))
    }

    fn authenticate(repo_path: &Path, host_url: &str) -> Result<String> {
        let mut cred_query = run_git_captured_with_input_and_output(repo_path, "credential", ["fill"])?;

        // writer is dropped at the end of the scope
        {
            let mut writer = cred_query.stdin()?;
            write!(writer, "url={host_url}\n\n")?;
        }

        let (response, _err) = cred_query.wait_with_output()?;

        let reader = BufReader::new(Cursor::new(response));

        for line in reader.lines() {
            let mut line = line?;
            line.retain(|c| !c.is_whitespace());

            if let Some(hf_token) = line.strip_prefix("password=")
                && !hf_token.is_empty()
            {
                return Ok(hf_token.to_owned());
            }
        }

        Err(GitXetError::config_error(format!("failed to find authentication for {host_url}")))
    }
}

#[cfg(test)]
mod test_cred_helpers {
    use std::io::Write;

    use anyhow::Result;
    use serial_test::serial;

    use super::GitCredentialHelper;
    use crate::git_process_wrapping::run_git_captured_with_input_and_output;
    use crate::git_url::GitUrl;
    use crate::test_utils::TestRepo;

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

        let git_cred_helper = GitCredentialHelper::new(test_repo.path(), &host_url)?;
        assert_eq!(git_cred_helper.hf_token, password);

        Ok(())
    }
}
