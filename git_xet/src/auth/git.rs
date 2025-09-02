use std::io::{BufRead, BufReader, Cursor, Write};
use std::path::Path;
use std::sync::Arc;

use hub_client::BearerCredentialHelper;

use crate::errors::{Result, config_error};
use crate::git_process_wrapping::run_git_captured_with_input_and_output;

pub struct GitCredentialHelper {}

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

#[cfg(test)]
mod test_cred_helpers {
    use anyhow::Result;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::git_url::GitUrl;
    use crate::test_utils::test_repo::TestRepo;

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
}
