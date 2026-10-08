use std::path::Path;

use git_extension_common::process_wrapping::{check_command_output, run_git_captured_with_input_and_output};

use crate::Result;

/// Reads every value for a key, preserving empty values and embedded newlines through NUL separators.
/// Explicit --local/--global scope skips included files by default. A missing key is harmless, but
/// invalid keys (which can also return status 1) and other command errors must still propagate.
pub(crate) fn git_get_all_config_values(wd: &Path, loc_profile: &str, key: &str) -> Result<Vec<Vec<u8>>> {
    let output = run_git_captured_with_input_and_output(wd, "config", [loc_profile, "--null", "--get-all", key])?
        .wait_with_status()?;
    if output.status.code() == Some(1) && output.stdout.is_empty() && output.stderr.is_empty() {
        return Ok(Vec::new());
    }
    let (stdout, _) = check_command_output(output)?;
    Ok(stdout
        .split_inclusive(|&byte| byte == 0)
        .map(|value| value.strip_suffix(&[0]).unwrap_or(value).to_vec())
        .collect())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use git_extension_common::process_wrapping::run_git_captured;
    use git_extension_common::test_utils::TestRepo;
    use serial_test::serial;

    use super::git_get_all_config_values;
    use crate::GitHfError;

    #[test]
    #[serial(env_var_write_read)]
    fn test_get_git_all_config_values_preserves_values_and_rejects_invalid_keys() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        test_repo.set_config("test.value", "first\nsecond")?;
        run_git_captured(test_repo.path(), "config", ["--add", "test.value", ""])?;
        assert_eq!(
            git_get_all_config_values(test_repo.path(), "--local", "test.value")?,
            vec![b"first\nsecond".to_vec(), Vec::new()]
        );
        assert!(git_get_all_config_values(test_repo.path(), "--local", "test.missing")?.is_empty());
        assert!(matches!(
            git_get_all_config_values(test_repo.path(), "--local", "invalid"),
            Err(GitHfError::CommandFailed { .. })
        ));
        Ok(())
    }
}
