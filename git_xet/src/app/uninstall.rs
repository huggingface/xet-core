use std::path::PathBuf;

use super::install::ConfigLocation;
use crate::constants::GIT_LFS_CUSTOM_TRANSFER_AGENT_NAME;
use crate::errors::Result;
use crate::git_process_wrapping::run_git_captured;

// Remove git-xet registration from the system Git config.
pub fn system() -> Result<()> {
    uninstall_impl(ConfigLocation::System)
}

// Remove git-xet registration from the global Git config.
pub fn global() -> Result<()> {
    uninstall_impl(ConfigLocation::Global)
}

// Remove git-xet registration from the local repo's Git config at
// `repo_path` if valid or at the current working directory.
pub fn local(repo_path: Option<PathBuf>) -> Result<()> {
    uninstall_impl(ConfigLocation::Local(repo_path))
}

// Remove git-xet registration from all Git config locations.
pub fn all() -> Result<()> {
    system()?;
    global()?;
    local(None)
}

// Remove "lfs.customtransfer.xet" entirely from the Git config specified by `location`.
// Remove "lfs.concurrenttransfers" from the same Git config as above.
// Ignore errors if git-xet is not installed at the specified config.
fn uninstall_impl(location: ConfigLocation) -> Result<()> {
    let cwd = std::env::current_dir()?;

    let (wd, loc_profile) = match location {
        ConfigLocation::System => (cwd, "--system"),
        ConfigLocation::Global => (cwd, "--global"),
        ConfigLocation::Local(maybe_loc) => (maybe_loc.unwrap_or(cwd), "--local"),
    };

    let _ = run_git_captured(
        &wd,
        "config",
        [
            loc_profile,
            "--remove-section",
            &format!("lfs.customtransfer.{}", GIT_LFS_CUSTOM_TRANSFER_AGENT_NAME),
        ],
    );

    let _ = run_git_captured(&wd, "config", [loc_profile, "--unset", "lfs.concurrenttransfers"]);

    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use serial_test::serial;
    use utils::CwdGuard;

    use super::{all, local};
    use crate::app::install;
    use crate::app::install::tests::get_lfs_env;
    use crate::git_process_wrapping::run_git_captured_with_input_and_output;
    use crate::test_utils::TestRepo;

    #[test]
    #[serial(env_var_write_read)]
    fn test_uninstall_local() -> Result<()> {
        // set up repo
        let test_repo = TestRepo::new("main")?;

        // install with custom concurrency
        install::local(Some(test_repo.path().to_owned()), Some(16))?;

        // uninstall
        local(Some(test_repo.path().to_owned()))?;

        let git_lfs_env = run_git_captured_with_input_and_output(test_repo.path(), "lfs", &["env"])?;
        let (env_list, _err) = git_lfs_env.wait_with_output()?;

        // test the "xet" transfer agent is unregistered
        assert!(!get_lfs_env(&env_list, "UploadTransfers")?.unwrap().contains("xet"));
        assert!(!get_lfs_env(&env_list, "DownloadTransfers")?.unwrap().contains("xet"));

        // test the "lfs.concurrenttransfers" is reset to default
        assert_eq!(get_lfs_env(&env_list, "ConcurrentTransfers")?.unwrap(), "8");

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_uninstall_ignores_error_even_if_not_installed() -> Result<()> {
        let test_repo = TestRepo::new("main")?;

        local(Some(test_repo.path().to_owned()))?;

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_uninstall_all() -> Result<()> {
        // set up repo
        let test_repo = TestRepo::new("main")?;

        // install local + global
        install::local(Some(test_repo.path().to_owned()), Some(16))?;
        install::global(Some(32))?;

        // uninstall all
        let _cwd_guard = CwdGuard::set(test_repo.path())?;
        all()?;

        let git_lfs_env = run_git_captured_with_input_and_output(test_repo.path(), "lfs", &["env"])?;
        let (env_list, _err) = git_lfs_env.wait_with_output()?;

        // test the "xet" transfer agent is unregistered
        assert!(!get_lfs_env(&env_list, "UploadTransfers")?.unwrap().contains("xet"));
        assert!(!get_lfs_env(&env_list, "DownloadTransfers")?.unwrap().contains("xet"));

        // test the "lfs.concurrenttransfers" is reset to default
        assert_eq!(get_lfs_env(&env_list, "ConcurrentTransfers")?.unwrap(), "8");

        Ok(())
    }
}
