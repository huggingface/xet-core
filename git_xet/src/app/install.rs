use std::path::PathBuf;

use crate::app::Command::Transfer;
use crate::constants::{GIT_LFS_CUSTOM_TRANSFER_AGENT_NAME, GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM};
use crate::errors::{GitXetError, Result};
use crate::git_process_wrapping::run_git_captured;

#[derive(Default)]
pub(crate) enum ConfigLocation {
    System,
    #[default]
    Global,
    Local(Option<PathBuf>),
}

// Install git-xet in the system Git config.
pub fn system(concurrency: Option<u32>) -> Result<()> {
    install_impl(ConfigLocation::System, concurrency)
}

// Install git-xet in the global Git config.
pub fn global(concurrency: Option<u32>) -> Result<()> {
    install_impl(ConfigLocation::Global, concurrency)
}

// Install git-xet in the local repo's Git config at `repo_path` if valid or
// at the current working directory.
pub fn local(repo_path: Option<PathBuf>, concurrency: Option<u32>) -> Result<()> {
    install_impl(ConfigLocation::Local(repo_path), concurrency)
}

// Set up the "xet" transfer agent under the name "lfs.customtransfer.xet" in
// the Git config specified by `location` with the below values
//     path = git-xet
//     args = transfer
//     concurrent = <according to value from `concurrency` (default true)>
//
// If `concurrency` is specified with a value greater than 1, set "lfs.concurrenttransfers" in the same
// Git config as above to this number.
fn install_impl(location: ConfigLocation, concurrency: Option<u32>) -> Result<()> {
    let cwd = std::env::current_dir()?;

    let (wd, loc_profile) = match location {
        ConfigLocation::System => (cwd, "--system"),
        ConfigLocation::Global => (cwd, "--global"),
        ConfigLocation::Local(maybe_loc) => (maybe_loc.unwrap_or(cwd), "--local"),
    };

    let concurrent = if let Some(c) = concurrency {
        if c == 0 {
            return Err(GitXetError::config_error("concurrency can't be 0"));
        }
        if c == 1 {
            "false"
        } else {
            run_git_captured(&wd, "config", [loc_profile, "lfs.concurrenttransfers", &c.to_string()])?;
            "true"
        }
    } else {
        "true"
    };

    run_git_captured(
        &wd,
        "config",
        [
            loc_profile,
            &format!("lfs.customtransfer.{}.path", GIT_LFS_CUSTOM_TRANSFER_AGENT_NAME),
            GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM,
        ],
    )?;

    run_git_captured(
        &wd,
        "config",
        [
            loc_profile,
            &format!("lfs.customtransfer.{}.args", GIT_LFS_CUSTOM_TRANSFER_AGENT_NAME),
            Transfer.name(),
        ],
    )?;

    run_git_captured(
        &wd,
        "config",
        [
            loc_profile,
            &format!("lfs.customtransfer.{}.concurrent", GIT_LFS_CUSTOM_TRANSFER_AGENT_NAME),
            concurrent,
        ],
    )?;

    Ok(())
}

#[cfg(test)]
pub mod tests {
    use std::io::{BufRead, BufReader, Cursor};
    use std::path::PathBuf;

    use anyhow::Result;
    use serial_test::serial;

    use super::{global, local};
    use crate::git_process_wrapping::run_git_captured_with_input_and_output;
    use crate::git_repo::GitRepo;
    use crate::test_utils::TestRepo;

    pub fn get_lfs_env(env_list: &[u8], key: &str) -> Result<Option<String>> {
        let reader = BufReader::new(Cursor::new(env_list));

        for line in reader.lines() {
            let mut line = line?;
            line.retain(|c| !c.is_whitespace());

            if let Some(value) = line.strip_prefix(&format!("{key}=")) {
                return Ok(Some(value.to_owned()));
            }
        }

        Ok(None)
    }

    fn test_install_with_default_concurrency<F>(install_fn: F) -> Result<()>
    where
        F: FnOnce(Option<PathBuf>, Option<u32>) -> crate::errors::Result<()>,
    {
        // set up repo
        let test_repo = TestRepo::new("main")?;

        // install with default concurrency
        install_fn(Some(test_repo.path().to_owned()), None)?;

        let git_lfs_env = run_git_captured_with_input_and_output(test_repo.path(), "lfs", &["env"])?;
        let (env_list, _err) = git_lfs_env.wait_with_output()?;

        // test the "xet" transfer agent is registered
        assert!(get_lfs_env(&env_list, "UploadTransfers")?.unwrap().contains("xet"));
        assert!(get_lfs_env(&env_list, "DownloadTransfers")?.unwrap().contains("xet"));

        // test the "xet" transfer agent has the desired concurrency
        let repo = GitRepo::open(test_repo.path())?;
        assert_eq!(repo.config()?.get_bool("lfs.customtransfer.xet.concurrent")?, true);
        assert_eq!(get_lfs_env(&env_list, "ConcurrentTransfers")?.unwrap(), "8");

        Ok(())
    }

    fn test_install_with_no_concurrency<F>(install_fn: F) -> Result<()>
    where
        F: FnOnce(Option<PathBuf>, Option<u32>) -> crate::errors::Result<()>,
    {
        // set up repo
        let test_repo = TestRepo::new("main")?;

        // install with concurrency disabled
        install_fn(Some(test_repo.path().to_owned()), Some(1))?;

        let git_lfs_env = run_git_captured_with_input_and_output(test_repo.path(), "lfs", &["env"])?;
        let (env_list, _err) = git_lfs_env.wait_with_output()?;

        // test the "xet" transfer agent is registered
        assert!(get_lfs_env(&env_list, "UploadTransfers")?.unwrap().contains("xet"));
        assert!(get_lfs_env(&env_list, "DownloadTransfers")?.unwrap().contains("xet"));

        // test the "xet" transfer agent has the desired concurrency
        let repo = GitRepo::open(test_repo.path())?;
        assert_eq!(repo.config()?.get_bool("lfs.customtransfer.xet.concurrent")?, false);

        Ok(())
    }

    fn test_install_with_custom_concurrency<F>(install_fn: F) -> Result<()>
    where
        F: FnOnce(Option<PathBuf>, Option<u32>) -> crate::errors::Result<()>,
    {
        // set up repo
        let test_repo = TestRepo::new("main")?;

        // install with concurrency = 16
        install_fn(Some(test_repo.path().to_owned()), Some(16))?;

        let git_lfs_env = run_git_captured_with_input_and_output(test_repo.path(), "lfs", &["env"])?;
        let (env_list, _err) = git_lfs_env.wait_with_output()?;

        // test the "xet" transfer agent is registered
        assert!(get_lfs_env(&env_list, "UploadTransfers")?.unwrap().contains("xet"));
        assert!(get_lfs_env(&env_list, "DownloadTransfers")?.unwrap().contains("xet"));

        // test the "xet" transfer agent has the desired concurrency
        let repo = GitRepo::open(test_repo.path())?;
        assert_eq!(repo.config()?.get_bool("lfs.customtransfer.xet.concurrent")?, true);
        assert_eq!(get_lfs_env(&env_list, "ConcurrentTransfers")?.unwrap(), "16");

        Ok(())
    }

    fn test_install_with_bad_concurrency<F>(install_fn: F) -> Result<()>
    where
        F: FnOnce(Option<PathBuf>, Option<u32>) -> crate::errors::Result<()>,
    {
        let test_repo = TestRepo::new("main")?;

        // install with concurrency = 0
        let ret = install_fn(Some(test_repo.path().to_owned()), Some(0));

        assert!(ret.is_err());

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_local_install_with_default_concurrency() -> Result<()> {
        test_install_with_default_concurrency(local)
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_local_install_with_no_concurrency() -> Result<()> {
        test_install_with_no_concurrency(local)
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_local_install_with_custom_concurrency() -> Result<()> {
        test_install_with_custom_concurrency(local)
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_local_install_with_bad_concurrency() -> Result<()> {
        test_install_with_bad_concurrency(local)
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_install_precedence() -> Result<()> {
        // set up repo
        let test_repo = TestRepo::new("main")?;

        // install local + global
        local(Some(test_repo.path().to_owned()), Some(16))?;
        global(Some(32))?;

        let git_lfs_env = run_git_captured_with_input_and_output(test_repo.path(), "lfs", &["env"])?;
        let (env_list, _err) = git_lfs_env.wait_with_output()?;

        // test the "xet" transfer agent is registered
        assert!(get_lfs_env(&env_list, "UploadTransfers")?.unwrap().contains("xet"));
        assert!(get_lfs_env(&env_list, "DownloadTransfers")?.unwrap().contains("xet"));

        // test the "xet" transfer agent has the desired concurrency: the local config has precedence
        let repo = GitRepo::open(test_repo.path())?;
        assert_eq!(repo.config()?.get_bool("lfs.customtransfer.xet.concurrent")?, true);
        assert_eq!(get_lfs_env(&env_list, "ConcurrentTransfers")?.unwrap(), "16");

        Ok(())
    }
}
