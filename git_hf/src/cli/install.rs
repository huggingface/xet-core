use std::path::PathBuf;

use git_extension_common::process_wrapping::run_git_captured;

use crate::utils::git_get_all_config_values;
use crate::{GitHfError, Result};

pub(super) const PROCESS_KEY: &str = "filter.hf.process";
pub(super) const REQUIRED_KEY: &str = "filter.hf.required";
pub(super) const PROCESS: &str = "git-hf filter-process --smudge-passthrough";

#[derive(Default)]
pub(super) enum ConfigLocation {
    System,
    #[default]
    Global,
    Local(Option<PathBuf>),
}

// Install git-hf in the system Git config.
pub(super) fn system() -> Result<()> {
    install_impl(ConfigLocation::System)
}

// Install git-hf in the global Git config.
pub(super) fn global() -> Result<()> {
    install_impl(ConfigLocation::Global)
}

// Install git-hf in the local repo's Git config at repo_path, or the current directory.
pub(super) fn local(repo_path: Option<PathBuf>) -> Result<()> {
    install_impl(ConfigLocation::Local(repo_path))
}

// Set up the required "hf" process filter.
fn install_impl(location: ConfigLocation) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let (wd, loc_profile) = match location {
        ConfigLocation::System => (cwd, "--system"),
        ConfigLocation::Global => (cwd, "--global"),
        ConfigLocation::Local(maybe_loc) => (maybe_loc.unwrap_or(cwd), "--local"),
    };

    // Check every process value before changing config, so a conflicting duplicate is not overlooked.
    let process = git_get_all_config_values(&wd, loc_profile, PROCESS_KEY)?;
    if process.iter().any(|value| value != PROCESS.as_bytes()) {
        return Err(GitHfError::InvalidGitConfig(format!(
            "{PROCESS_KEY} conflicts with git-hf in {loc_profile} config; preserve or remove that setting before installing"
        )));
    }

    // Once the process is compatible, enforce required=true even if it was disabled or duplicated.
    run_git_captured(&wd, "config", [loc_profile, "--replace-all", REQUIRED_KEY, "true"])?;
    if process.is_empty() {
        run_git_captured(&wd, "config", [loc_profile, PROCESS_KEY, PROCESS])?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::fs;

    use anyhow::Result;
    use git_extension_common::GitRepo;
    use git_extension_common::process_wrapping::run_git_captured;
    use git_extension_common::test_utils::{TempHome, TestRepo};
    use serial_test::serial;
    use xet_runtime::utils::EnvVarGuard;

    use super::{PROCESS, PROCESS_KEY, REQUIRED_KEY, global, local};
    use crate::GitHfError;
    use crate::cli::GitHfApp;
    use crate::utils::git_get_all_config_values;

    #[test]
    #[serial(env_var_write_read)]
    fn test_install_system() -> Result<()> {
        let home = TempHome::new()?;
        let _system = EnvVarGuard::set("GIT_CONFIG_SYSTEM", home.path().join("system.gitconfig"));
        let app = GitHfApp::try_parse_args_from(["git-hf", "install", "--system"].map(OsString::from))?;
        app.run()?;
        assert_eq!(git_get_all_config_values(home.path(), "--system", PROCESS_KEY)?, vec![PROCESS.as_bytes().to_vec()]);
        assert_eq!(git_get_all_config_values(home.path(), "--system", REQUIRED_KEY)?, vec![b"true".to_vec()]);
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_install_local() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let mut args = ["git-hf", "install", "--local", "--path"].map(OsString::from).to_vec();
        args.push(test_repo.path().as_os_str().to_owned());
        GitHfApp::try_parse_args_from(args)?.run()?;
        let repo = GitRepo::open(test_repo.path())?;
        assert_eq!(repo.config()?.get_string(PROCESS_KEY)?, PROCESS);
        assert!(repo.config()?.get_bool(REQUIRED_KEY)?);
        let before = fs::read(repo.common_dir()?.join("config"))?;
        local(Some(test_repo.path().to_owned()))?;
        assert_eq!(fs::read(repo.common_dir()?.join("config"))?, before);
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_install_global() -> Result<()> {
        let home = TempHome::new()?;
        for _ in 0..2 {
            global()?;
        }
        assert_eq!(git_get_all_config_values(home.path(), "--global", PROCESS_KEY)?, vec![PROCESS.as_bytes().to_vec()]);
        assert_eq!(git_get_all_config_values(home.path(), "--global", REQUIRED_KEY)?, vec![b"true".to_vec()]);
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_install_conflicts_preserve_config() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        test_repo.set_config(PROCESS_KEY, "custom-filter")?;
        test_repo.set_config(REQUIRED_KEY, "false")?;
        let config_path = GitRepo::open(test_repo.path())?.common_dir()?.join("config");
        let before = fs::read(&config_path)?;
        assert!(matches!(local(Some(test_repo.path().to_owned())), Err(GitHfError::InvalidGitConfig(_))));
        assert_eq!(fs::read(config_path)?, before);
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_install_overwrites_required_values() -> Result<()> {
        for existing_process in [None, Some(PROCESS)] {
            let test_repo = TestRepo::new("main")?;
            if let Some(process) = existing_process {
                test_repo.set_config(PROCESS_KEY, process)?;
            }
            for values in [&["false"][..], &["false", "true", "invalid"]] {
                test_repo.set_config(REQUIRED_KEY, values[0])?;
                for value in &values[1..] {
                    run_git_captured(test_repo.path(), "config", ["--add", REQUIRED_KEY, value])?;
                }
                local(Some(test_repo.path().to_owned()))?;
                assert_eq!(
                    git_get_all_config_values(test_repo.path(), "--local", REQUIRED_KEY)?,
                    vec![b"true".to_vec()]
                );
                assert_eq!(
                    git_get_all_config_values(test_repo.path(), "--local", PROCESS_KEY)?,
                    vec![PROCESS.as_bytes().to_vec()]
                );
            }
        }
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_install_checks_duplicate_values() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        test_repo.set_config(PROCESS_KEY, "custom")?;
        run_git_captured(test_repo.path(), "config", ["--add", PROCESS_KEY, PROCESS])?;
        assert!(matches!(local(Some(test_repo.path().to_owned())), Err(GitHfError::InvalidGitConfig(_))));
        assert!(git_get_all_config_values(test_repo.path(), "--local", REQUIRED_KEY)?.is_empty());
        run_git_captured(test_repo.path(), "config", ["--replace-all", PROCESS_KEY, PROCESS])?;
        run_git_captured(test_repo.path(), "config", ["--add", PROCESS_KEY, PROCESS])?;
        local(Some(test_repo.path().to_owned()))?;
        assert_eq!(git_get_all_config_values(test_repo.path(), "--local", PROCESS_KEY)?.len(), 2);
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_install_reports_config_lock_errors() -> Result<()> {
        let home = TempHome::new()?;
        fs::write(home.path().join(".gitconfig.lock"), b"locked")?;
        assert!(global().is_err());
        Ok(())
    }
}
