use git_extension_common::process_wrapping::run_git_captured;

use super::install::ConfigLocation;
use crate::Result;

// Remove git-hf registration from the system Git config.
pub(super) fn system() -> Result<()> {
    uninstall_impl(ConfigLocation::System)
}

// Remove git-hf registration from the global Git config.
pub(super) fn global() -> Result<()> {
    uninstall_impl(ConfigLocation::Global)
}

// Remove git-hf registration from the Git config of the repository Git selects for the current directory.
pub(super) fn local() -> Result<()> {
    uninstall_impl(ConfigLocation::Local)
}

// Remove git-hf registration from all Git config locations.
pub(super) fn all() -> Result<()> {
    system()?;
    global()?;
    local()
}

// Remove the entire filter.hf section. Configuration removal is best effort.
fn uninstall_impl(location: ConfigLocation) -> Result<()> {
    let wd = std::env::current_dir()?;
    let _ = run_git_captured(&wd, "config", [location.profile(), "--remove-section", "filter.hf"]);
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use git_extension_common::GitRepo;
    use git_extension_common::process_wrapping::run_git_captured;
    use git_extension_common::test_utils::TestRepo;
    use serial_test::serial;
    use xet_runtime::utils::{CwdGuard, EnvVarGuard};

    use super::{all, global, local, system};
    use crate::cli::install::{self, PROCESS, PROCESS_KEY, REQUIRED_KEY};
    use crate::utils::git_get_all_config_values;

    #[test]
    #[serial(env_var_write_read)]
    fn test_uninstall_local() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(test_repo.path())?;
        install::local()?;
        for _ in 0..2 {
            local()?;
        }
        assert!(git_get_all_config_values(test_repo.path(), "--local", PROCESS_KEY)?.is_empty());
        assert!(git_get_all_config_values(test_repo.path(), "--local", REQUIRED_KEY)?.is_empty());
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_uninstall_global() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        install::global()?;
        for _ in 0..2 {
            global()?;
        }
        assert!(git_get_all_config_values(test_repo.path(), "--global", PROCESS_KEY)?.is_empty());
        assert!(git_get_all_config_values(test_repo.path(), "--global", REQUIRED_KEY)?.is_empty());
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_uninstall_when_not_installed() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(test_repo.path())?;
        local()?;
        global()?;
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_uninstall_removes_entire_section() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        test_repo.set_config(PROCESS_KEY, "custom-filter")?;
        test_repo.set_config(REQUIRED_KEY, "false")?;
        test_repo.set_config("filter.hf.clean", "user-clean")?;
        let _cwd = CwdGuard::set(test_repo.path())?;
        local()?;
        let config = GitRepo::open(test_repo.path())?.config()?;
        for key in [PROCESS_KEY, REQUIRED_KEY, "filter.hf.clean"] {
            assert!(config.get_string(key).is_err());
        }
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_uninstall_system_and_all() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let _system = EnvVarGuard::set("GIT_CONFIG_SYSTEM", test_repo.path().join("system.gitconfig"));
        let _cwd = CwdGuard::set(test_repo.path())?;
        run_git_captured(test_repo.path(), "config", ["--system", PROCESS_KEY, PROCESS])?;
        system()?;
        assert!(git_get_all_config_values(test_repo.path(), "--system", PROCESS_KEY)?.is_empty());

        run_git_captured(test_repo.path(), "config", ["--system", PROCESS_KEY, PROCESS])?;
        install::global()?;
        install::local()?;
        all()?;
        for scope in ["--system", "--global", "--local"] {
            assert!(git_get_all_config_values(test_repo.path(), scope, PROCESS_KEY)?.is_empty());
            assert!(git_get_all_config_values(test_repo.path(), scope, REQUIRED_KEY)?.is_empty());
        }
        Ok(())
    }
}
