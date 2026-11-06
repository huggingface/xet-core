use git_extension_common::process_wrapping::{check_command_output, run_git_captured_with_input_and_output};

use super::install::{ConfigLocation, PROCESS_KEY, REQUIRED_KEY};
use crate::Result;
use crate::utils::{git_get_all_config_values, is_inside_repository};

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

// Remove git-hf registration from all Git config locations. Every scope is attempted even if an earlier one fails,
// and the first error is returned. The local scope is skipped outside a repository.
pub(super) fn all() -> Result<()> {
    let system = system();
    let global = global();
    let local = if is_inside_repository(&std::env::current_dir()?)? {
        local()
    } else {
        Ok(())
    };
    system.and(global).and(local)
}

// Remove the entire filter.hf section when git-hf is registered in this scope. Git locks the config file before
// looking for the section, so removing from a file the user can't write (such as /etc/gitconfig for non-root
// users) fails even when there is nothing to remove. Reading takes no lock, so check for a registration first.
fn uninstall_impl(location: ConfigLocation) -> Result<()> {
    let wd = std::env::current_dir()?;
    let loc_profile = location.profile();
    if git_get_all_config_values(&wd, loc_profile, PROCESS_KEY)?.is_empty()
        && git_get_all_config_values(&wd, loc_profile, REQUIRED_KEY)?.is_empty()
    {
        return Ok(());
    }

    // Exit 128 means the section no longer exists, for example because a concurrent uninstall removed it. Any other
    // failure, such as a locked or unwritable config file (exit 255), is reported.
    let output = run_git_captured_with_input_and_output(&wd, "config", [loc_profile, "--remove-section", "filter.hf"])?
        .wait_with_status()?;
    if output.status.code() == Some(128) {
        return Ok(());
    }
    check_command_output(output)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use anyhow::Result;
    use git_extension_common::GitRepo;
    use git_extension_common::process_wrapping::run_git_captured;
    use git_extension_common::test_utils::{TempHome, TestRepo};
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

    #[test]
    #[serial(env_var_write_read)]
    fn test_uninstall_reports_config_lock_errors() -> Result<()> {
        let home = TempHome::new()?;
        install::global()?;
        fs::write(home.path().join(".gitconfig.lock"), b"locked")?;
        assert!(global().is_err());
        assert!(all().is_err());
        fs::remove_file(home.path().join(".gitconfig.lock"))?;
        assert_eq!(git_get_all_config_values(home.path(), "--global", PROCESS_KEY)?, vec![PROCESS.as_bytes().to_vec()]);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    #[serial(env_var_write_read)]
    fn test_uninstall_unwritable_system_config() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let home = TempHome::new()?;
        let system_dir = home.path().join("etc");
        fs::create_dir(&system_dir)?;
        let _system = EnvVarGuard::set("GIT_CONFIG_SYSTEM", system_dir.join("gitconfig"));
        let _cwd = CwdGuard::set(home.path())?;
        let set_mode = |mode| fs::set_permissions(&system_dir, fs::Permissions::from_mode(mode));

        // Without a registration, the unwritable system config isn't touched, so --all succeeds.
        install::global()?;
        set_mode(0o555)?;
        system()?;
        all()?;
        assert!(git_get_all_config_values(home.path(), "--global", PROCESS_KEY)?.is_empty());

        // With a registration, the lock failure is reported, the registration stays, and --all still removes the
        // global registration.
        set_mode(0o755)?;
        run_git_captured(home.path(), "config", ["--system", PROCESS_KEY, PROCESS])?;
        install::global()?;
        set_mode(0o555)?;
        let system_result = system();
        let all_result = all();
        set_mode(0o755)?;
        assert!(system_result.is_err());
        assert!(all_result.is_err());
        assert_eq!(git_get_all_config_values(home.path(), "--system", PROCESS_KEY)?, vec![PROCESS.as_bytes().to_vec()]);
        assert!(git_get_all_config_values(home.path(), "--global", PROCESS_KEY)?.is_empty());
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_uninstall_outside_repository() -> Result<()> {
        let home = TempHome::new()?;
        let _ceiling = EnvVarGuard::set("GIT_CEILING_DIRECTORIES", home.path());
        let _cwd = CwdGuard::set(home.path())?;
        assert!(local().is_err());
        install::global()?;
        all()?;
        assert!(git_get_all_config_values(home.path(), "--global", PROCESS_KEY)?.is_empty());
        Ok(())
    }
}
