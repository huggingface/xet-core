use std::path::Path;
use std::rc::Rc;

use anyhow::Result;
use tempfile::{TempDir, tempdir};
use xet_runtime::utils::EnvVarGuard;

use crate::utils::process_wrapping::run_git_captured;

const GIT_ENV_OVERRIDES: &[&str] = &[
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_COMMON_DIR",
    "GIT_INDEX_FILE",
    "GIT_CONFIG",
    "GIT_CONFIG_GLOBAL",
    "GIT_CONFIG_COUNT",
    "GIT_CONFIG_PARAMETERS",
    "XDG_CONFIG_HOME",
];

/// Temporary home with isolated Git configuration and repository discovery.
/// Inherited Git overrides and system configuration are disabled before any Git setup runs.
/// Environment settings are restored when the last clone is dropped. Tests must serialize process
/// environment changes with `#[serial(env_var_write_read)]`; `Rc` alone does not provide isolation.
#[derive(Clone)]
pub struct TempHome {
    pub _env_guard: Rc<EnvVarGuard>,
    _git_env_guards: Rc<Vec<EnvVarGuard>>,
    pub dir: Rc<TempDir>,
}

impl TempHome {
    pub fn new() -> Result<Self> {
        let home = tempdir()?;
        let home_dir = home.path();
        let abs_home_dir = std::path::absolute(home_dir)?;
        let home_env_guard = EnvVarGuard::set("HOME", abs_home_dir.as_os_str());
        let mut git_env_guards: Vec<_> = GIT_ENV_OVERRIDES.iter().map(|name| EnvVarGuard::unset(name)).collect();
        git_env_guards.push(EnvVarGuard::set("GIT_CONFIG_NOSYSTEM", "1"));

        Ok(Self {
            _env_guard: home_env_guard.into(),
            _git_env_guards: git_env_guards.into(),
            dir: home.into(),
        })
    }

    pub fn with_default_git_config(self) -> Result<Self> {
        run_git_captured(self.dir.path(), "config", &["--global", "user.name", "test"])?;
        run_git_captured(self.dir.path(), "config", &["--global", "user.email", "test@hf.co"])?;

        #[cfg(target_os = "macos")]
        let _ = run_git_captured(self.dir.path(), "config", &["--global", "--unset", "credential.helper"]);

        Ok(self)
    }

    pub fn path(&self) -> &Path {
        self.dir.path()
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs};

    use anyhow::Result;
    use serial_test::serial;
    use tempfile::tempdir;
    use xet_runtime::utils::EnvVarGuard;

    use super::{GIT_ENV_OVERRIDES, TempHome};
    use crate::process_wrapping::run_git_captured_with_input_and_output;
    use crate::test_utils::TestRepo;

    #[test]
    #[serial(env_var_write_read)]
    fn test_git_environment_is_restored_after_last_clone() -> Result<()> {
        let mut inherited: Vec<_> = GIT_ENV_OVERRIDES
            .iter()
            .map(|name| EnvVarGuard::set(name, "inherited"))
            .collect();
        inherited.push(EnvVarGuard::set("GIT_CONFIG_NOSYSTEM", "0"));
        let original_home = env::var_os("HOME");
        let home = TempHome::new()?;
        let path = home.path().to_owned();
        let clone = home.clone();
        drop(home);
        assert_eq!(env::var_os("HOME"), Some(path.clone().into_os_string()));
        for name in GIT_ENV_OVERRIDES {
            assert!(env::var_os(name).is_none(), "{name}");
        }
        assert_eq!(env::var("GIT_CONFIG_NOSYSTEM")?, "1");
        assert!(path.exists());
        drop(clone);
        assert_eq!(env::var_os("HOME"), original_home);
        for name in GIT_ENV_OVERRIDES {
            assert_eq!(env::var(name)?, "inherited", "{name}");
        }
        assert_eq!(env::var("GIT_CONFIG_NOSYSTEM")?, "0");
        assert!(!path.exists());
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_repo_setup_does_not_write_inherited_global_config() -> Result<()> {
        let external = tempdir()?;
        let path = external.path().join("config");
        let original = "[user]\nname = external\n";
        fs::write(&path, original)?;
        let _global = EnvVarGuard::set("GIT_CONFIG_GLOBAL", &path);
        let _git_dir = EnvVarGuard::set("GIT_DIR", external.path().join("missing-repo"));
        {
            let repo = TestRepo::new("main")?;
            let (value, _) =
                run_git_captured_with_input_and_output(repo.path(), "config", ["--global", "--get", "user.name"])?
                    .wait_with_output()?;
            assert_eq!(String::from_utf8(value)?.trim(), "test");
            assert_eq!(fs::read_to_string(&path)?, original);
        }
        assert_eq!(env::var_os("GIT_CONFIG_GLOBAL"), Some(path.into_os_string()));
        Ok(())
    }
}
