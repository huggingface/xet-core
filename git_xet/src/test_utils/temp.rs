#![cfg(test)]
use std::path::Path;
use std::rc::Rc;

use anyhow::Result;
use tempfile::{TempDir, tempdir};
use utils::EnvVarGuard;

use crate::git_process_wrapping::run_git_captured;

// A test utility to create a temporary HOME environment, this sets up a clean environment for
// git operations which depend heavily on a global config file.
// This is meant to be used only in single-threaded tests as `Rc` is `!Send` and `!Sync`.
#[derive(Clone)]
pub struct TempHome {
    pub _env_guard: Rc<EnvVarGuard>,
    pub dir: Rc<TempDir>,
}

impl TempHome {
    pub fn new() -> Result<Self> {
        let home = tempdir()?;
        let home_dir = home.path();
        let abs_home_dir = std::path::absolute(home_dir)?;
        let home_env_guard = EnvVarGuard::set("HOME", abs_home_dir.as_os_str());

        Ok(Self {
            _env_guard: home_env_guard.into(),
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
