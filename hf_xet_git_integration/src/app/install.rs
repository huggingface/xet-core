use std::path::{Path, PathBuf};
use std::process::Command;

use crate::app::Command::Action;
use crate::constants::{GIT_EXECUTABLE, GIT_LFS_CUSTOM_TRANSFER_AGENT_NAME, GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM};
use crate::errors::{GitXetError, Result};

#[derive(Default)]
enum ConfigLocation {
    System,
    #[default]
    Global,
    Local(Option<PathBuf>),
}

pub fn system() -> Result<()> {
    install_impl(ConfigLocation::System)
}

pub fn global() -> Result<()> {
    install_impl(ConfigLocation::Global)
}

pub fn local(repo_path: Option<PathBuf>) -> Result<()> {
    install_impl(ConfigLocation::Local(repo_path))
}

fn install_impl(location: ConfigLocation) -> Result<()> {
    let cwd = std::env::current_dir()?;

    let (wd, loc_profile) = match location {
        ConfigLocation::System => (cwd, "--system"),
        ConfigLocation::Global => (cwd, "--global"),
        ConfigLocation::Local(maybe_loc) => (maybe_loc.unwrap_or(cwd), "--local"),
    };

    run_git(
        &wd,
        "config",
        &[
            loc_profile,
            &format!("lfs.customtransfer.{}.path", GIT_LFS_CUSTOM_TRANSFER_AGENT_NAME),
            GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM,
        ],
    )?;

    run_git(
        &wd,
        "config",
        &[
            loc_profile,
            &format!("lfs.customtransfer.{}.args", GIT_LFS_CUSTOM_TRANSFER_AGENT_NAME),
            Action.name(),
        ],
    )?;

    run_git(
        &wd,
        "config",
        &[
            loc_profile,
            &format!("lfs.customtransfer.{}.concurrent", GIT_LFS_CUSTOM_TRANSFER_AGENT_NAME),
            "false",
        ],
    )?;

    Ok(())
}

fn run_git(working_dir: impl AsRef<Path>, git_command: &str, args: &[&str]) -> Result<()> {
    let mut command = Command::new(GIT_EXECUTABLE);
    command.current_dir(working_dir).arg(git_command).args(args);
    let ret = command.spawn()?.wait_with_output()?;

    match ret.status.code() {
        Some(0) => Ok(()),
        _ => {
            let stdout = std::str::from_utf8(&ret.stdout).unwrap_or("<Binary Data>").trim();
            let stderr = std::str::from_utf8(&ret.stderr).unwrap_or("<Binary Data>").trim();
            Err(GitXetError::GitCommandError(format!(
                "err_code = {:?}, stdout = \"{}\", stderr = \"{}\"",
                ret.status.code(),
                stdout,
                stderr
            )))
        },
    }
}
