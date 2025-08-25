use std::path::PathBuf;

use crate::app::Command::Transfer;
use crate::constants::{GIT_LFS_CUSTOM_TRANSFER_AGENT_NAME, GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM};
use crate::errors::Result;
use crate::git_process_wrapping::run_git_captured;

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

    run_git_captured(
        &wd,
        "config",
        &[
            loc_profile,
            &format!("lfs.customtransfer.{}.path", GIT_LFS_CUSTOM_TRANSFER_AGENT_NAME),
            GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM,
        ],
    )?;

    run_git_captured(
        &wd,
        "config",
        &[
            loc_profile,
            &format!("lfs.customtransfer.{}.args", GIT_LFS_CUSTOM_TRANSFER_AGENT_NAME),
            Transfer.name(),
        ],
    )?;

    run_git_captured(
        &wd,
        "config",
        &[
            loc_profile,
            &format!("lfs.customtransfer.{}.concurrent", GIT_LFS_CUSTOM_TRANSFER_AGENT_NAME),
            "true",
        ],
    )?;

    Ok(())
}
