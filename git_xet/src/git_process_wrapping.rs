use std::path::Path;
use std::process::{Command, Stdio};

use crate::constants::GIT_EXECUTABLE;
use crate::errors::{GitXetError, Result};

pub fn run_git_captured(working_dir: impl AsRef<Path>, git_command: &str, args: &[&str]) -> Result<()> {
    let mut command = Command::new(GIT_EXECUTABLE);
    command.current_dir(working_dir).arg(git_command).args(args);

    run_command_captured(&mut command)
}

pub fn run_command_captured(command: &mut Command) -> Result<()> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
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
