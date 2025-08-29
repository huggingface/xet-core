use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};

use crate::constants::GIT_EXECUTABLE;
use crate::errors::{GitXetError, Result, internal};

pub fn run_git_captured(working_dir: impl AsRef<Path>, git_command: &str, args: &[&str]) -> Result<()> {
    let mut command = Command::new(GIT_EXECUTABLE);
    command.current_dir(working_dir).arg(git_command).args(args);

    CapturedCommand::new(command)?.wait()
}

pub fn run_git_captured_with_input_and_output(
    working_dir: impl AsRef<Path>,
    git_command: &str,
    args: &[&str],
) -> Result<CapturedCommand> {
    let mut command = Command::new(GIT_EXECUTABLE);
    command.current_dir(working_dir).arg(git_command).args(args);
    command.stdin(Stdio::piped());

    CapturedCommand::new(command)
}

pub struct CapturedCommand {
    child_process: Child,
}

impl CapturedCommand {
    pub fn new(mut command: Command) -> Result<Self> {
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        Ok(Self {
            child_process: command.spawn()?,
        })
    }

    pub fn stdin(&mut self) -> Result<ChildStdin> {
        self.child_process
            .stdin
            .take()
            .ok_or_else(|| internal("stdin of child process was closed"))
    }

    pub fn wait(self) -> Result<()> {
        // ignores output
        let _ = self.wait_with_output()?;

        Ok(())
    }

    pub fn wait_with_output(self) -> Result<(Vec<u8>, Vec<u8>)> {
        let ret = self.child_process.wait_with_output()?;

        match ret.status.code() {
            Some(0) => Ok((ret.stdout, ret.stderr)),
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
}
