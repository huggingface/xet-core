use std::ffi::OsStr;
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};

use crate::constants::GIT_EXECUTABLE;
use crate::errors::{GitXetError, Result};

// This mod implements utilities to invoke Git commands through child processes from the `git` program.

/// Run a Git command as a child process by setting the current working directory to `working_dir`.
/// This function doesn't allow the parent process to send data to the child.
///
/// Return `Ok(())` if the Git command finishes correctly and the child's stdout and stderr are ignored;
/// Return the underlying I/O error if the child process spawning or waiting fails; otherwise, the captured
/// stdout and stderr of the child are wrapped in an `Err(GitXetError::GitCommandFailed(_))` and returned.
pub fn run_git_captured<P, S1, I, S2>(working_dir: P, git_command: S1, args: I) -> Result<()>
where
    P: AsRef<Path>,
    S1: AsRef<OsStr>,
    I: IntoIterator<Item = S2>,
    S2: AsRef<OsStr>,
{
    let mut command = Command::new(GIT_EXECUTABLE);
    command.current_dir(working_dir).arg(git_command.as_ref()).args(args);

    CapturedCommand::new(command)?.wait()
}

/// Run a Git command as a child process by setting the current working directory to `working_dir`.
/// This function allows the parent process to send data to the child through the piped stdin.
///
/// Return `Ok(CapturedCommand)` if the Git command process spawns correctly; otherwise, the underlying I/O
/// error is returned.
///
/// # Examples
/// ```ignore
/// let mut git_cmd = run_git_captured_with_input_and_output(repo_path, "cmd", &["arg"])?;
///
/// {
///     let mut writer = git_cmd.stdin()?;
///     write!(writer, "some_data")?;
/// }
///
/// let (response, _err) = git_cmd.wait_with_output()?;
/// ```
pub fn run_git_captured_with_input_and_output<P, S1, I, S2>(
    working_dir: P,
    git_command: S1,
    args: I,
) -> Result<CapturedCommand>
where
    P: AsRef<Path>,
    S1: AsRef<OsStr>,
    I: IntoIterator<Item = S2>,
    S2: AsRef<OsStr>,
{
    let mut command = Command::new(GIT_EXECUTABLE);
    command.current_dir(working_dir).arg(git_command).args(args);

    CapturedCommand::new_with_piped_stdin(command)
}

// This struct wraps inside a spawned child process, whose stdout and stderr is piped
// to the parent process instead of pointing at the terminal.
pub struct CapturedCommand {
    child_process: Child,
}

impl CapturedCommand {
    pub fn new(mut command: Command) -> Result<Self> {
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        Ok(Self {
            child_process: command.spawn().map_err(|e| match e.kind() {
                // From past experience, if the "git" program is not found the underlying error
                // only says "Not Found" and is not very helpful to identify the cause. We thus
                // capture this error and make the message more explicit.
                std::io::ErrorKind::NotFound => GitXetError::git_cmd_failed(r#"program "git" not found"#, Some(e)),
                _ => GitXetError::git_cmd_failed("internal", Some(e)),
            })?,
        })
    }

    pub fn new_with_piped_stdin(mut command: Command) -> Result<Self> {
        command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());

        Ok(Self {
            child_process: command.spawn()?,
        })
    }

    /// Return the handle for writing to the child's stdin, if it has been captured; otherwise,
    /// return an error.
    pub fn stdin(&mut self) -> Result<ChildStdin> {
        self.child_process
            .stdin
            .take()
            .ok_or_else(|| GitXetError::internal("stdin of child process is not captured"))
    }

    /// Synchronously wait for the child to exit completely, returning `Ok(())` if the child exits with status code 0;
    /// otherwise, return the captured output wrapped in an `Err(GitXetError::GitCommandFailed(_))`.
    pub fn wait(self) -> Result<()> {
        // ignores output
        let _ = self.wait_with_output()?;

        Ok(())
    }

    /// Synchronously wait for the child to exit and collect all remaining output on the stdout/stderr handles,
    /// returning a tuple of captured output if the child exits with status code 0; otherwise, return the captured
    /// output wrapped in an `Err(GitXetError::GitCommandFailed(_))`.
    pub fn wait_with_output(self) -> Result<(Vec<u8>, Vec<u8>)> {
        let ret = self.child_process.wait_with_output()?;

        match ret.status.code() {
            Some(0) => Ok((ret.stdout, ret.stderr)),
            _ => {
                let stdout = std::str::from_utf8(&ret.stdout).unwrap_or("<Binary Data>").trim();
                let stderr = std::str::from_utf8(&ret.stderr).unwrap_or("<Binary Data>").trim();
                Err(GitXetError::git_cmd_failed(
                    format!("err_code = {:?}, stdout = \"{}\", stderr = \"{}\"", ret.status.code(), stdout, stderr),
                    None,
                ))
            },
        }
    }
}
