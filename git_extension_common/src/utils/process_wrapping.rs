use std::ffi::OsStr;
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::time::{Duration, Instant};

use crate::constants::GIT_EXECUTABLE;
use crate::errors::{GitExtensionError, Result};

// This mod implements utilities to invoke commands through child processes from any program, and
// exposes special helpers to invoke git commands.

/// Run a Git command as a child process by setting the current working directory to `working_dir`.
/// This function doesn't allow the parent process to send data to the child.
///
/// Return `Ok(())` if the Git command finishes correctly and the child's stdout and stderr are ignored;
/// Return the underlying I/O error if the child process spawning or waiting fails; otherwise, the captured
/// stdout and stderr of the child are wrapped in an `Err(GitExtensionError::CommandFailed(_))` and returned.
pub fn run_git_captured<P, S1, I, S2>(working_dir: P, git_command: S1, args: I) -> Result<()>
where
    P: AsRef<Path>,
    S1: AsRef<OsStr>,
    I: IntoIterator<Item = S2>,
    S2: AsRef<OsStr>,
{
    let mut command = Command::new(GIT_EXECUTABLE);
    command.current_dir(working_dir).arg(git_command).args(args);

    CapturedCommand::new(command)?.wait()
}

/// Run a command as a child process by setting the current working directory to `working_dir`.
/// This function doesn't allow the parent process to send data to the child.
///
/// Return `Ok(())` if the command finishes correctly and the child's stdout and stderr are ignored;
/// Return the underlying I/O error if the child process spawning or waiting fails; otherwise, the captured
/// stdout and stderr of the child are wrapped in an `Err(GitExtensionError::CommandFailed(_))` and returned.
pub fn run_program_captured<S1, P, I, S2>(program: S1, working_dir: P, args: I) -> Result<()>
where
    S1: AsRef<OsStr>,
    P: AsRef<Path>,
    I: IntoIterator<Item = S2>,
    S2: AsRef<OsStr>,
{
    let mut command = Command::new(program);
    command.current_dir(working_dir).args(args);

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

/// Run a command as a child process by setting the current working directory to `working_dir`.
/// This function allows the parent process to send data to the child through the piped stdin.
///
/// Return `Ok(CapturedCommand)` if the command process spawns correctly; otherwise, the underlying I/O
/// error is returned.
///
/// # Examples
/// ```ignore
/// let mut cmd = run_program_captured_with_input_and_output("tee", path, &["dump.txt"])?;
///
/// {
///     let mut writer = cmd.stdin()?;
///     write!(writer, "some_data")?;
/// }
///
/// let (response, _err) = cmd.wait_with_output()?;
/// ```
pub fn run_program_captured_with_input_and_output<S1, P, I, S2>(
    program: S1,
    working_dir: P,
    args: I,
) -> Result<CapturedCommand>
where
    S1: AsRef<OsStr>,
    P: AsRef<Path>,
    I: IntoIterator<Item = S2>,
    S2: AsRef<OsStr>,
{
    let mut command = Command::new(program);
    command.current_dir(working_dir).args(args);

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
                std::io::ErrorKind::NotFound => GitExtensionError::cmd_failed(
                    format!(r#"program "{}" not found"#, command.get_program().display()),
                    Some(e),
                ),
                _ => GitExtensionError::cmd_failed("internal", Some(e)),
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
            .ok_or_else(|| GitExtensionError::internal("stdin of child process is not captured"))
    }

    /// Synchronously wait for the child to exit completely, returning `Ok(())` if the child exits with status code 0;
    /// otherwise, return the captured output wrapped in an `Err(GitExtensionError::CommandFailed(_))`.
    pub fn wait(self) -> Result<()> {
        // ignores output
        let _ = self.wait_with_output()?;

        Ok(())
    }

    /// Synchronously wait for the child to exit and collect all remaining output on the stdout/stderr handles,
    /// returning a tuple of captured output if the child exits with status code 0; otherwise, return the captured
    /// output wrapped in an `Err(GitExtensionError::CommandFailed(_))`.
    pub fn wait_with_output(self) -> Result<(Vec<u8>, Vec<u8>)> {
        let ret = self.wait_with_status()?;

        check_command_output(ret)
    }

    /// Waits for the child and returns its exit status and captured stdout/stderr without treating
    /// nonzero status as an error. Use when a command gives specific exit codes meaning, such as
    /// `git config --get-all` returning 1 for a missing key. Spawn and wait errors still propagate.
    /// Any stdin handle still owned by the child wrapper is closed before waiting.
    pub fn wait_with_status(self) -> Result<Output> {
        Ok(self.child_process.wait_with_output()?)
    }

    /// Like [`Self::wait_with_status`], but kills the child and returns `Ok(None)` if it has not exited
    /// within `timeout`. Output is collected after exit, so a child that fills a pipe before exiting
    /// times out; use only for commands with small output.
    pub fn wait_with_status_timeout(mut self, timeout: Duration) -> Result<Option<Output>> {
        drop(self.child_process.stdin.take());
        let deadline = Instant::now() + timeout;
        while self.child_process.try_wait()?.is_none() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let _ = self.child_process.kill();
                let _ = self.child_process.wait();
                return Ok(None);
            }
            std::thread::sleep(remaining.min(Duration::from_millis(10)));
        }
        Ok(Some(self.child_process.wait_with_output()?))
    }
}

/// Returns captured stdout/stderr on success, or the standard command error with status and output.
pub fn check_command_output(ret: Output) -> Result<(Vec<u8>, Vec<u8>)> {
    match ret.status.code() {
        Some(0) => Ok((ret.stdout, ret.stderr)),
        _ => {
            let stdout = std::str::from_utf8(&ret.stdout).unwrap_or("<Binary Data>").trim();
            let stderr = std::str::from_utf8(&ret.stderr).unwrap_or("<Binary Data>").trim();
            Err(GitExtensionError::cmd_failed(
                format!("err_code = {:?}, stdout = \"{}\", stderr = \"{}\"", ret.status.code(), stdout, stderr),
                None,
            ))
        },
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::process::Command;
    use std::time::Duration;

    use anyhow::Result;

    use super::{CapturedCommand, run_program_captured, run_program_captured_with_input_and_output};

    #[cfg(unix)]
    #[test]
    fn test_wait_with_status_timeout_kills_slow_child() -> Result<()> {
        let wd = std::env::current_dir()?;
        let slow = run_program_captured_with_input_and_output("sh", &wd, ["-c", "sleep 30"])?;
        let start = std::time::Instant::now();
        assert!(slow.wait_with_status_timeout(Duration::from_millis(200))?.is_none());
        assert!(start.elapsed() < Duration::from_secs(5));

        let quick = run_program_captured_with_input_and_output("sh", &wd, ["-c", "printf done; exit 3"])?;
        let output = quick.wait_with_status_timeout(Duration::from_secs(5))?.unwrap();
        assert_eq!(output.status.code(), Some(3));
        assert_eq!(output.stdout, b"done");
        Ok(())
    }

    #[test]
    fn test_wait_with_status_preserves_nonzero_exit_and_output() -> Result<()> {
        let command = if cfg!(windows) {
            run_program_captured_with_input_and_output(
                "cmd",
                std::env::current_dir()?,
                ["/C", "echo output& echo diagnostic 1>&2& exit /b 5"],
            )?
        } else {
            run_program_captured_with_input_and_output(
                "sh",
                std::env::current_dir()?,
                ["-c", "printf output; printf diagnostic >&2; exit 5"],
            )?
        };
        let output = command.wait_with_status()?;
        assert_eq!(output.status.code(), Some(5));
        assert_eq!(String::from_utf8(output.stdout)?.trim(), "output");
        assert_eq!(String::from_utf8(output.stderr)?.trim(), "diagnostic");
        Ok(())
    }

    #[test]
    fn test_run_program_captured() -> Result<()> {
        #[cfg(unix)]
        run_program_captured("sh", std::env::current_dir()?, &["-c", "echo hello"])?;
        #[cfg(windows)]
        run_program_captured("cmd", std::env::current_dir()?, &["/C", "echo hello"])?;

        Ok(())
    }

    #[test]
    fn test_program_captured_with_input_and_output() -> Result<()> {
        let mut cmd = if cfg!(windows) {
            run_program_captured_with_input_and_output("cmd", std::env::current_dir()?, &["/C", "more"])?
        } else {
            run_program_captured_with_input_and_output("sh", std::env::current_dir()?, &["-c", "cat"])?
        };

        {
            let mut writer = cmd.stdin()?;
            write!(writer, "hello")?;
        }

        let (response, _err) = cmd.wait_with_output()?;
        #[cfg(unix)]
        assert_eq!(response, "hello".as_bytes());
        #[cfg(windows)]
        assert_eq!(response, "hello\r\n".as_bytes());

        Ok(())
    }

    #[test]
    fn test_error_on_get_stdin_without_captured() -> Result<()> {
        let command = Command::new(if cfg!(windows) { "cmd" } else { "sh" });

        let mut command = CapturedCommand::new(command)?;
        assert!(command.stdin().is_err());

        Ok(())
    }
}
