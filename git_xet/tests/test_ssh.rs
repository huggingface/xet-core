//! Integration tests for verifying access to POSIX utility commands when running programs
//! through `git-xet` (invoked via `git-lfs` and `git`). These tests focus on the behaviour on
//! Windows where "Git for Windows" ships a MinGW/MSYS environment containing common POSIX
//! utilities (`ssh`, `sh`, `chmod`, `uname`, etc.) and augments the `PATH` of the `git`
//! process with the directories that contain those utilities.
//!
//! Rationale
//! - When `git` is executed on Windows it adds the MinGW/MSYS directories to the `PATH` so
//!   child processes can find POSIX tools bundled with Git for Windows. When `git-xet` is
//!   invoked by the git-lfs filter process (the filter itself is started by `git`), it
//!   ultimately runs as a descendant of the `git` process and therefore inherits the augmented
//!   environment. These tests ensure that `git-xet` (launched via the `git` invocation
//!   chain) can locate and execute those POSIX utilities as expected. This is the same mechaism
//!   used by git-lfs on Windows to access the "ssh" utility.
//!
//! What is tested
//! - test_access_posix_commands: runs a set of simple POSIX commands through `git-xet`
//!   and asserts that the commands execute successfully (exit code 0) and emit output matching
//!   expected substrings.
//!
//! - test_ssh_connect_through_ssh_cmd and test_ssh_connect_through_sh_cmd:
//!   These tests start a local SSH server and then attempt to run `ssh` to that server through
//!   `git-xet`. They validate that invoking `ssh` directly or indirectly via `sh -c "ssh ..."`
//!   results in the expected JSON response from the server, proving that `ssh` is callable and
//!   functional when executed from within the `git-xet` invocation context.
//!
//! Implementation notes
//! - `git_xet_run` constructs a command that runs `git xet runany -- <command...>` pointing
//!   at the `git-xet` test binary (resolved through the `CARGO_BIN_EXE_git-xet` env var) called
//!   by `git`.
//! - On Windows the test sets the current directory to the `git-xet` build directory so the
//!   local `git-xet` executable can be found and executed (local directory precedence is
//!   relied upon on Windows). On Unix the build directory is prepended to `PATH` so the
//!   correct binary is found.
//! - The `runany` command of `git-xet` is gated behind the `git-xet-for-integration-test` feature.
//!   They are ignored by default unless that feature is enabled in the test run.
//!
//! These tests provide confidence that environment inheritance from `git` to `git-xet` is
//! sufficient for locating and invoking the POSIX utilities bundled with Git for Windows,
//! including functional SSH execution to a local server.
use std::ffi::OsStr;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use anyhow::Result;
use git_xet::test_utils::{GitLFSAuthenticateResponse, start_local_ssh_server};

fn git_xet_run<I, S>(command: I) -> std::io::Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let test_bin_path = env!("CARGO_BIN_EXE_git-xet");
    let buildpath = Path::new(&test_bin_path).parent().unwrap();
    let mut cmd = Command::new("git");
    cmd.args(["xet", "runany", "--"]); // Add "--" to escape options ("-[]" or "--[]") in the actual command
    cmd.args(command);
    cmd.current_dir(buildpath); // on Windows local directory takes the precedence to find an executable
    #[cfg(unix)]
    {
        cmd.env("PATH", format!("{}:{}", buildpath.to_str().unwrap_or_default(), std::env::var("PATH").unwrap()));
    }
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()?.wait_with_output()
}

#[test]
#[cfg(windows)]
#[cfg_attr(not(feature = "git-xet-for-integration-test"), ignore)]
fn test_access_posix_commands() -> Result<()> {
    let posix_commands_and_expected_output = [
        (vec!["env"], "PATH="),
        (vec!["chmod", "--version"], "chmod"),
        (vec!["sh", "-c", "echo hello"], "hello"),
        (vec!["ssh"], "usage: ssh"),
        (vec!["uname", "-s"], "MINGW64"),
    ];

    for (pc, expected_output) in posix_commands_and_expected_output {
        let o = git_xet_run(pc)?;
        // If command executed correctly, the return code should be 0;
        // otherwise if "program not found" on executing the command, the return code should be non-zero.
        assert_eq!(o.status.code(), Some(0));
        // The execution should output some text containing the expected output pattern, either through
        // stdout or stderr.
        assert!(
            String::from_utf8_lossy(&o.stdout).contains(expected_output)
                || String::from_utf8_lossy(&o.stderr).contains(expected_output)
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(not(feature = "git-xet-for-integration-test"), ignore)]
async fn test_ssh_connect_through_ssh_cmd() -> Result<()> {
    let (port, server_task) = start_local_ssh_server(None).await?;

    let ssh_cmd = [
        "ssh",
        "-p",
        &port.to_string(),
        "-o",
        "StrictHostKeyChecking no",
        "git@localhost",
        "git-lfs-authenticate",
        "user/repo",
        "upload",
    ];

    let o = git_xet_run(ssh_cmd)?;
    let response: GitLFSAuthenticateResponse = serde_json::from_slice(&o.stdout)?;
    assert!(response.header.authorization.starts_with("Basic "));
    assert_eq!(response.href, "https://huggingface.co/user/repo.git/info/lfs");
    assert_eq!(response.expires_in, 3600);

    server_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(not(feature = "git-xet-for-integration-test"), ignore)]
async fn test_ssh_connect_through_sh_cmd() -> Result<()> {
    let (port, server_task) = start_local_ssh_server(None).await?;

    let sh_cmd = [
        "sh",
        "-c",
        &format!("ssh -p {port}  -o \"StrictHostKeyChecking no\" git@localhost git-lfs-authenticate user/repo upload"),
    ];

    let o = git_xet_run(sh_cmd)?;
    let response: GitLFSAuthenticateResponse = serde_json::from_slice(&o.stdout)?;
    assert!(response.header.authorization.starts_with("Basic "));
    assert_eq!(response.href, "https://huggingface.co/user/repo.git/info/lfs");
    assert_eq!(response.expires_in, 3600);

    server_task.abort();
    Ok(())
}
