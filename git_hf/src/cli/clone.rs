//! `git hf clone`: clones a repository and, if it uses the `hf` filter, sets up git-hf in it and
//! materializes its Xet files.
//!
//! Git's own `git clone` does the cloning, with the `hf` filter registered for that one command only
//! (`git -c`, saved in no config), so its checkout writes the pointer files unchanged whether or not
//! git-hf is installed globally. All arguments pass to Git as given. Git starts the filter only for a
//! repository whose attributes use it, and the filter then reports that worktree's location. This
//! includes submodules checked out with `--recurse-submodules`, which may use the filter whether or
//! not the clone itself does. Once `git clone` succeeds, clone sets up each reported worktree by
//! running `git hf install --local`, which registers the filter and the pre-push hook, and
//! `git hf pull` there. A repository that does not use the filter, or a clone that checks nothing
//! out (such as `--no-checkout` or `--bare`), is left as `git clone` made it. A failed set-up leaves
//! a pointer checkout, and clone lists the worktrees still to set up.
use std::ffi::{OsStr, OsString};

use git_extension_common::process_wrapping::run_git;

use super::install::{self, PROCESS, PROCESS_KEY, REQUIRED_KEY};
use super::pull;
use crate::constants::{CLONE_REPORT_ENV, GIT_HF_PROGRAM};
use crate::utils::path_from_bytes;
use crate::{GitHfError, Result};

pub(super) fn run(git_args: &[OsString]) -> Result<()> {
    let report = tempfile::NamedTempFile::new()?;
    let process = OsString::from(format!("{PROCESS_KEY}={PROCESS}"));
    let required = OsString::from(format!("{REQUIRED_KEY}=true"));
    let args = [
        OsStr::new("-c"),
        &process,
        OsStr::new("-c"),
        &required,
        OsStr::new("clone"),
    ]
    .into_iter()
    .chain(git_args.iter().map(OsString::as_os_str));
    run_git(std::env::current_dir()?, args, &[(CLONE_REPORT_ENV, report.path().as_os_str())])?;

    // Each filter process reports once, so a worktree appears again only if Git restarted its filter.
    let mut worktrees = Vec::new();
    for entry in std::fs::read(report.path())?
        .split(|&b| b == 0)
        .filter(|entry| !entry.is_empty())
    {
        let worktree = path_from_bytes(entry)?;
        if !worktrees.contains(&worktree) {
            worktrees.push(worktree);
        }
    }
    // `git clone` ignores an inherited `GIT_DIR`, such as one set by `git --git-dir`, but the set-up's
    // Git commands would follow it to another repository. Git's other variables that locate a repository
    // make `git clone` itself fail or misplace the clone, so they cannot reach here.
    // SAFETY: no other thread is running: `git clone` has finished and the set-up has not started.
    unsafe { std::env::remove_var("GIT_DIR") };
    // One worktree's failure does not stop the others.
    let mut failed = Vec::new();
    for worktree in worktrees {
        let set_up = std::env::set_current_dir(&worktree)
            .map_err(GitHfError::from)
            .and_then(|()| install::local())
            .and_then(|()| pull::run(None, false));
        if let Err(error) = set_up {
            eprintln!("{GIT_HF_PROGRAM}: {}: {error}", worktree.display());
            failed.push(format!("  {}", worktree.display()));
        }
    }
    if failed.is_empty() {
        return Ok(());
    }
    Err(GitHfError::CommandFailed {
        reason: format!(
            "these worktrees hold pointer checkouts; run `{GIT_HF_PROGRAM} install --local` and `{GIT_HF_PROGRAM} \
             pull` in each to retry:\n{}",
            failed.join("\n")
        ),
        source: None,
    })
}
