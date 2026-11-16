//! `git hf clone`: clones a repository and, if it uses the `hf` filter, sets up git-hf in it and
//! materializes its Xet files.
//!
//! Git's own `git clone` does the cloning, with the `hf` filter registered for that one command only
//! (`git -c`, saved in no config), so its checkout writes the pointer files unchanged whether or not
//! git-hf is installed globally. All arguments pass to Git as given. Git starts the filter only for a
//! repository whose attributes use it, and the filter then reports that worktree's location. This
//! includes submodules checked out with `--recurse-submodules`, which may use the filter whether or
//! not the clone itself does. Once `git clone` succeeds, clone sets up each reported worktree: it
//! registers the filter and the pre-push hook there, as `git hf install --local` does, and pulls its
//! Xet files. A repository that does not use the filter, or a clone that checks nothing out (such as
//! `--no-checkout` or `--bare`), is left as `git clone` made it. A failed set-up leaves a pointer
//! checkout, and clone lists the worktrees still to set up.
use std::ffi::{OsStr, OsString};

use git_extension_common::process_wrapping::run_git;

use super::install::{PROCESS, PROCESS_KEY, REQUIRED_KEY};
use super::{install, pull};
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
    // One worktree's failure does not stop the others.
    let mut failed = Vec::new();
    for worktree in worktrees {
        if let Err(error) = install::local_in(&worktree).and_then(|()| pull::pull(&worktree, None, false)) {
            eprintln!("{GIT_HF_PROGRAM}: could not set up {}: {error}", worktree.display());
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
