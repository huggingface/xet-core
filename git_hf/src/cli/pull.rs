use git_extension_common::GitRepo;

use super::checkout::report;
use super::fetch::download;
use crate::checkout::CheckoutPlan;
use crate::constants::GIT_HF_PROGRAM;
use crate::file_cache::FileCache;
use crate::{GitHfError, Result};

/// Downloads the Xet files the current checkout is missing, then reconstructs them into the
/// worktree. Contacts the Hub only when some file is missing; never changes Git refs.
pub(super) fn run(remote: Option<&str>, force: bool) -> Result<()> {
    let wd = std::env::current_dir()?;
    let repo = GitRepo::open(&wd)?;
    if repo.worktree_dir()?.is_none() {
        return Err(GitHfError::NotSupported("pull requires a worktree".into()));
    }
    let cache = FileCache::new(&repo.common_dir()?)?;
    let plan = CheckoutPlan::new(&wd, &[], force)?;
    let missing = plan.missing(&cache);
    if !missing.is_empty() {
        let summary = download(&repo, remote, &cache, missing)?;
        eprintln!("{GIT_HF_PROGRAM}: fetched {} Xet files ({} bytes)", summary.files, summary.bytes);
    }
    report(&plan.apply(&cache)?);
    Ok(())
}
