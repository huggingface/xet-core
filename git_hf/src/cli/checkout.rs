use std::path::{Path, PathBuf};

use git_extension_common::GitRepo;

use crate::checkout::{checkout, list_paths};
use crate::file_cache::FileCache;
use crate::{GitHfError, Result};

/// Reconstructs the files of indexed pointers under `paths` (all when empty) from local Xet data,
/// without network access.
pub(super) fn run(paths: &[PathBuf], force: bool) -> Result<()> {
    let wd = std::env::current_dir()?;
    let repo = GitRepo::open(&wd)?;
    let workdir = repo
        .worktree_dir()?
        .ok_or_else(|| GitHfError::NotSupported("checkout requires a worktree".into()))?
        .canonicalize()?;
    let pathspecs = paths
        .iter()
        .map(|path| repo_relative(&workdir, &wd.join(path)))
        .collect::<Result<Vec<_>>>()?;
    let cache = FileCache::new(&repo.common_dir()?)?;
    let summary = checkout(&wd, &cache, &pathspecs, force)?;
    let copied = match summary.copied {
        0 => String::new(),
        n => format!("; {n} copied because the filesystem cannot clone them"),
    };
    eprintln!("git-hf: checked out {} Xet files ({} bytes){copied}", summary.files, summary.bytes);
    if !summary.modified.is_empty() {
        eprintln!(
            "git-hf: left {} modified files unchanged; use --force to replace them:\n{}",
            summary.modified.len(),
            list_paths(&summary.modified)
        );
    }
    Ok(())
}

/// `path` relative to the worktree root, as Git spells paths.
fn repo_relative(workdir: &Path, path: &Path) -> Result<Vec<u8>> {
    let path = path.canonicalize()?;
    let relative = path
        .strip_prefix(workdir)
        .map_err(|_| GitHfError::NotSupported(format!("{} is outside the worktree", path.display())))?;
    let mut spec = Vec::new();
    for component in relative.components() {
        if !spec.is_empty() {
            spec.push(b'/');
        }
        let component = component.as_os_str();
        #[cfg(unix)]
        spec.extend_from_slice(std::os::unix::ffi::OsStrExt::as_bytes(component));
        #[cfg(not(unix))]
        spec.extend_from_slice(
            component
                .to_str()
                .ok_or_else(|| GitHfError::Internal("Non-UTF-8 path".into()))?
                .as_bytes(),
        );
    }
    Ok(spec)
}
