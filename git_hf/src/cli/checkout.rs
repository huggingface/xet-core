use std::path::{Component, Path, PathBuf};

use git_extension_common::GitRepo;

use crate::checkout::{CheckoutSummary, checkout, list_paths};
use crate::constants::GIT_HF_PROGRAM;
use crate::file_cache::FileCache;
use crate::{GitHfError, Result};

/// Reconstructs the files of indexed pointers under `paths` (all when empty) from local Xet data,
/// without network access.
pub(super) fn run(paths: &[PathBuf], force: bool) -> Result<()> {
    let wd = std::env::current_dir()?;
    let repo = GitRepo::open(&wd)?;
    let workdir = repo
        .worktree_dir()?
        .ok_or_else(|| GitHfError::NotSupported("checkout requires a worktree".into()))?;
    let pathspecs = paths
        .iter()
        .map(|path| repo_relative(&workdir, &wd.join(path)))
        .collect::<Result<Vec<_>>>()?;
    let cache = FileCache::new(&repo.common_dir()?)?;
    report(&checkout(&wd, &cache, &pathspecs, force)?);
    Ok(())
}

/// Prints what a checkout replaced and what it left unchanged.
pub(super) fn report(summary: &CheckoutSummary) {
    let copied = match summary.copied {
        0 => String::new(),
        n => format!("; {n} copied because the filesystem cannot clone them"),
    };
    eprintln!("{GIT_HF_PROGRAM}: checked out {} Xet files ({} bytes){copied}", summary.files, summary.bytes);
    if !summary.modified.is_empty() {
        eprintln!(
            "{GIT_HF_PROGRAM}: left {} modified files unchanged; use --force to replace them:\n{}",
            summary.modified.len(),
            list_paths(&summary.modified)
        );
    }
}

/// `path` relative to the worktree root, as Git spells paths. Like Git, resolves `.` and `..`
/// lexically, and follows no symlink inside the worktree, so a path names index entries whether or
/// not its file exists. A path that reaches the worktree another way, through a symlinked directory
/// or another spelling of it (such as a Windows `\\?\` prefix), is matched by resolving its leading
/// components until they name the worktree.
fn repo_relative(workdir: &Path, path: &Path) -> Result<Vec<u8>> {
    let mut resolved = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {},
            Component::ParentDir => {
                resolved.pop();
            },
            component => resolved.push(component),
        }
    }
    let outside = || GitHfError::NotSupported(format!("{} is outside the worktree", resolved.display()));
    let relative = match resolved.strip_prefix(workdir) {
        Ok(relative) => relative.to_path_buf(),
        Err(_) => {
            let canonical = workdir.canonicalize()?;
            let components: Vec<_> = resolved.components().collect();
            let mut prefix = PathBuf::new();
            let mut rest = None;
            for (i, component) in components.iter().enumerate() {
                prefix.push(component);
                // Only complete absolute paths: a bare Windows drive (`C:`) names that drive's current
                // directory, which may be the worktree itself.
                if prefix.is_absolute() && prefix.canonicalize().is_ok_and(|prefix| prefix == canonical) {
                    rest = Some(components[i + 1..].iter().collect::<PathBuf>());
                    break;
                }
            }
            rest.ok_or_else(outside)?
        },
    };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_resolve_lexically_within_the_worktree() -> anyhow::Result<()> {
        let workdir = Path::new("/repo");
        assert_eq!(repo_relative(workdir, Path::new("/repo/dir/./deleted"))?, b"dir/deleted");
        assert_eq!(repo_relative(workdir, Path::new("/repo/dir/../other/"))?, b"other");
        assert_eq!(repo_relative(workdir, Path::new("/repo/."))?, b"");
        assert!(repo_relative(workdir, Path::new("/repo/../elsewhere")).is_err());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_are_not_followed() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let workdir = dir.path().canonicalize()?;
        std::fs::create_dir(workdir.join("target"))?;
        std::os::unix::fs::symlink("target", workdir.join("link"))?;
        assert_eq!(repo_relative(&workdir, &workdir.join("link/file"))?, b"link/file");
        Ok(())
    }

    // A path typed through a symlink to the worktree still names its index entries.
    #[cfg(unix)]
    #[test]
    fn worktree_reached_through_a_symlink() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let workdir = dir.path().canonicalize()?.join("real");
        std::fs::create_dir_all(workdir.join("dir"))?;
        std::os::unix::fs::symlink(&workdir, dir.path().join("alias"))?;
        let alias = dir.path().join("alias");
        assert_eq!(repo_relative(&workdir, &alias.join("dir/deleted"))?, b"dir/deleted");
        assert_eq!(repo_relative(&workdir, &alias)?, b"");
        assert!(repo_relative(&workdir, &dir.path().join("elsewhere")).is_err());
        Ok(())
    }
}
