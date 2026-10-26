//! Reconstructs the files of indexed Xet pointers into the worktree from local Xet data, without
//! network access.
//!
//! Candidates are stage-zero index entries for regular files whose blob is a pointer. Unmerged,
//! submodule, symlink, and sparse-excluded (skip-worktree) entries are skipped. A worktree file is
//! replaced only while it still holds exactly its pointer, unless forced; any other content is left
//! untouched. Every file to replace must be available locally before any is replaced.
//!
//! Checkout changes worktree content outside Git, so it records each replaced file's stat data in
//! the index. Otherwise Git would see the files as modified and rerun the clean filter over each.
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use xet_runtime::utils::UniqueId;

use crate::file_cache::FileCache;
use crate::{GitHfError, MAX_POINTER_SIZE, Result, XetPointer};

/// `GIT_INDEX_ENTRY_SKIP_WORKTREE`, in an index entry's extended flags.
const SKIP_WORKTREE: u16 = 1 << 14;
/// `GIT_INDEX_ENTRY_STAGEMASK`, in an index entry's flags.
const STAGE_MASK: u16 = 0x3000;
/// Paths listed in a message before the rest are counted.
const LISTED_PATHS: usize = 10;

/// Counts reported after a checkout.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct CheckoutSummary {
    pub(crate) files: usize,
    pub(crate) bytes: u64,
    /// Replaced files that were copied because the filesystem cannot clone them.
    pub(crate) copied: usize,
    /// Paths left unchanged because they no longer hold their pointer.
    pub(crate) modified: Vec<String>,
}

struct Target {
    /// Repository-relative path, as Git stores it.
    path: Vec<u8>,
    blob: git2::Oid,
    /// The pointer blob's exact content.
    blob_data: Vec<u8>,
    pointer: XetPointer,
}

impl Target {
    fn display(&self) -> String {
        String::from_utf8_lossy(&self.path).into_owned()
    }
}

/// Replaces the worktree files of indexed pointers under `pathspecs` (repository-relative paths;
/// empty selects everything) with their reconstructed content.
pub(crate) fn checkout(wd: &Path, cache: &FileCache, pathspecs: &[Vec<u8>], force: bool) -> Result<CheckoutSummary> {
    let repo = git2::Repository::discover(wd)?;
    let workdir = repo
        .workdir()
        .ok_or_else(|| GitHfError::NotSupported("checkout requires a worktree".into()))?
        .canonicalize()?;
    let mut summary = CheckoutSummary::default();
    let mut selected = Vec::new();
    for target in indexed_pointers(&repo, pathspecs)? {
        let dest = workdir.join(path_from_git(&target.path)?);
        if !is_regular_file_in(&workdir, &dest)? {
            continue;
        }
        if force || holds(&dest, &target.blob_data)? {
            selected.push((target, dest));
        } else {
            summary.modified.push(target.display());
        }
    }

    let missing: Vec<_> = selected
        .iter()
        .filter(|(target, _)| !cache.contains(&target.pointer))
        .map(|(target, _)| target.display())
        .collect();
    if !missing.is_empty() {
        return Err(GitHfError::NotCached(format!(
            "{} files; run `git hf fetch` first:\n{}",
            missing.len(),
            list_paths(&missing)
        )));
    }

    let mut written = Vec::new();
    for (target, dest) in selected {
        let expected = (!force).then_some(target.blob_data.as_slice());
        match materialize(&cache.path(&target.pointer.hash), &dest, expected)? {
            Some(cloned) => {
                summary.files += 1;
                summary.bytes += target.pointer.size;
                summary.copied += usize::from(!cloned);
                written.push((target, dest));
            },
            None => summary.modified.push(target.display()),
        }
    }
    refresh_index(&repo, &written)?;
    Ok(summary)
}

/// Formats paths one per line, listing at most [`LISTED_PATHS`].
pub(crate) fn list_paths(paths: &[String]) -> String {
    let mut lines: Vec<_> = paths.iter().take(LISTED_PATHS).map(|path| format!("  {path}")).collect();
    if paths.len() > LISTED_PATHS {
        lines.push(format!("  ... and {} more", paths.len() - LISTED_PATHS));
    }
    lines.join("\n")
}

fn indexed_pointers(repo: &git2::Repository, pathspecs: &[Vec<u8>]) -> Result<Vec<Target>> {
    let index = repo.index()?;
    let odb = repo.odb()?;
    let mut targets = Vec::new();
    let mut invalid = Vec::new();
    for entry in index.iter() {
        if entry.flags & STAGE_MASK != 0
            || entry.mode & 0o170000 != 0o100000
            || entry.flags_extended & SKIP_WORKTREE != 0
            || !is_selected(&entry.path, pathspecs)
        {
            continue;
        }
        let (size, _) = odb.read_header(entry.id)?;
        if size > MAX_POINTER_SIZE {
            continue;
        }
        let blob_data = odb.read(entry.id)?.data().to_vec();
        match XetPointer::parse(&blob_data) {
            Ok(Some(pointer)) => targets.push(Target {
                path: entry.path,
                blob: entry.id,
                blob_data,
                pointer,
            }),
            Ok(None) => {},
            Err(error) => invalid.push(format!("{}: {error}", String::from_utf8_lossy(&entry.path))),
        }
    }
    if !invalid.is_empty() {
        return Err(GitHfError::NotSupported(format!(
            "the index contains pointers git-hf cannot check out:\n{}",
            list_paths(&invalid)
        )));
    }
    Ok(targets)
}

fn is_selected(path: &[u8], pathspecs: &[Vec<u8>]) -> bool {
    pathspecs.is_empty()
        || pathspecs.iter().any(|spec| {
            spec.is_empty() || path == spec.as_slice() || (path.starts_with(spec) && path[spec.len()] == b'/')
        })
}

fn path_from_git(path: &[u8]) -> Result<PathBuf> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        Ok(PathBuf::from(std::ffi::OsStr::from_bytes(path)))
    }
    #[cfg(not(unix))]
    {
        let path = std::str::from_utf8(path).map_err(|_| GitHfError::Internal("Non-UTF-8 index path".into()))?;
        Ok(PathBuf::from(path))
    }
}

/// Whether `dest` is a regular file reached without leaving `workdir` through a symlinked directory.
fn is_regular_file_in(workdir: &Path, dest: &Path) -> Result<bool> {
    match fs::symlink_metadata(dest) {
        Ok(metadata) if metadata.is_file() => {},
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    }
    Ok(dest.parent().unwrap().canonicalize()?.starts_with(workdir))
}

/// Whether `path` holds exactly `data`. Reads the file only when the sizes match.
fn holds(path: &Path, data: &[u8]) -> Result<bool> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.len() == data.len() as u64 => Ok(fs::read(path)? == data),
        Ok(_) => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

// Removes a partially written file unless it was renamed into place.
struct Partial(PathBuf);

impl Drop for Partial {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// Replaces `dest` with a clone (copy-on-write) or copy of `source`, keeping `dest`'s permissions.
/// Unlike a hardlink, the result is independent of `source`: writing one never changes the other.
/// With `expected`, replaces `dest` only if it still holds exactly those bytes. Returns whether
/// `source` was cloned, or `None` if `dest` changed.
fn materialize(source: &Path, dest: &Path, expected: Option<&[u8]>) -> Result<Option<bool>> {
    let permissions = fs::metadata(dest)?.permissions();
    let name = dest.file_name().unwrap().to_string_lossy();
    // Cloning requires a destination that does not exist yet.
    let temp = Partial(dest.with_file_name(format!(".{name}.git-hf-{}.tmp", UniqueId::new())));
    let cloned = reflink_copy::reflink_or_copy(source, &temp.0)?.is_none();
    fs::set_permissions(&temp.0, permissions)?;
    if let Some(expected) = expected
        && !holds(dest, expected)?
    {
        return Ok(None);
    }
    fs::rename(&temp.0, dest)?;
    Ok(Some(cloned))
}

/// Records the stat data of the files checkout wrote in their index entries, unless another process
/// changed an entry meanwhile.
fn refresh_index(repo: &git2::Repository, written: &[(Target, PathBuf)]) -> Result<()> {
    if written.is_empty() {
        return Ok(());
    }
    let mut stats = HashMap::new();
    let mut latest = UNIX_EPOCH;
    for (target, dest) in written {
        let metadata = fs::symlink_metadata(dest)?;
        latest = latest.max(metadata.modified()?);
        stats.insert(target.path.as_slice(), (target.blob, metadata));
    }
    // Git rechecks the content of an entry whose file is not older than the index, which would run
    // the clean filter again. Timestamps may have one-second granularity.
    wait_past_second_of(latest);

    let mut index = repo.index()?;
    index.read(true)?;
    let updated: Vec<_> = index
        .iter()
        .filter_map(|mut entry| {
            let (blob, metadata) = stats.get(entry.path.as_slice())?;
            if entry.flags & STAGE_MASK != 0 || entry.id != *blob {
                return None;
            }
            set_stat(&mut entry, metadata);
            Some(entry)
        })
        .collect();
    for entry in &updated {
        index.add(entry)?;
    }
    index.write()?;
    Ok(())
}

fn wait_past_second_of(time: SystemTime) {
    let second = time.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    let next = UNIX_EPOCH + Duration::from_secs(second + 1);
    if let Ok(remaining) = next.duration_since(SystemTime::now()) {
        std::thread::sleep(remaining);
    }
}

// Git stores 32-bit stat fields, truncating larger values.
#[cfg(unix)]
fn set_stat(entry: &mut git2::IndexEntry, metadata: &fs::Metadata) {
    use std::os::unix::fs::MetadataExt;
    entry.ctime = git2::IndexTime::new(metadata.ctime() as i32, metadata.ctime_nsec() as u32);
    entry.mtime = git2::IndexTime::new(metadata.mtime() as i32, metadata.mtime_nsec() as u32);
    entry.dev = metadata.dev() as u32;
    entry.ino = metadata.ino() as u32;
    entry.uid = metadata.uid();
    entry.gid = metadata.gid();
    entry.file_size = metadata.len() as u32;
}

#[cfg(not(unix))]
fn set_stat(entry: &mut git2::IndexEntry, metadata: &fs::Metadata) {
    let time = |time: std::io::Result<SystemTime>| {
        let since = time.ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok()).unwrap_or_default();
        git2::IndexTime::new(since.as_secs() as i32, since.subsec_nanos())
    };
    // Git for Windows reports the creation time as ctime.
    entry.ctime = time(metadata.created());
    entry.mtime = time(metadata.modified());
    entry.file_size = metadata.len() as u32;
}

#[cfg(test)]
mod tests {
    use git_extension_common::process_wrapping::{run_git_captured, run_git_captured_with_input_and_output};
    use git_extension_common::test_utils::TestRepo;
    use serial_test::serial;
    use xet_core_structures::merklehash::MerkleHash;

    use super::*;

    struct Fixture {
        repo: TestRepo,
        cache: FileCache,
        files: Vec<(&'static str, Vec<u8>, XetPointer)>,
    }

    // Commits a pointer for each file and caches the files whose `cached` flag is set.
    fn fixture(files: &[(&'static str, &[u8], bool)]) -> anyhow::Result<Fixture> {
        let repo = TestRepo::new("main")?;
        let cache = FileCache::new(&repo.path().join(".git"))?;
        let mut committed = Vec::new();
        for (i, (path, data, cached)) in files.iter().enumerate() {
            let pointer = XetPointer::new(MerkleHash::from([i as u64 + 1; 4]), data.len() as u64);
            let dest = repo.path().join(path);
            fs::create_dir_all(dest.parent().unwrap())?;
            fs::write(&dest, pointer.to_string())?;
            if *cached {
                let temp = cache.temp_path()?;
                fs::write(&temp, data)?;
                cache.publish(temp, &pointer.hash)?;
            }
            committed.push((*path, data.to_vec(), pointer));
        }
        run_git_captured(repo.path(), "add", ["-A"])?;
        run_git_captured(repo.path(), "commit", ["-m", "pointers"])?;
        Ok(Fixture {
            repo,
            cache,
            files: committed,
        })
    }

    fn status(repo: &TestRepo) -> anyhow::Result<String> {
        let (out, _) =
            run_git_captured_with_input_and_output(repo.path(), "status", ["--porcelain"])?.wait_with_output()?;
        Ok(String::from_utf8(out)?)
    }

    #[test]
    #[serial(env_var_write_read)]
    fn replaces_pointers_and_refreshes_the_index() -> anyhow::Result<()> {
        let f = fixture(&[
            ("a", b"alpha data", true),
            ("dir/b", b"beta data", true),
            ("c", b"gamma", true),
        ])?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            run_git_captured(f.repo.path(), "update-index", ["--chmod=+x", "dir/b"])?;
            fs::set_permissions(f.repo.path().join("dir/b"), fs::Permissions::from_mode(0o755))?;
            run_git_captured(f.repo.path(), "commit", ["-m", "exec"])?;
        }
        fs::write(f.repo.path().join("c"), b"edited")?;

        let summary = checkout(f.repo.path(), &f.cache, &[], false)?;
        assert_eq!((summary.files, summary.bytes), (2, 19));
        assert_eq!(summary.modified, ["c"]);
        assert_eq!(fs::read(f.repo.path().join("a"))?, b"alpha data");
        assert_eq!(fs::read(f.repo.path().join("dir/b"))?, b"beta data");
        assert_eq!(fs::read(f.repo.path().join("c"))?, b"edited");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(fs::metadata(f.repo.path().join("dir/b"))?.permissions().mode() & 0o111, 0o111);
        }
        // Without the clean filter, Git would report a and dir/b as modified unless their stat data
        // were recorded.
        assert_eq!(status(&f.repo)?, " M c\n");

        // A materialized file is independent of its cache entry.
        fs::write(f.repo.path().join("a"), b"changed")?;
        assert_eq!(fs::read(f.cache.path(&f.files[0].2.hash))?, b"alpha data");

        // Nothing holds a pointer now; force replaces modified and materialized files alike.
        assert_eq!(checkout(f.repo.path(), &f.cache, &[], false)?.files, 0);
        let forced = checkout(f.repo.path(), &f.cache, &[], true)?;
        assert_eq!((forced.files, forced.modified.len()), (3, 0));
        assert_eq!(fs::read(f.repo.path().join("a"))?, b"alpha data");
        assert_eq!(fs::read(f.repo.path().join("c"))?, b"gamma");
        assert_eq!(status(&f.repo)?, "");
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn missing_cache_entries_fail_before_any_replacement() -> anyhow::Result<()> {
        let f = fixture(&[("a", b"alpha", true), ("b", b"beta", false)])?;
        let error = checkout(f.repo.path(), &f.cache, &[], false).unwrap_err();
        assert!(matches!(&error, GitHfError::NotCached(message) if message.contains("  b")), "{error}");
        for (path, _, pointer) in &f.files {
            assert_eq!(fs::read(f.repo.path().join(path))?, pointer.to_string().as_bytes());
        }
        // Restricting the checkout to cached paths succeeds.
        assert_eq!(checkout(f.repo.path(), &f.cache, &[b"a".to_vec()], false)?.files, 1);
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn selects_paths_and_skips_excluded_entries() -> anyhow::Result<()> {
        let f = fixture(&[
            ("dir/a", b"alpha", true),
            ("dir2/b", b"beta", true),
            ("dir/skipped", b"gamma", true),
        ])?;
        run_git_captured(f.repo.path(), "update-index", ["--skip-worktree", "dir/skipped"])?;
        // `dir` selects dir/a but not dir2/b; skip-worktree entries are never replaced.
        let summary = checkout(f.repo.path(), &f.cache, &[b"dir".to_vec()], false)?;
        assert_eq!(summary.files, 1);
        assert_eq!(fs::read(f.repo.path().join("dir/a"))?, b"alpha");
        assert_eq!(fs::read(f.repo.path().join("dir2/b"))?, f.files[1].2.to_string().as_bytes());
        assert_eq!(fs::read(f.repo.path().join("dir/skipped"))?, f.files[2].2.to_string().as_bytes());

        // A deleted file stays deleted.
        fs::remove_file(f.repo.path().join("dir2/b"))?;
        assert_eq!(checkout(f.repo.path(), &f.cache, &[], false)?.files, 0);
        assert!(!f.repo.path().join("dir2/b").exists());
        Ok(())
    }

    #[test]
    fn materialized_files_are_independent_and_temporaries_are_removed() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let (source, dest) = (dir.path().join("source"), dir.path().join("dest"));
        fs::write(&source, b"content")?;
        fs::write(&dest, b"pointer")?;
        // The destination changed after it was selected: nothing is replaced.
        assert_eq!(materialize(&source, &dest, Some(b"other"))?, None);
        assert_eq!(fs::read(&dest)?, b"pointer");
        assert!(materialize(&source, &dest, Some(b"pointer"))?.is_some());
        fs::write(&dest, b"modified")?;
        assert_eq!(fs::read(&source)?, b"content");
        let mut names: Vec<_> = fs::read_dir(dir.path())?.map(|e| e.unwrap().file_name()).collect();
        names.sort();
        assert_eq!(names, ["dest", "source"]);
        Ok(())
    }
}
