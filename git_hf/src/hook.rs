//! Installs, chains, and removes the repository's pre-push hook.
//!
//! The hook directory is resolved by Git, so `core.hooksPath` and linked worktrees are honored. A
//! pre-existing hook that git-hf did not write is preserved by renaming it to [`CHAINED_HOOK`]; the
//! git-hf hook runs it first with the same arguments and stdin, and uploads only if it succeeds.
//! Uninstalling restores it.
//!
//! Concurrent `git add` processes may install at once. Installation and removal therefore hold an
//! OS file lock on [`LOCK_FILE`] in the hooks directory and recheck ownership under it; otherwise one
//! process could rename another's freshly written git-hf hook over the preserved original.
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use git_extension_common::process_wrapping::run_git_captured_with_input_and_output;

use crate::{GitHfError, Result};

const HOOK: &str = "pre-push";
const CHAINED_HOOK: &str = "pre-push.git-hf-chained";
/// Never removed: deleting a lock file others may hold open would split later lockers.
const LOCK_FILE: &str = ".git-hf-pre-push.lock";
/// Identifies a hook written by git-hf, independent of the rest of its contents.
const HOOK_MARKER: &str = "# git-hf pre-push hook";
const HOOK_SCRIPT: &str = "#!/bin/sh
# git-hf pre-push hook: uploads staged Xet data. Remove with `git hf uninstall --local`.
exec git-hf pre-push \"$@\"
";

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum InstallOutcome {
    AlreadyInstalled,
    Installed,
    /// Installed after preserving an existing hook at this path.
    Chained(PathBuf),
}

/// Resolves the hooks directory Git uses for the repository containing `wd`.
pub(crate) fn hooks_dir(wd: &Path) -> Result<PathBuf> {
    let (output, _) =
        run_git_captured_with_input_and_output(wd, "rev-parse", ["--git-path", "hooks"])?.wait_with_output()?;
    let path = String::from_utf8(output).map_err(|_| GitHfError::Internal("Non-UTF-8 hooks path".into()))?;
    Ok(wd.join(path.trim_end_matches(['\r', '\n'])))
}

fn is_owned(path: &Path) -> Result<Option<bool>> {
    match fs::read(path) {
        Ok(content) => Ok(Some(
            content
                .split(|&b| b == b'\n')
                .any(|line| line.starts_with(HOOK_MARKER.as_bytes())),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

// Serializes hook changes within `dir`. The OS releases the lock when the file closes or the process dies.
fn lock(dir: &Path) -> Result<File> {
    fs::create_dir_all(dir)?;
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(LOCK_FILE))?;
    file.lock()?;
    Ok(file)
}

/// Ensures the git-hf hook is installed in the repository containing `wd`, chaining any foreign hook.
pub(crate) fn ensure_installed(wd: &Path) -> Result<InstallOutcome> {
    let dir = hooks_dir(wd)?;
    let hook = dir.join(HOOK);
    let chained = dir.join(CHAINED_HOOK);
    // The common case needs no lock; any other state is rechecked under it.
    if is_owned(&hook)? == Some(true) {
        return Ok(InstallOutcome::AlreadyInstalled);
    }
    let _lock = lock(&dir)?;
    let outcome = match is_owned(&hook)? {
        Some(true) => return Ok(InstallOutcome::AlreadyInstalled),
        Some(false) => {
            if chained.exists() {
                return Err(GitHfError::Hook(format!(
                    "both {} and {} exist; merge or remove one before installing",
                    hook.display(),
                    chained.display()
                )));
            }
            fs::rename(&hook, &chained)?;
            InstallOutcome::Chained(chained)
        },
        None => InstallOutcome::Installed,
    };
    write_executable(&dir, &hook)?;
    Ok(outcome)
}

// Publishes the hook atomically, already executable.
fn write_executable(dir: &Path, hook: &Path) -> Result<()> {
    let mut temp = tempfile::Builder::new().prefix(".pre-push.").tempfile_in(dir)?;
    temp.write_all(HOOK_SCRIPT.as_bytes())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temp.as_file().set_permissions(fs::Permissions::from_mode(0o755))?;
    }
    temp.persist(hook).map_err(|error| error.error)?;
    Ok(())
}

/// Removes the git-hf hook and restores a chained hook. A foreign hook at the hook path is kept.
pub(crate) fn uninstall(wd: &Path) -> Result<()> {
    let dir = hooks_dir(wd)?;
    let hook = dir.join(HOOK);
    let chained = dir.join(CHAINED_HOOK);
    if !dir.is_dir() {
        return Ok(());
    }
    let _lock = lock(&dir)?;
    match is_owned(&hook)? {
        Some(false) => return Ok(()),
        Some(true) => fs::remove_file(&hook)?,
        None => {},
    }
    if chained.exists() {
        fs::rename(&chained, &hook)?;
    }
    Ok(())
}

/// Runs the chained hook, if any, with Git's arguments and stdin. A nonzero exit aborts the push.
pub(crate) fn run_chained(wd: &Path, args: &[&OsStr], stdin: &[u8]) -> Result<()> {
    let chained = hooks_dir(wd)?.join(CHAINED_HOOK);
    // Never run git-hf's own hook as the chained hook, which would recurse.
    if is_owned(&chained)? != Some(false) {
        return Ok(());
    }
    // Git skips a hook that is not executable; a preserved one stays disabled.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if fs::metadata(&chained)?.permissions().mode() & 0o111 == 0 {
            eprintln!("git-hf: skipping {} because it is not executable", chained.display());
            return Ok(());
        }
    }
    // Git for Windows runs hooks through its shell; elsewhere the hook's shebang selects the interpreter.
    let mut command = if cfg!(windows) {
        let mut command = Command::new("sh");
        command.arg(&chained);
        command
    } else {
        Command::new(&chained)
    };
    let mut child = command.args(args).current_dir(wd).stdin(Stdio::piped()).spawn()?;
    // The hook may exit without reading stdin; a broken pipe is then its own business.
    if let Err(error) = child.stdin.take().unwrap().write_all(stdin)
        && error.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(error.into());
    }
    let status = child.wait()?;
    if !status.success() {
        return Err(GitHfError::Hook(format!("{} failed with {status}", chained.display())));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use git_extension_common::process_wrapping::run_git_captured;
    use git_extension_common::test_utils::TestRepo;
    use serial_test::serial;

    use super::*;

    #[test]
    #[serial(env_var_write_read)]
    fn install_is_idempotent_and_uninstall_removes_it() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        assert_eq!(ensure_installed(repo.path())?, InstallOutcome::Installed);
        assert_eq!(ensure_installed(repo.path())?, InstallOutcome::AlreadyInstalled);
        let hook = hooks_dir(repo.path())?.join(HOOK);
        assert_eq!(fs::read_to_string(&hook)?, HOOK_SCRIPT);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(fs::metadata(&hook)?.permissions().mode() & 0o777, 0o755);
        }
        uninstall(repo.path())?;
        assert!(!hook.exists());
        uninstall(repo.path())?;
        Ok(())
    }

    // Every concurrent installer must leave the original hook preserved, exactly once.
    #[cfg(unix)]
    #[test]
    #[serial(env_var_write_read)]
    fn concurrent_installs_preserve_the_original_hook() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let path = repo.path().to_owned();
        let dir = hooks_dir(&path)?;
        let foreign = "#!/bin/sh\necho original\n";
        for _ in 0..20 {
            let _ = fs::remove_file(dir.join(CHAINED_HOOK));
            fs::create_dir_all(&dir)?;
            fs::write(dir.join(HOOK), foreign)?;
            let outcomes = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..8).map(|_| scope.spawn(|| ensure_installed(&path))).collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect::<Result<Vec<_>>>()
            })?;
            assert_eq!(outcomes.iter().filter(|o| matches!(o, InstallOutcome::Chained(_))).count(), 1);
            assert_eq!(fs::read_to_string(dir.join(CHAINED_HOOK))?, foreign);
            assert_eq!(fs::read_to_string(dir.join(HOOK))?, HOOK_SCRIPT);
        }
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn honors_core_hooks_path() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        run_git_captured(repo.path(), "config", ["core.hooksPath", "custom-hooks"])?;
        ensure_installed(repo.path())?;
        assert!(repo.path().join("custom-hooks").join(HOOK).exists());
        assert!(!repo.path().join(".git/hooks").join(HOOK).exists());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    #[serial(env_var_write_read)]
    fn foreign_hook_is_chained_with_arguments_and_stdin_then_restored() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let repo = TestRepo::new("main")?;
        let dir = hooks_dir(repo.path())?;
        fs::create_dir_all(&dir)?;
        let foreign = "#!/bin/sh\necho \"$@\" > args\ncat > input\nexit \"$(cat status 2>/dev/null || echo 0)\"\n";
        fs::write(dir.join(HOOK), foreign)?;
        fs::set_permissions(dir.join(HOOK), fs::Permissions::from_mode(0o755))?;

        assert_eq!(ensure_installed(repo.path())?, InstallOutcome::Chained(dir.join(CHAINED_HOOK)));
        assert_eq!(fs::read_to_string(dir.join(CHAINED_HOOK))?, foreign);
        let args = [OsStr::new("origin"), OsStr::new("https://example.com/repo")];
        run_chained(repo.path(), &args, b"refs/heads/main abc refs/heads/main def\n")?;
        assert_eq!(fs::read_to_string(repo.path().join("args"))?, "origin https://example.com/repo\n");
        assert_eq!(fs::read(repo.path().join("input"))?, b"refs/heads/main abc refs/heads/main def\n");
        fs::write(repo.path().join("status"), "3")?;
        assert!(matches!(run_chained(repo.path(), &args, b""), Err(GitHfError::Hook(_))));

        // A non-executable hook, which Git would skip, is preserved but not run.
        fs::set_permissions(dir.join(CHAINED_HOOK), fs::Permissions::from_mode(0o644))?;
        fs::remove_file(repo.path().join("input"))?;
        run_chained(repo.path(), &args, b"ignored\n")?;
        assert!(!repo.path().join("input").exists());
        fs::set_permissions(dir.join(CHAINED_HOOK), fs::Permissions::from_mode(0o755))?;
        fs::remove_file(repo.path().join("status"))?;

        // A second foreign hook cannot be chained over the first.
        uninstall(repo.path())?;
        assert_eq!(fs::read_to_string(dir.join(HOOK))?, foreign);
        assert!(!dir.join(CHAINED_HOOK).exists());
        fs::copy(dir.join(HOOK), dir.join(CHAINED_HOOK))?;
        assert!(matches!(ensure_installed(repo.path()), Err(GitHfError::Hook(_))));
        Ok(())
    }
}
