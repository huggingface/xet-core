//! Installs and removes the repository's pre-push hook.
//!
//! The hook directory is resolved by Git, so `core.hooksPath` and linked worktrees are honored. A
//! pre-existing hook that git-hf did not write is left in place, as git-lfs does: renaming it would
//! break hook managers that dispatch on the hook's name or directory, such as husky. Its owner adds
//! [`HOOK_COMMAND`] to it instead. On Windows, a `pre-push.exe` counts as such a hook, since Git runs
//! it only while `pre-push` is absent.
//!
//! Concurrent `git add` processes may install at once. Each publishes the same script without
//! replacing an existing file, so no lock is needed.
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use git_extension_common::process_wrapping::run_git_captured_with_input_and_output;

use crate::constants::GIT_HF_PROGRAM;
use crate::{GitHfError, Result};

const HOOK: &str = "pre-push";
/// Identifies a hook written by git-hf, independent of the rest of its contents.
const HOOK_MARKER: &str = "# git-hf pre-push hook";
/// The line a hook git-hf did not write needs, before anything else reads its standard input, to upload staged data.
pub(crate) const HOOK_COMMAND: &str = "git-hf pre-push \"$@\" || exit 1";
const HOOK_SCRIPT: &str = "#!/bin/sh
# git-hf pre-push hook: uploads staged Xet data. Remove with `git hf uninstall --local`.
exec git-hf pre-push \"$@\"
";

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum InstallOutcome {
    /// The git-hf hook is installed, or an existing hook already calls git-hf.
    AlreadyInstalled,
    Installed,
    /// A hook git-hf did not write exists at this path and was left unchanged.
    ForeignHook(PathBuf),
}

impl InstallOutcome {
    /// Tells the user how to make a foreign hook upload staged data; `None` for other outcomes.
    pub(crate) fn foreign_hook_notice(&self) -> Option<String> {
        let InstallOutcome::ForeignHook(path) = self else {
            return None;
        };
        let notice = format!("{} was not written by {GIT_HF_PROGRAM} and was left unchanged.", path.display());
        // Git runs a `pre-push.exe` directly, so it has no script line to add.
        if cfg!(windows) && path.extension().is_some_and(|ext| ext == "exe") {
            return Some(format!(
                "{notice} To upload staged files when you push, make it run `{GIT_HF_PROGRAM} pre-push` with the \
                 hook's arguments and standard input."
            ));
        }
        Some(format!(
            "{notice} To upload staged files when you push, add `{HOOK_COMMAND}` to it right after the `#!` line, \
             before any other command. If the rest of the hook also reads standard input, see the git-hf README."
        ))
    }
}

/// Resolves the hooks directory Git uses for the repository containing `wd`.
pub(crate) fn hooks_dir(wd: &Path) -> Result<PathBuf> {
    let (output, _) =
        run_git_captured_with_input_and_output(wd, "rev-parse", ["--git-path", "hooks"])?.wait_with_output()?;
    let path = String::from_utf8(output).map_err(|_| GitHfError::Internal("Non-UTF-8 hooks path".into()))?;
    Ok(wd.join(path.trim_end_matches(['\r', '\n'])))
}

/// Reads the hook at `path`, if it exists.
fn read_hook(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(content) => Ok(Some(content)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn is_owned(content: &[u8]) -> bool {
    content
        .split(|&b| b == b'\n')
        .any(|line| line.starts_with(HOOK_MARKER.as_bytes()))
}

fn calls_git_hf(content: &[u8]) -> bool {
    [&b"git-hf pre-push"[..], b"git hf pre-push"]
        .iter()
        .any(|command| content.windows(command.len()).any(|w| w == *command))
}

/// Ensures pushes from the repository containing `wd` run git-hf: installs the git-hf hook unless
/// another hook exists, which is left unchanged.
pub(crate) fn ensure_installed(wd: &Path) -> Result<InstallOutcome> {
    let dir = hooks_dir(wd)?;
    let hook = dir.join(HOOK);
    if cfg!(windows) {
        let exe = dir.join(format!("{HOOK}.exe"));
        if exe.exists() {
            return Ok(InstallOutcome::ForeignHook(exe));
        }
    }
    if let Some(content) = read_hook(&hook)? {
        return Ok(classify(&hook, &content));
    }
    fs::create_dir_all(&dir)?;
    let mut temp = tempfile::Builder::new().prefix(".pre-push.").tempfile_in(&dir)?;
    temp.write_all(HOOK_SCRIPT.as_bytes())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temp.as_file().set_permissions(fs::Permissions::from_mode(0o755))?;
    }
    // Never replaces a hook created meanwhile, by a concurrent install or by the user.
    match temp.persist_noclobber(&hook) {
        Ok(_) => Ok(InstallOutcome::Installed),
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            let content = read_hook(&hook)?.unwrap_or_default();
            Ok(classify(&hook, &content))
        },
        Err(error) => Err(error.error.into()),
    }
}

fn classify(hook: &Path, content: &[u8]) -> InstallOutcome {
    if is_owned(content) || calls_git_hf(content) {
        InstallOutcome::AlreadyInstalled
    } else {
        InstallOutcome::ForeignHook(hook.to_owned())
    }
}

/// Removes the git-hf hook. A hook git-hf did not write is kept.
pub(crate) fn uninstall(wd: &Path) -> Result<()> {
    let hook = hooks_dir(wd)?.join(HOOK);
    if read_hook(&hook)?.is_some_and(|content| is_owned(&content)) {
        fs::remove_file(&hook)?;
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

    // Exactly one concurrent installer writes the hook; the others find it.
    #[test]
    #[serial(env_var_write_read)]
    fn concurrent_installs_write_the_hook_once() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let path = repo.path().to_owned();
        let hook = hooks_dir(&path)?.join(HOOK);
        for _ in 0..20 {
            let _ = fs::remove_file(&hook);
            let outcomes = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..8).map(|_| scope.spawn(|| ensure_installed(&path))).collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect::<Result<Vec<_>>>()
            })?;
            assert_eq!(outcomes.iter().filter(|o| **o == InstallOutcome::Installed).count(), 1);
            assert!(
                outcomes
                    .iter()
                    .all(|o| matches!(o, InstallOutcome::Installed | InstallOutcome::AlreadyInstalled))
            );
            assert_eq!(fs::read_to_string(&hook)?, HOOK_SCRIPT);
        }
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn foreign_hook_is_left_unchanged() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let dir = hooks_dir(repo.path())?;
        fs::create_dir_all(&dir)?;
        let foreign = "#!/usr/bin/env python3\nprint('hook')\n";
        fs::write(dir.join(HOOK), foreign)?;
        let outcome = ensure_installed(repo.path())?;
        assert_eq!(outcome, InstallOutcome::ForeignHook(dir.join(HOOK)));
        assert!(outcome.foreign_hook_notice().unwrap().contains(HOOK_COMMAND));
        uninstall(repo.path())?;
        assert_eq!(fs::read_to_string(dir.join(HOOK))?, foreign);

        // A hook that already calls git-hf needs nothing more.
        for command in ["git-hf pre-push \"$@\"", "git hf pre-push \"$1\" \"$2\""] {
            fs::write(dir.join(HOOK), format!("#!/bin/sh\nnpm test\n{command}\n"))?;
            assert_eq!(ensure_installed(repo.path())?, InstallOutcome::AlreadyInstalled);
        }
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    #[serial(env_var_write_read)]
    fn windows_leaves_an_exe_hook_in_charge() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let dir = hooks_dir(repo.path())?;
        fs::create_dir_all(&dir)?;
        fs::write(dir.join("pre-push.exe"), b"MZ")?;
        assert_eq!(ensure_installed(repo.path())?, InstallOutcome::ForeignHook(dir.join("pre-push.exe")));
        assert!(!dir.join(HOOK).exists());
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
}
