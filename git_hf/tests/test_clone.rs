//! `git hf clone` against local source repositories, run through the real binary. A local source is
//! not a Hub repository, so pull's download fails before any network access.
#![cfg(unix)]

use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::Result;
use git_extension_common::process_wrapping::{run_git_captured, run_git_captured_with_input_and_output};
use git_extension_common::test_utils::TestRepo;
use serial_test::serial;
use xet_runtime::utils::EnvVarGuard;

/// Puts the built `git-hf` first on PATH, where Git finds the filter clone registers.
fn git_hf_on_path() -> Result<EnvVarGuard> {
    let binary = Path::new(env!("CARGO_BIN_EXE_git-hf"));
    let path = std::env::join_paths(
        std::iter::once(binary.parent().unwrap().to_owned())
            .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
    )?;
    Ok(EnvVarGuard::set("PATH", path))
}

fn commit(repo: &TestRepo, files: &[(&str, &str)]) -> Result<()> {
    for (path, content) in files {
        fs::write(repo.path().join(path), content)?;
    }
    run_git_captured(repo.path(), "add", ["-A"])?;
    run_git_captured(repo.path(), "commit", ["-m", "files"])?;
    Ok(())
}

fn git_hf_clone(cwd: &Path, args: &[&Path]) -> Result<std::process::Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_git-hf"))
        .current_dir(cwd)
        .arg("clone")
        .args(args)
        .output()?)
}

fn local_config(repo: &Path, key: &str) -> Result<String> {
    let (value, _) = run_git_captured_with_input_and_output(repo, "config", ["--local", "--default", "", key])?
        .wait_with_output()?;
    Ok(String::from_utf8(value)?.trim().to_owned())
}

/// A repository that uses the filter is checked out as pointers with the filter in place, even with no
/// global installation, then set up for git-hf. Its failed download leaves that checkout for pull.
#[test]
#[serial(env_var_write_read)]
fn clone_sets_up_repositories_that_use_the_filter() -> Result<()> {
    let _path = git_hf_on_path()?;
    let source = TestRepo::new("main")?;
    let pointer = git_hf::XetPointer::new(git_hf::HexHash::from_hex(&"1".repeat(64)).unwrap(), 5).to_string();
    // The source has no filter configured, so these commit as written.
    commit(
        &source,
        &[
            (".gitattributes", "* filter=hf -text\n.gitattributes filter=\n"),
            ("small", "text\n"),
            ("large", &pointer),
        ],
    )?;
    let parent = tempfile::tempdir()?;

    // Without a destination, Git chooses one; the filter reports it.
    let output = git_hf_clone(parent.path(), &[source.path()])?;
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    let copy = parent.path().join(source.path().file_name().unwrap()).canonicalize()?;
    assert!(stderr.contains("run `git-hf install --local` and `git-hf pull` in each"), "{stderr}");
    assert!(stderr.contains(&format!("\n  {}", copy.display())), "{stderr}");
    assert_eq!(fs::read_to_string(copy.join("large"))?, pointer);
    assert_eq!(fs::read_to_string(copy.join("small"))?, "text\n");
    assert_eq!(local_config(&copy, "filter.hf.process")?, "git-hf filter-process --smudge-passthrough");
    assert_eq!(local_config(&copy, "filter.hf.required")?, "true");
    assert!(fs::read_to_string(copy.join(".git/hooks/pre-push"))?.contains("git-hf pre-push"));
    let (status, _) = run_git_captured_with_input_and_output(&copy, "status", ["--porcelain"])?.wait_with_output()?;
    assert!(status.is_empty(), "{}", String::from_utf8_lossy(&status));

    // A clone that checks nothing out never starts the filter and is left as Git made it.
    let unchecked = parent.path().join("unchecked");
    assert!(
        git_hf_clone(parent.path(), &[Path::new("--no-checkout"), source.path(), &unchecked])?
            .status
            .success()
    );
    assert_eq!(local_config(&unchecked, "filter.hf.process")?, "");
    Ok(())
}

/// A repository that does not use the filter is a plain `git clone`.
#[test]
#[serial(env_var_write_read)]
fn clone_leaves_other_repositories_as_git_clones() -> Result<()> {
    let _path = git_hf_on_path()?;
    let source = TestRepo::new("main")?;
    commit(&source, &[("a", "alpha\n")])?;
    let parent = tempfile::tempdir()?;
    let copy = parent.path().join("copy");
    let output = git_hf_clone(parent.path(), &[source.path(), &copy])?;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(fs::read_to_string(copy.join("a"))?, "alpha\n");
    assert_eq!(local_config(&copy, "filter.hf.process")?, "");
    assert!(!copy.join(".git/hooks/pre-push").exists());
    Ok(())
}

/// A submodule that uses the filter is set up even when the clone itself does not use it.
#[test]
#[serial(env_var_write_read)]
fn clone_sets_up_submodules_that_use_the_filter() -> Result<()> {
    let _path = git_hf_on_path()?;
    let source = TestRepo::new("main")?;
    // Git refuses local submodule clones unless allowed.
    run_git_captured(source.path(), "config", ["--global", "protocol.file.allow", "always"])?;
    let dir = tempfile::tempdir()?;
    let sub = dir.path().join("sub");
    run_git_captured(dir.path(), "init", ["-b", "main", "sub"])?;
    let pointer = git_hf::XetPointer::new(git_hf::HexHash::from_hex(&"1".repeat(64)).unwrap(), 5).to_string();
    fs::write(sub.join(".gitattributes"), "* filter=hf -text\n.gitattributes filter=\n")?;
    fs::write(sub.join("large"), &pointer)?;
    run_git_captured(&sub, "add", ["-A"])?;
    run_git_captured(&sub, "commit", ["-m", "pointer"])?;
    commit(&source, &[("a", "alpha\n")])?;
    run_git_captured(source.path(), "submodule", [OsStr::new("add"), sub.as_os_str(), OsStr::new("sub")])?;
    run_git_captured(source.path(), "commit", ["-m", "submodule"])?;

    let parent = tempfile::tempdir()?;
    let copy = parent.path().join("copy");
    let output = git_hf_clone(parent.path(), &[Path::new("--recurse-submodules"), source.path(), &copy])?;
    // The submodule's pointer cannot be downloaded from a local source.
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    let copy_sub = copy.canonicalize()?.join("sub");
    assert!(stderr.contains(&format!("\n  {}", copy_sub.display())), "{stderr}");
    assert_eq!(local_config(&copy, "filter.hf.process")?, "");
    assert_eq!(local_config(&copy_sub, "filter.hf.process")?, "git-hf filter-process --smudge-passthrough");
    assert_eq!(fs::read_to_string(copy_sub.join("large"))?, pointer);
    assert_eq!(fs::read_to_string(copy.join("a"))?, "alpha\n");
    Ok(())
}
