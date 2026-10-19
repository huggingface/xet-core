use std::fs;
use std::io::{Cursor, Write};

use anyhow::Result;
use git_extension_common::GitRepo;
use git_extension_common::process_wrapping::{run_git_captured, run_git_captured_with_input_and_output};
use git_extension_common::test_utils::TestRepo;
use git_hf::XetPointer;
use serial_test::serial;
use xet_core_structures::metadata_shard::streaming_shard::MDBMinimalShard;
use xet_core_structures::xorb_object::XorbObject;

#[test]
#[serial(env_var_write_read)]
fn git_add_checkout_and_staged_reconstruction() -> Result<()> {
    let repo = TestRepo::new("main")?;
    let binary = env!("CARGO_BIN_EXE_git-hf").replace('\'', "'\\''");
    repo.set_config("filter.hf.process", &format!("'{binary}' filter-process --smudge-passthrough"))?;
    repo.set_config("filter.hf.required", "true")?;
    fs::write(repo.path().join(".gitattributes"), "* filter=hf -text\n.gitattributes filter=\n")?;
    let mut data = vec![b'a'; 150000];
    data[0] = 0;
    fs::write(repo.path().join("large"), &data)?;
    fs::write(repo.path().join("small"), b"hello\n")?;
    run_git_captured(repo.path(), "add", ["."])?;
    let (blob, _) = run_git_captured_with_input_and_output(repo.path(), "show", [":large"])?.wait_with_output()?;
    let pointer = XetPointer::parse(&blob)?.unwrap();
    assert_eq!(pointer.size, data.len() as u64);
    assert_eq!(fs::read(repo.path().join("large"))?, data);
    let (small, _) = run_git_captured_with_input_and_output(repo.path(), "show", [":small"])?.wait_with_output()?;
    assert_eq!(small, b"hello\n");

    let staging = GitRepo::open(repo.path())?.common_dir()?.join("xet/staging");
    let root = staging;
    let mut restored = Vec::new();
    for entry in fs::read_dir(root.join("shards"))? {
        let shard = MDBMinimalShard::from_reader(&mut Cursor::new(fs::read(entry?.path())?), true, true)?;
        for i in 0..shard.num_files() {
            let file = shard.file(i).unwrap();
            if file.file_hash() != pointer.hash {
                continue;
            }
            assert!(!file.contains_metadata_ext());
            for i in 0..file.num_entries() {
                let segment = file.entry(i);
                let mut reader =
                    Cursor::new(fs::read(root.join("xorbs").join(format!("{}.xorb", segment.xorb_hash.hex())))?);
                let xorb = XorbObject::deserialize(&mut reader)?;
                restored.extend(xorb.get_bytes_by_chunk_range(
                    &mut reader,
                    segment.chunk_index_start,
                    segment.chunk_index_end,
                )?);
            }
        }
    }
    assert_eq!(restored, data);
    run_git_captured(repo.path(), "commit", ["-m", "Add filtered files"])?;
    let xorbs_before = fs::read_dir(root.join("xorbs"))?
        .map(|e| e.map(|e| e.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    run_git_captured(repo.path(), "add", ["--renormalize", "large"])?;
    let (status, _) =
        run_git_captured_with_input_and_output(repo.path(), "status", ["--porcelain"])?.wait_with_output()?;
    assert!(status.is_empty());
    let xorbs_after = fs::read_dir(root.join("xorbs"))?
        .map(|e| e.map(|e| e.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    assert_eq!(xorbs_after, xorbs_before);
    fs::remove_file(repo.path().join("large"))?;
    run_git_captured(repo.path(), "checkout-index", ["-f", "large"])?;
    assert_eq!(fs::read(repo.path().join("large"))?, blob);

    // Git's stdin, not the worktree file named by --path, determines the cleaned blob.
    let mut command =
        run_git_captured_with_input_and_output(repo.path(), "hash-object", ["-w", "--stdin", "--path=large"])?;
    command.stdin()?.write_all(b"different input\n")?;
    let (oid, _) = command.wait_with_output()?;
    let oid = String::from_utf8(oid)?;
    let (blob, _) =
        run_git_captured_with_input_and_output(repo.path(), "cat-file", ["blob", oid.trim()])?.wait_with_output()?;
    assert_eq!(blob, b"different input\n");

    fs::write(repo.path().join("invalid"), b"version https://hf.co/spec/xet/v1\n")?;
    assert!(run_git_captured(repo.path(), "add", ["invalid"]).is_err());
    assert!(run_git_captured(repo.path(), "show", [":invalid"]).is_err());
    Ok(())
}

/// Git writes the index before closing the filter and ignores its exit status, so a failed
/// finalization cannot fail `git add`. Staging is shared by linked worktrees, so the marker must
/// name the worktree whose relative paths need re-adding.
#[cfg(unix)]
#[test]
#[serial(env_var_write_read)]
fn finalize_failure_is_reported_and_recoverable_per_worktree() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let repo = TestRepo::new("main")?;
    let binary = env!("CARGO_BIN_EXE_git-hf").replace('\'', "'\\''");
    repo.set_config("filter.hf.process", &format!("'{binary}' filter-process --smudge-passthrough"))?;
    repo.set_config("filter.hf.required", "true")?;
    fs::write(repo.path().join(".gitattributes"), "* filter=hf -text\n.gitattributes filter=\n")?;
    let mut data = vec![b'a'; 150000];
    data[0] = 0;
    fs::write(repo.path().join("large"), &data)?;
    run_git_captured(repo.path(), "add", ["."])?;
    run_git_captured(repo.path(), "commit", ["-m", "Add large in main worktree"])?;
    let (blob, _) = run_git_captured_with_input_and_output(repo.path(), "show", [":large"])?.wait_with_output()?;
    let main_pointer = XetPointer::parse(&blob)?.unwrap();

    // A linked worktree with different content at the same path.
    let linked_parent = tempfile::tempdir()?;
    let linked = linked_parent.path().join("linked");
    run_git_captured(repo.path(), "worktree", ["add", "-b", "linked", linked.to_str().unwrap()])?;
    data[1] = b'b';
    fs::write(linked.join("large"), &data)?;

    // The staged xorb is only written at finalization; make that the first failing write.
    let root = GitRepo::open(repo.path())?.common_dir()?.join("xet/staging");
    let xorbs = root.join("xorbs");
    fs::set_permissions(&xorbs, fs::Permissions::from_mode(0o555))?;
    if fs::write(xorbs.join("probe"), b"").is_ok() {
        // Permissions are not enforced (e.g. running as root); the failure cannot be injected.
        return Ok(());
    }
    let shards_before = fs::read_dir(root.join("shards"))?.count();

    let (_, stderr) = run_git_captured_with_input_and_output(&linked, "add", ["large"])?.wait_with_output()?;
    let stderr = String::from_utf8_lossy(&stderr);
    assert!(stderr.contains("--renormalize"), "{stderr}");
    let (blob, _) = run_git_captured_with_input_and_output(&linked, "show", [":large"])?.wait_with_output()?;
    let linked_pointer = XetPointer::parse(&blob)?.unwrap();
    assert_ne!(linked_pointer.hash, main_pointer.hash);
    assert_eq!(fs::read_dir(root.join("shards"))?.count(), shards_before);

    let markers = fs::read_dir(root.join("finalize-failed"))?.collect::<std::io::Result<Vec<_>>>()?;
    assert_eq!(markers.len(), 1);
    let marker = fs::read(markers[0].path())?;
    let records = marker.strip_suffix(b"\0").unwrap().split(|&b| b == 0).collect::<Vec<_>>();
    assert_eq!(fs::canonicalize(String::from_utf8(records[0].to_vec())?)?, fs::canonicalize(&linked)?);
    assert_eq!(records[1..], [linked_pointer.hash.hex().as_bytes(), b"large".as_slice()]);

    // Recovery happens in the recorded worktree and stages that worktree's content.
    fs::set_permissions(&xorbs, fs::Permissions::from_mode(0o755))?;
    run_git_captured(&linked, "add", ["--renormalize", "large"])?;
    let mut staged = false;
    for entry in fs::read_dir(root.join("shards"))? {
        let shard = MDBMinimalShard::from_reader(&mut Cursor::new(fs::read(entry?.path())?), true, true)?;
        staged |= (0..shard.num_files()).any(|i| shard.file(i).unwrap().file_hash() == linked_pointer.hash);
    }
    assert!(staged);
    Ok(())
}

/// A real `git push` runs the installed hook, which runs the pre-existing hook first with Git's
/// arguments and stdin; uninstalling restores that hook.
#[cfg(unix)]
#[test]
#[serial(env_var_write_read)]
fn git_push_runs_chained_hook() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    use xet_runtime::utils::EnvVarGuard;

    // Install refers to `git-hf` on PATH, as a user installation would.
    let binary = std::path::PathBuf::from(env!("CARGO_BIN_EXE_git-hf"));
    let path = std::env::join_paths(
        std::iter::once(binary.parent().unwrap().to_owned())
            .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
    )?;
    let _path = EnvVarGuard::set("PATH", path);

    let repo = TestRepo::new("main")?;
    let hooks = repo.path().join(".git/hooks");
    fs::create_dir_all(&hooks)?;
    fs::write(hooks.join("pre-push"), "#!/bin/sh\ncat > \"$(git rev-parse --git-dir)/chained-input\"\n")?;
    fs::set_permissions(hooks.join("pre-push"), fs::Permissions::from_mode(0o755))?;
    let (out, _) =
        run_git_captured_with_input_and_output(repo.path(), "hf", ["install", "--local"])?.wait_with_output()?;
    assert!(String::from_utf8(out)?.contains("pre-push.git-hf-chained"));

    let remote = tempfile::tempdir()?;
    run_git_captured(remote.path(), "init", ["--bare", "-q"])?;
    fs::write(repo.path().join("small"), b"hello\n")?;
    run_git_captured(repo.path(), "add", ["."])?;
    run_git_captured(repo.path(), "commit", ["-m", "small"])?;
    run_git_captured(repo.path(), "push", [remote.path().to_str().unwrap(), "main"])?;
    let input = fs::read_to_string(repo.path().join(".git/chained-input"))?;
    assert!(input.starts_with("refs/heads/main "), "{input}");

    // Uninstall restores the original hook.
    run_git_captured(repo.path(), "hf", ["uninstall", "--local"])?;
    assert!(fs::read_to_string(hooks.join("pre-push"))?.contains("chained-input"));
    assert!(!hooks.join("pre-push.git-hf-chained").exists());
    Ok(())
}
