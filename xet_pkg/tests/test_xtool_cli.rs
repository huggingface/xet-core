use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use tempfile::{TempDir, tempdir};

fn xtool_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_xtool"))
}

fn xtool_cmd(cas_dir: &Path, args: &[&str]) -> Output {
    let endpoint = format!("local://{}", cas_dir.display());
    Command::new(xtool_bin())
        .arg("--endpoint")
        .arg(&endpoint)
        .args(args)
        .output()
        .expect("failed to execute xtool binary")
}

fn xtool_cmd_with_env(args: &[&str], env_vars: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(xtool_bin());
    cmd.args(args);
    for (key, value) in env_vars {
        cmd.env(key, value);
    }
    cmd.output().expect("failed to execute xtool binary")
}

fn xtool_ok(cas_dir: &Path, args: &[&str]) -> String {
    let out = xtool_cmd(cas_dir, args);
    assert!(
        out.status.success(),
        "xtool {:?} failed:\nstdout: {}\nstderr: {}",
        args,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8(out.stdout).unwrap()
}

fn xtool_ok_stderr(cas_dir: &Path, args: &[&str]) -> (String, String) {
    let out = xtool_cmd(cas_dir, args);
    assert!(
        out.status.success(),
        "xtool {:?} failed:\nstdout: {}\nstderr: {}",
        args,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    (String::from_utf8(out.stdout).unwrap(), String::from_utf8(out.stderr).unwrap())
}

#[allow(dead_code)]
fn xtool_err(cas_dir: &Path, args: &[&str]) -> String {
    let out = xtool_cmd(cas_dir, args);
    assert!(
        !out.status.success(),
        "xtool {:?} unexpectedly succeeded:\nstdout: {}\nstderr: {}",
        args,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stderr = String::from_utf8(out.stderr).unwrap();
    let stdout = String::from_utf8(out.stdout).unwrap();
    format!("{stdout}{stderr}")
}

/// Upload a file via CLI and parse the stderr output line to extract hash and size.
/// Output format on stderr: `<name>  hash=<hex>  size=<n>  sha256=<hex|->`
fn upload_file(cas_dir: &Path, file_path: &Path) -> (String, u64) {
    let (_stdout, stderr) = xtool_ok_stderr(cas_dir, &["file", "upload", file_path.to_str().unwrap()]);
    parse_upload_line(&stderr)
}

fn parse_upload_line(line: &str) -> (String, u64) {
    let mut hash = String::new();
    let mut size = 0u64;
    for part in line.split_whitespace() {
        if let Some(h) = part.strip_prefix("hash=") {
            hash = h.to_string();
        }
        if let Some(s) = part.strip_prefix("size=") {
            size = s.parse().unwrap();
        }
    }
    assert!(!hash.is_empty(), "could not parse hash from: {line}");
    (hash, size)
}

fn write_test_file(dir: &TempDir, name: &str, content: &[u8]) -> PathBuf {
    let path = dir.path().join(name);
    std::fs::write(&path, content).unwrap();
    path
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[test]
fn test_cli_help() {
    let out = Command::new(xtool_bin())
        .arg("--help")
        .output()
        .expect("failed to run xtool --help");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains("file"));
    assert!(stdout.contains("dedup"));
    assert!(stdout.contains("query"));
}

#[test]
fn test_cli_file_help() {
    let out = Command::new(xtool_bin())
        .args(["file", "--help"])
        .output()
        .expect("failed to run xtool file --help");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains("upload"));
    assert!(stdout.contains("download"));
    assert!(stdout.contains("scan"));
    assert!(stdout.contains("dump-reconstruction"));
}

#[test]
fn test_cli_upload_and_download_roundtrip() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();
    let content = b"integration test roundtrip content";
    let src = write_test_file(&src_dir, "roundtrip.txt", content);

    let (hash, size) = upload_file(cas_dir.path(), &src);
    assert_eq!(size, content.len() as u64);

    let dest_dir = tempdir().unwrap();
    let dest = dest_dir.path().join("downloaded.txt");
    xtool_ok(
        cas_dir.path(),
        &[
            "file",
            "download",
            &hash,
            "-o",
            dest.to_str().unwrap(),
            "--size",
            &size.to_string(),
        ],
    );

    assert_eq!(std::fs::read(&dest).unwrap(), content);
}

#[test]
fn test_cli_download_to_stdout() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();
    let content = b"stdout download test data";
    let src = write_test_file(&src_dir, "stdout.bin", content);

    let (hash, _size) = upload_file(cas_dir.path(), &src);

    let stdout_bytes = xtool_ok(cas_dir.path(), &["file", "download", &hash]);
    assert_eq!(stdout_bytes.as_bytes(), content);
}

#[test]
fn test_cli_download_source_range() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();
    let content = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let src = write_test_file(&src_dir, "range.bin", content);

    let (hash, size) = upload_file(cas_dir.path(), &src);

    let dest_dir = tempdir().unwrap();
    let dest = dest_dir.path().join("range_out.bin");
    xtool_ok(
        cas_dir.path(),
        &[
            "file",
            "download",
            &hash,
            "-o",
            dest.to_str().unwrap(),
            "--size",
            &size.to_string(),
            "--source-range",
            "5..13",
        ],
    );

    assert_eq!(std::fs::read(&dest).unwrap(), content[5..13].to_vec());
}

#[test]
fn test_cli_download_write_range() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();
    let content = b"abcdefghij";
    let src = write_test_file(&src_dir, "write_range.bin", content);
    let (hash, _size) = upload_file(cas_dir.path(), &src);

    let dest_dir = tempdir().unwrap();
    let dest = dest_dir.path().join("write_range_out.bin");
    let mut initial = b"................".to_vec();
    std::fs::write(&dest, &initial).unwrap();

    xtool_ok(
        cas_dir.path(),
        &[
            "file",
            "download",
            &hash,
            "-o",
            dest.to_str().unwrap(),
            "--source-range",
            "2..7",
            "--write-range",
            "4..9",
        ],
    );

    initial[4..9].copy_from_slice(&content[2..7]);
    assert_eq!(std::fs::read(&dest).unwrap(), initial);
}

#[test]
fn test_cli_download_write_range_requires_output() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();
    let src = write_test_file(&src_dir, "write_range_required.bin", b"abcdef");
    let (hash, _size) = upload_file(cas_dir.path(), &src);

    let out = xtool_cmd(cas_dir.path(), &["file", "download", &hash, "--write-range", "0..4"]);
    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("--output"));
}

#[test]
fn test_cli_upload_from_stdin() {
    let cas_dir = tempdir().unwrap();
    let content = b"piped stdin content for upload";

    let endpoint = format!("local://{}", cas_dir.path().display());
    let mut child = Command::new(xtool_bin())
        .arg("--endpoint")
        .arg(&endpoint)
        .args(["file", "upload", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn xtool");

    child.stdin.take().unwrap().write_all(content).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "xtool file upload - failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let stderr = String::from_utf8(out.stderr).unwrap();
    let (hash, size) = parse_upload_line(&stderr);
    assert_eq!(size, content.len() as u64);

    let stdout_bytes = xtool_ok(cas_dir.path(), &["file", "download", &hash]);
    assert_eq!(stdout_bytes.as_bytes(), content);
}

#[test]
fn test_cli_upload_multiple_files() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();

    let files: Vec<PathBuf> = (0..3)
        .map(|i| write_test_file(&src_dir, &format!("multi_{i}.bin"), format!("file {i} data").as_bytes()))
        .collect();

    let file_args: Vec<&str> = files.iter().map(|p| p.to_str().unwrap()).collect();
    let mut args = vec!["file", "upload"];
    args.extend(&file_args);
    let (_stdout, stderr) = xtool_ok_stderr(cas_dir.path(), &args);

    let lines: Vec<&str> = stderr.lines().filter(|l| l.contains("hash=")).collect();
    assert_eq!(lines.len(), 3);
}

#[test]
fn test_cli_upload_json_output() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();
    let src = write_test_file(&src_dir, "json.txt", b"json output via cli");

    let out_dir = tempdir().unwrap();
    let json_path = out_dir.path().join("results.json");

    xtool_ok(
        cas_dir.path(),
        &[
            "file",
            "upload",
            "--output",
            json_path.to_str().unwrap(),
            src.to_str().unwrap(),
        ],
    );

    let json_str = std::fs::read_to_string(&json_path).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["xet_info"]["file_size"], 19);
    assert!(!arr[0]["xet_info"]["hash"].as_str().unwrap().is_empty());
}

#[test]
fn test_cli_download_bad_hash() {
    let cas_dir = tempdir().unwrap();

    let fake_hash = "0".repeat(64);
    let out = xtool_cmd(cas_dir.path(), &["file", "download", &fake_hash]);
    assert!(out.stdout.is_empty(), "expected no stdout for nonexistent hash");
}

#[test]
fn test_cli_scan_basic() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();
    let src = write_test_file(&src_dir, "scan_test.bin", &vec![7u8; 4096]);

    let stdout = xtool_ok(cas_dir.path(), &["file", "scan", src.to_str().unwrap()]);
    assert!(stdout.contains("total_bytes=4096"));
}

#[test]
fn test_cli_dump_reconstruction_after_upload() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();
    let src = write_test_file(&src_dir, "recon.bin", &vec![3u8; 2048]);

    let (hash, _size) = upload_file(cas_dir.path(), &src);

    let stdout = xtool_ok(cas_dir.path(), &["file", "dump-reconstruction", &hash]);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    if !parsed.is_null() {
        assert!(parsed["terms"].is_array());
    }
}

#[test]
fn test_cli_dump_reconstruction_with_source_range() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();
    let src = write_test_file(&src_dir, "recon_range.bin", &vec![9u8; 4096]);

    let (hash, _size) = upload_file(cas_dir.path(), &src);
    let stdout = xtool_ok(cas_dir.path(), &["file", "dump-reconstruction", &hash, "--source-range", "0..512"]);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert!(!parsed.is_null());
    let terms = parsed["terms"].as_array().unwrap();
    assert!(!terms.is_empty());
    let total_unpacked: u64 = terms.iter().map(|t| t["unpacked_length"].as_u64().unwrap()).sum();
    assert!(total_unpacked > 0);
}

#[test]
fn test_cli_dump_reconstruction_nonexistent_hash() {
    let cas_dir = tempdir().unwrap();
    let fake_hash = "0".repeat(64);
    let stdout = xtool_ok(cas_dir.path(), &["file", "dump-reconstruction", &fake_hash]);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    if !parsed.is_null()
        && let Some(t) = parsed["terms"].as_array()
    {
        assert!(t.is_empty());
    }
}

#[test]
fn test_cli_quiet_mode() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();
    let src = write_test_file(&src_dir, "quiet.txt", b"quiet test");

    let out = xtool_cmd(cas_dir.path(), &["--quiet", "file", "upload", src.to_str().unwrap()]);
    assert!(out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.is_empty(), "expected no stderr in quiet mode, got: {stderr}");
}

#[test]
fn test_cli_config_override_accepted() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();
    let src = write_test_file(&src_dir, "config_test.txt", b"config test");

    let endpoint = format!("local://{}", cas_dir.path().display());
    let out = Command::new(xtool_bin())
        .arg("--endpoint")
        .arg(&endpoint)
        .arg("-c")
        .arg("client.enable_multirange_fetching=true")
        .args(["file", "upload", src.to_str().unwrap()])
        .output()
        .expect("failed to execute xtool");

    assert!(out.status.success(), "config override failed:\nstderr: {}", String::from_utf8_lossy(&out.stderr));
}

#[test]
fn test_cli_hf_endpoint_env_fallback() {
    let cas_dir = tempdir().unwrap();
    let endpoint = format!("local://{}", cas_dir.path().display());
    let src_dir = tempdir().unwrap();
    let content = b"hf endpoint fallback";
    let src = write_test_file(&src_dir, "env_fallback.txt", content);

    let out = xtool_cmd_with_env(&["file", "upload", src.to_str().unwrap()], &[("HF_ENDPOINT", &endpoint)]);
    assert!(out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    let (hash, size) = parse_upload_line(&stderr);
    assert_eq!(size, content.len() as u64);

    let downloaded = xtool_ok(cas_dir.path(), &["file", "download", &hash]);
    assert_eq!(downloaded.as_bytes(), content);
}

#[test]
fn test_cli_endpoint_flag_overrides_hf_endpoint() {
    let env_cas_dir = tempdir().unwrap();
    let env_endpoint = format!("local://{}", env_cas_dir.path().display());
    let flag_cas_dir = tempdir().unwrap();
    let flag_endpoint = format!("local://{}", flag_cas_dir.path().display());
    let src_dir = tempdir().unwrap();
    let content = b"endpoint override";
    let src = write_test_file(&src_dir, "override.txt", content);

    let out = xtool_cmd_with_env(
        &["--endpoint", &flag_endpoint, "file", "upload", src.to_str().unwrap()],
        &[("HF_ENDPOINT", &env_endpoint)],
    );
    assert!(out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    let (hash, _size) = parse_upload_line(&stderr);

    let downloaded = xtool_ok(flag_cas_dir.path(), &["file", "download", &hash]);
    assert_eq!(downloaded.as_bytes(), content);

    let env_downloaded = xtool_ok(env_cas_dir.path(), &["file", "download", &hash]);
    assert!(env_downloaded.is_empty());
}

#[test]
fn test_cli_parallel_upload_download_stress() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();

    let n = 20;
    let files: Vec<(PathBuf, Vec<u8>)> = (0..n)
        .map(|i| {
            let content: Vec<u8> = (0..256).map(|b| ((b as u16 * (i + 1) as u16) % 256) as u8).collect();
            let path = write_test_file(&src_dir, &format!("stress_{i}.bin"), &content);
            (path, content)
        })
        .collect();

    let file_args: Vec<&str> = files.iter().map(|(p, _)| p.to_str().unwrap()).collect();
    let mut args = vec!["file", "upload", "--no-sha256"];
    args.extend(&file_args);
    let (_stdout, stderr) = xtool_ok_stderr(cas_dir.path(), &args);

    let upload_lines: Vec<&str> = stderr.lines().filter(|l| l.contains("hash=")).collect();
    assert_eq!(upload_lines.len(), n);
    let hashes: Vec<String> = upload_lines.iter().map(|line| parse_upload_line(line).0).collect();

    let dest_dir = tempdir().unwrap();
    let cas_path = cas_dir.path().to_path_buf();
    let dest_root = dest_dir.path().to_path_buf();
    let mut workers = Vec::with_capacity(hashes.len());
    for (i, hash) in hashes.into_iter().enumerate() {
        let cas_path = cas_path.clone();
        let dest_root = dest_root.clone();
        let expected = files[i].1.clone();
        workers.push(std::thread::spawn(move || {
            let dest = dest_root.join(format!("out_{i}.bin"));
            xtool_ok(&cas_path, &["file", "download", &hash, "-o", dest.to_str().unwrap()]);
            let downloaded = std::fs::read(&dest).unwrap();
            assert_eq!(downloaded, expected);
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
}
