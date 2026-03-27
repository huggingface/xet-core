use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use tempfile::{TempDir, tempdir};

fn xet_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_xet"))
}

fn xet_cmd(cas_dir: &Path, args: &[&str]) -> Output {
    let endpoint = format!("local://{}", cas_dir.display());
    Command::new(xet_bin())
        .arg("--endpoint")
        .arg(&endpoint)
        .args(args)
        .output()
        .expect("failed to execute xet binary")
}

fn xet_ok(cas_dir: &Path, args: &[&str]) -> String {
    let out = xet_cmd(cas_dir, args);
    assert!(
        out.status.success(),
        "xet {:?} failed:\nstdout: {}\nstderr: {}",
        args,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8(out.stdout).unwrap()
}

fn xet_ok_stderr(cas_dir: &Path, args: &[&str]) -> (String, String) {
    let out = xet_cmd(cas_dir, args);
    assert!(
        out.status.success(),
        "xet {:?} failed:\nstdout: {}\nstderr: {}",
        args,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    (String::from_utf8(out.stdout).unwrap(), String::from_utf8(out.stderr).unwrap())
}

#[allow(dead_code)]
fn xet_err(cas_dir: &Path, args: &[&str]) -> String {
    let out = xet_cmd(cas_dir, args);
    assert!(
        !out.status.success(),
        "xet {:?} unexpectedly succeeded:\nstdout: {}\nstderr: {}",
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
    let (_stdout, stderr) = xet_ok_stderr(cas_dir, &["file", "upload", file_path.to_str().unwrap()]);
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
    let out = Command::new(xet_bin())
        .arg("--help")
        .output()
        .expect("failed to run xet --help");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains("file"));
}

#[test]
fn test_cli_file_help() {
    let out = Command::new(xet_bin())
        .args(["file", "--help"])
        .output()
        .expect("failed to run xet file --help");
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
    xet_ok(
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

    let stdout_bytes = xet_ok(cas_dir.path(), &["file", "download", &hash]);
    assert_eq!(stdout_bytes.as_bytes(), content);
}

#[test]
fn test_cli_upload_from_stdin() {
    let cas_dir = tempdir().unwrap();
    let content = b"piped stdin content for upload";

    let endpoint = format!("local://{}", cas_dir.path().display());
    let mut child = Command::new(xet_bin())
        .arg("--endpoint")
        .arg(&endpoint)
        .args(["file", "upload", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn xet");

    child.stdin.take().unwrap().write_all(content).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "xet file upload - failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let stderr = String::from_utf8(out.stderr).unwrap();
    let (hash, size) = parse_upload_line(&stderr);
    assert_eq!(size, content.len() as u64);

    // Download and verify
    let stdout_bytes = xet_ok(cas_dir.path(), &["file", "download", &hash]);
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
    let (_stdout, stderr) = xet_ok_stderr(cas_dir.path(), &args);

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

    xet_ok(
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
    let out = xet_cmd(cas_dir.path(), &["file", "download", &fake_hash]);
    assert!(out.stdout.is_empty(), "expected no stdout for nonexistent hash");
}

#[test]
fn test_cli_scan_basic() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();
    let src = write_test_file(&src_dir, "scan_test.bin", &vec![7u8; 4096]);

    let stdout = xet_ok(cas_dir.path(), &["file", "scan", src.to_str().unwrap()]);
    assert!(stdout.contains("total_bytes=4096"));
}

#[test]
fn test_cli_dump_reconstruction_after_upload() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();
    let src = write_test_file(&src_dir, "recon.bin", &vec![3u8; 2048]);

    let (hash, _size) = upload_file(cas_dir.path(), &src);

    let stdout = xet_ok(cas_dir.path(), &["file", "dump-reconstruction", &hash]);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    // JSON output should contain reconstruction data or be null
    if !parsed.is_null() {
        assert!(parsed["terms"].is_array());
    }
}

#[test]
fn test_cli_dump_reconstruction_nonexistent_hash() {
    let cas_dir = tempdir().unwrap();
    let fake_hash = "0".repeat(64);
    let stdout = xet_ok(cas_dir.path(), &["file", "dump-reconstruction", &fake_hash]);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    if !parsed.is_null() && let Some(t) = parsed["terms"].as_array() {
        assert!(t.is_empty());
    }
}

#[test]
fn test_cli_quiet_mode() {
    let cas_dir = tempdir().unwrap();
    let src_dir = tempdir().unwrap();
    let src = write_test_file(&src_dir, "quiet.txt", b"quiet test");

    let out = xet_cmd(cas_dir.path(), &["--quiet", "file", "upload", src.to_str().unwrap()]);
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
    let out = Command::new(xet_bin())
        .arg("--endpoint")
        .arg(&endpoint)
        .arg("-c")
        .arg("client.enable_multirange_fetching=true")
        .args(["file", "upload", src.to_str().unwrap()])
        .output()
        .expect("failed to execute xet");

    assert!(out.status.success(), "config override failed:\nstderr: {}", String::from_utf8_lossy(&out.stderr));
}

#[test]
fn test_cli_concurrent_upload_download_stress() {
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
    let (_stdout, stderr) = xet_ok_stderr(cas_dir.path(), &args);

    let upload_lines: Vec<&str> = stderr.lines().filter(|l| l.contains("hash=")).collect();
    assert_eq!(upload_lines.len(), n);

    let dest_dir = tempdir().unwrap();
    for (i, line) in upload_lines.iter().enumerate() {
        let (hash, _size) = parse_upload_line(line);

        let dest = dest_dir.path().join(format!("out_{i}.bin"));
        xet_ok(cas_dir.path(), &["file", "download", &hash, "-o", dest.to_str().unwrap()]);

        let downloaded = std::fs::read(&dest).unwrap();
        assert_eq!(downloaded, files[i].1, "content mismatch for file {i} (hash={hash})");
    }
}
