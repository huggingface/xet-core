"""
Smoke test for hf-xet using the `hf` CLI for upload/download through both
HuggingFace model repositories and storage buckets.

Creates temporary resources, exercises upload/download paths, verifies content
integrity, then cleans up. Requires HF_TOKEN with write access.

Usage:
    uv run scripts/smoke_tests/test_upload_download.py
    uv run scripts/smoke_tests/test_upload_download.py --hf-xet-version 1.4.0
    uv run scripts/smoke_tests/test_upload_download.py --keep-repo
    uv run scripts/smoke_tests/test_upload_download.py --skip-buckets
"""

# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "huggingface_hub>=1.0.0",
#     "hf_xet",
# ]
# ///

import argparse
import hashlib
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(cmd: list[str], check: bool = True) -> str:
    """Run a CLI command, return stdout. Raises RuntimeError on failure."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\n"
            f"stdout: {result.stdout.strip()}\n"
            f"stderr: {result.stderr.strip()}"
        )
    return result.stdout.strip()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def generate_file(path: str | Path, size_bytes: int) -> str:
    """Write random bytes to path; return sha256 hex."""
    data = secrets.token_bytes(size_bytes)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)
    return sha256_bytes(data)


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

class Results:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors: list[tuple[str, str]] = []

    def run(self, name: str, fn):
        print(f"\n{'='*60}")
        print(f"TEST: {name}")
        print(f"{'='*60}")
        try:
            fn()
            self.passed += 1
            print(f"PASSED: {name}")
        except Exception as e:
            self.failed += 1
            self.errors.append((name, str(e)))
            print(f"FAILED: {name}: {e}", file=sys.stderr)

    def summary(self):
        print(f"\n{'='*60}")
        print(f"RESULTS: {self.passed} passed, {self.failed} failed")
        print(f"{'='*60}")
        if self.errors:
            for name, err in self.errors:
                print(f"  FAIL: {name}: {err}")
            sys.exit(1)
        else:
            print("All smoke tests passed!")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Smoke test hf-xet via hf CLI")
    parser.add_argument("--hf-xet-version", help="Expected hf_xet version (display/warn only)")
    parser.add_argument("--keep-repo", action="store_true", help="Skip cleanup of test repo/bucket")
    parser.add_argument("--repo-prefix", default="smoke-test-xet", help="Prefix for temp resource names")
    parser.add_argument("--skip-buckets", action="store_true", help="Skip storage bucket tests")
    args = parser.parse_args()

    # --- preflight checks ---
    token = os.environ.get("HF_TOKEN")
    if not token:
        print("ERROR: HF_TOKEN environment variable is required", file=sys.stderr)
        sys.exit(1)

    if not shutil.which("hf"):
        print("ERROR: hf CLI not found. Install: pip install huggingface_hub", file=sys.stderr)
        sys.exit(1)

    # --- print environment ---
    import huggingface_hub
    print(f"huggingface_hub version: {huggingface_hub.__version__}")
    try:
        from importlib.metadata import version as pkg_version
        installed_xet = pkg_version("hf_xet")
        print(f"hf_xet version: {installed_xet}")
        if args.hf_xet_version and installed_xet != args.hf_xet_version:
            print(f"WARNING: hf_xet version mismatch: got {installed_xet}, expected {args.hf_xet_version}")
    except Exception:
        print("hf_xet version: unknown")
    print(f"hf CLI: {run(['hf', 'version'])}")

    # --- resolve username ---
    from huggingface_hub import HfApi
    user = HfApi(token=token).whoami()["name"]

    suffix = secrets.token_hex(4)
    repo_id = f"{user}/{args.repo_prefix}-{suffix}"
    bucket_id = f"{user}/{args.repo_prefix}-bucket-{suffix}"

    print(f"\nTest repo:   {repo_id}")
    if not args.skip_buckets:
        print(f"Test bucket: {bucket_id}")

    results = Results()

    # ===================================================================== #
    # Repository tests  (hf upload / hf download)
    # ===================================================================== #

    repo_test_files = {
        "small.bin":         1024,            # 1 KB   — below chunk size
        "medium.bin":        256 * 1024,      # 256 KB — a few chunks
        "large.bin":         5 * 1024 * 1024, # 5 MB   — multiple chunks
        "xlarge.bin":        50 * 1024 * 1024,# 50 MB  — large multi-xorb
        "xxlarge.bin":       100 * 1024 * 1024,# 100 MB — stress test
        "subdir/nested.bin": 128 * 1024,      # 128 KB — subdirectory
    }

    repo_created = False
    try:
        print(f"\nCreating repo {repo_id}...")
        run(["hf", "repos", "create", repo_id, "--private"])
        repo_created = True

        with tempfile.TemporaryDirectory() as tmpdir:
            upload_dir = Path(tmpdir) / "upload"
            download_dir = Path(tmpdir) / "download"
            upload_dir.mkdir()
            download_dir.mkdir()

            expected = {}
            for rel, size in repo_test_files.items():
                expected[rel] = generate_file(upload_dir / rel, size)
                print(f"  Generated {rel} ({size:,} bytes)")

            # -- 1. upload single file --
            def test_repo_upload_single():
                t = time.time()
                run(["hf", "upload", repo_id,
                     str(upload_dir / "small.bin"), "small.bin", "--quiet"])
                print(f"  Uploaded small.bin in {time.time()-t:.1f}s")
            results.run("Repo: upload single file", test_repo_upload_single)

            # -- 2. upload folder --
            def test_repo_upload_folder():
                t = time.time()
                run(["hf", "upload", repo_id, str(upload_dir), ".", "--quiet"])
                print(f"  Uploaded folder in {time.time()-t:.1f}s")
            results.run("Repo: upload folder", test_repo_upload_folder)

            # -- 3. download individual files and verify --
            def test_repo_download_single():
                out = str(download_dir / "single")
                for rel in repo_test_files:
                    t = time.time()
                    run(["hf", "download", repo_id, rel, "--local-dir", out, "--quiet"])
                    actual = sha256_file(Path(out) / rel)
                    assert actual == expected[rel], (
                        f"Hash mismatch for {rel}: "
                        f"expected {expected[rel][:16]}..., got {actual[:16]}..."
                    )
                    print(f"  Downloaded+verified {rel} in {time.time()-t:.1f}s")
            results.run("Repo: download and verify individual files", test_repo_download_single)

            # -- 4. download entire repo and verify --
            def test_repo_download_all():
                out = str(download_dir / "all")
                t = time.time()
                run(["hf", "download", repo_id, "--local-dir", out, "--quiet"])
                print(f"  Downloaded all files in {time.time()-t:.1f}s")
                for rel in repo_test_files:
                    p = Path(out) / rel
                    assert p.exists(), f"Missing file: {rel}"
                    actual = sha256_file(p)
                    assert actual == expected[rel], (
                        f"Hash mismatch for {rel}: "
                        f"expected {expected[rel][:16]}..., got {actual[:16]}..."
                    )
                    print(f"  Verified {rel}")
            results.run("Repo: download all files and verify", test_repo_download_all)

            # -- 5. overwrite a file and verify new content --
            def test_repo_overwrite():
                new_hash = generate_file(upload_dir / "small.bin", 2048)
                run(["hf", "upload", repo_id,
                     str(upload_dir / "small.bin"), "small.bin", "--quiet"])
                out = str(download_dir / "overwrite")
                run(["hf", "download", repo_id, "small.bin",
                     "--local-dir", out, "--force-download", "--quiet"])
                actual = sha256_file(Path(out) / "small.bin")
                assert actual == new_hash, (
                    f"Overwrite mismatch: expected {new_hash[:16]}..., got {actual[:16]}..."
                )
                print("  Overwrite verified: new content downloaded correctly")
            results.run("Repo: upload overwrite and verify", test_repo_overwrite)

            # -- 6. delete files from repo --
            def test_repo_delete_files():
                run(["hf", "repos", "delete-files", repo_id, "small.bin"])
                # Re-download all; small.bin should be absent
                out = str(download_dir / "post-delete")
                run(["hf", "download", repo_id, "--local-dir", out, "--quiet"])
                assert not (Path(out) / "small.bin").exists(), \
                    "small.bin still present after deletion"
                print("  small.bin confirmed absent after delete")
            results.run("Repo: delete file from repo", test_repo_delete_files)

    finally:
        if repo_created and not args.keep_repo:
            print(f"\nCleaning up repo {repo_id}...")
            try:
                run(["hf", "repos", "delete", repo_id])
                print("  Deleted.")
            except Exception as e:
                print(f"  Warning: failed to delete repo: {e}", file=sys.stderr)

    # ===================================================================== #
    # Storage bucket tests  (hf buckets)
    # ===================================================================== #

    if args.skip_buckets:
        results.summary()
        return

    # Check that hf buckets is available
    bucket_check = run(["hf", "buckets", "--help"], check=False)
    if "buckets" not in bucket_check.lower() and "error" in bucket_check.lower():
        print("\nWARNING: hf buckets not available in this hf CLI version — skipping bucket tests")
        results.summary()
        return

    bucket_created = False
    try:
        print(f"\nCreating bucket {bucket_id}...")
        run(["hf", "buckets", "create", bucket_id, "--private"])
        bucket_created = True
        handle = f"hf://buckets/{bucket_id}"

        with tempfile.TemporaryDirectory() as tmpdir:
            upload_dir = Path(tmpdir) / "upload"
            download_dir = Path(tmpdir) / "download"
            (upload_dir / "subdir").mkdir(parents=True)
            download_dir.mkdir()

            # Files used in bucket tests
            single_hash   = generate_file(upload_dir / "single.bin",        512 * 1024)
            subdir1_hash  = generate_file(upload_dir / "subdir/file1.bin",   256 * 1024)
            subdir2_hash  = generate_file(upload_dir / "subdir/file2.bin",   256 * 1024)
            print(f"  Generated single.bin, subdir/file1.bin, subdir/file2.bin")

            # -- 1. cp: upload single file --
            def test_bucket_cp_upload():
                t = time.time()
                run(["hf", "buckets", "cp",
                     str(upload_dir / "single.bin"), f"{handle}/single.bin"])
                print(f"  Uploaded single.bin via cp in {time.time()-t:.1f}s")
            results.run("Bucket: cp upload single file", test_bucket_cp_upload)

            # -- 2. sync: upload directory --
            def test_bucket_sync_upload():
                t = time.time()
                run(["hf", "buckets", "sync",
                     str(upload_dir / "subdir"), f"{handle}/subdir"])
                print(f"  Synced subdir/ up in {time.time()-t:.1f}s")
            results.run("Bucket: sync upload directory", test_bucket_sync_upload)

            # -- 3. list files (recursive quiet) --
            def test_bucket_list():
                out = run(["hf", "buckets", "list", bucket_id, "-R", "--quiet"])
                listed = set(out.splitlines())
                for path in ("single.bin", "subdir/file1.bin", "subdir/file2.bin"):
                    assert path in listed, f"Expected {path!r} in listing, got: {listed}"
                print(f"  Listed {len(listed)} file(s): {sorted(listed)}")
            results.run("Bucket: list files (recursive)", test_bucket_list)

            # -- 4. cp: download single file and verify --
            def test_bucket_cp_download():
                out_path = download_dir / "single.bin"
                t = time.time()
                run(["hf", "buckets", "cp", f"{handle}/single.bin", str(out_path)])
                actual = sha256_file(out_path)
                assert actual == single_hash, (
                    f"Hash mismatch: expected {single_hash[:16]}..., got {actual[:16]}..."
                )
                print(f"  Downloaded+verified single.bin in {time.time()-t:.1f}s")
            results.run("Bucket: cp download and verify", test_bucket_cp_download)

            # -- 5. sync: download directory and verify --
            def test_bucket_sync_download():
                out_dir = download_dir / "subdir"
                t = time.time()
                run(["hf", "buckets", "sync", f"{handle}/subdir", str(out_dir)])
                print(f"  Synced subdir/ down in {time.time()-t:.1f}s")
                for fname, expected_hash in (
                    ("file1.bin", subdir1_hash),
                    ("file2.bin", subdir2_hash),
                ):
                    p = out_dir / fname
                    assert p.exists(), f"Missing: {p}"
                    actual = sha256_file(p)
                    assert actual == expected_hash, (
                        f"Hash mismatch for {fname}: "
                        f"expected {expected_hash[:16]}..., got {actual[:16]}..."
                    )
                    print(f"  Verified subdir/{fname}")
            results.run("Bucket: sync download and verify", test_bucket_sync_download)

            # -- 6. overwrite via cp and verify new content --
            def test_bucket_overwrite():
                new_hash = generate_file(upload_dir / "single.bin", 1024 * 1024)
                run(["hf", "buckets", "cp",
                     str(upload_dir / "single.bin"), f"{handle}/single.bin"])
                out_path = download_dir / "single_overwrite.bin"
                run(["hf", "buckets", "cp", f"{handle}/single.bin", str(out_path)])
                actual = sha256_file(out_path)
                assert actual == new_hash, (
                    f"Overwrite mismatch: expected {new_hash[:16]}..., got {actual[:16]}..."
                )
                print("  Overwrite verified: new content downloaded correctly")
            results.run("Bucket: cp overwrite and verify", test_bucket_overwrite)

            # -- 7. sync --delete: remove files absent from source --
            def test_bucket_sync_delete():
                # Local subdir now only has file1.bin; sync --delete should remove file2.bin
                (upload_dir / "subdir" / "file2.bin").unlink()
                run(["hf", "buckets", "sync",
                     str(upload_dir / "subdir"), f"{handle}/subdir", "--delete"])
                out = run(["hf", "buckets", "list", bucket_id, "-R", "--quiet"])
                listed = set(out.splitlines())
                assert "subdir/file2.bin" not in listed, \
                    f"subdir/file2.bin still present after sync --delete: {listed}"
                assert "subdir/file1.bin" in listed, \
                    f"subdir/file1.bin missing after sync --delete: {listed}"
                print(f"  sync --delete verified: remaining files: {sorted(listed)}")
            results.run("Bucket: sync --delete removes extraneous files", test_bucket_sync_delete)

            # -- 8. rm: delete a file and confirm it's gone --
            def test_bucket_rm():
                run(["hf", "buckets", "rm", f"{bucket_id}/single.bin", "--yes"])
                out = run(["hf", "buckets", "list", bucket_id, "-R", "--quiet"])
                listed = set(out.splitlines())
                assert "single.bin" not in listed, \
                    f"single.bin still present after rm: {listed}"
                print(f"  rm verified: remaining files: {sorted(listed)}")
            results.run("Bucket: rm file", test_bucket_rm)

    finally:
        if bucket_created and not args.keep_repo:
            print(f"\nCleaning up bucket {bucket_id}...")
            try:
                run(["hf", "buckets", "delete", bucket_id, "--yes"])
                print("  Deleted.")
            except Exception as e:
                print(f"  Warning: failed to delete bucket: {e}", file=sys.stderr)

    results.summary()


if __name__ == "__main__":
    main()
