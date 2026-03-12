"""
Smoke test for hf-xet upload and download through huggingface_hub.

Creates a temporary repo, uploads files of various sizes, downloads them,
and verifies content integrity. Cleans up the repo on exit.

Requires HF_TOKEN environment variable with write access.

Usage:
    uv run smoke_tests/test_upload_download.py
    uv run smoke_tests/test_upload_download.py --hf-xet-version 1.1.0
    uv run smoke_tests/test_upload_download.py --keep-repo  # don't delete repo after test
"""

# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "huggingface_hub>=0.30.0",
#     "hf_xet",
# ]
# ///

import argparse
import hashlib
import os
import secrets
import sys
import tempfile
import time
from pathlib import Path

from huggingface_hub import HfApi, hf_hub_download


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def generate_test_file(path: str, size_bytes: int) -> str:
    """Generate a file with random data and return its sha256."""
    data = secrets.token_bytes(size_bytes)
    with open(path, "wb") as f:
        f.write(data)
    return sha256_bytes(data)


def main():
    parser = argparse.ArgumentParser(description="Smoke test hf-xet upload/download")
    parser.add_argument("--hf-xet-version", help="Specific hf_xet version to test (for display only)")
    parser.add_argument("--keep-repo", action="store_true", help="Don't delete the test repo after")
    parser.add_argument("--repo-prefix", default="smoke-test-xet", help="Prefix for temp repo name")
    args = parser.parse_args()

    token = os.environ.get("HF_TOKEN")
    if not token:
        print("ERROR: HF_TOKEN environment variable is required", file=sys.stderr)
        sys.exit(1)

    # Print versions for debugging
    import huggingface_hub
    print(f"huggingface_hub version: {huggingface_hub.__version__}")
    try:
        from importlib.metadata import version as pkg_version
        hf_xet_version = pkg_version("hf_xet")
        print(f"hf_xet version: {hf_xet_version}")
        if args.hf_xet_version and hf_xet_version != args.hf_xet_version:
            print(f"WARNING: hf_xet version mismatch: got {hf_xet_version}, expected {args.hf_xet_version}")
    except Exception:
        print("hf_xet version: unknown")

    api = HfApi(token=token)
    user = api.whoami()["name"]
    repo_name = f"{user}/{args.repo_prefix}-{secrets.token_hex(4)}"
    print(f"\nTest repo: {repo_name}")

    test_files = {
        "small.bin": 1024,                  # 1 KB — below chunk size
        "medium.bin": 256 * 1024,           # 256 KB — a few chunks
        "large.bin": 5 * 1024 * 1024,       # 5 MB — multiple chunks, single xorb
        "subdir/nested.bin": 128 * 1024,    # 128 KB — test subdirectory handling
    }

    results = {"passed": 0, "failed": 0, "errors": []}

    def run_test(name, fn):
        try:
            print(f"\n{'='*60}")
            print(f"TEST: {name}")
            print(f"{'='*60}")
            fn()
            results["passed"] += 1
            print(f"PASSED: {name}")
        except Exception as e:
            results["failed"] += 1
            results["errors"].append((name, str(e)))
            print(f"FAILED: {name}: {e}", file=sys.stderr)

    try:
        # Create repo
        print(f"\nCreating repo {repo_name}...")
        api.create_repo(repo_name, private=True)

        with tempfile.TemporaryDirectory() as tmpdir:
            upload_dir = Path(tmpdir) / "upload"
            download_dir = Path(tmpdir) / "download"
            upload_dir.mkdir()
            download_dir.mkdir()

            # Generate test files and record hashes
            expected_hashes = {}
            for rel_path, size in test_files.items():
                fpath = upload_dir / rel_path
                fpath.parent.mkdir(parents=True, exist_ok=True)
                expected_hashes[rel_path] = generate_test_file(str(fpath), size)
                print(f"  Generated {rel_path} ({size:,} bytes) sha256={expected_hashes[rel_path][:16]}...")

            # Test 1: Upload individual file
            def test_upload_single_file():
                start = time.time()
                api.upload_file(
                    path_or_fileobj=str(upload_dir / "small.bin"),
                    path_in_repo="small.bin",
                    repo_id=repo_name,
                )
                elapsed = time.time() - start
                print(f"  Uploaded small.bin in {elapsed:.1f}s")

            run_test("Upload single file", test_upload_single_file)

            # Test 2: Upload folder (remaining files)
            def test_upload_folder():
                start = time.time()
                api.upload_folder(
                    folder_path=str(upload_dir),
                    repo_id=repo_name,
                )
                elapsed = time.time() - start
                print(f"  Uploaded folder in {elapsed:.1f}s")

            run_test("Upload folder", test_upload_folder)

            # Test 3: Download individual files and verify
            def test_download_and_verify_single():
                for rel_path in test_files:
                    start = time.time()
                    local_path = hf_hub_download(
                        repo_id=repo_name,
                        filename=rel_path,
                        local_dir=str(download_dir / "single"),
                    )
                    elapsed = time.time() - start
                    actual_hash = sha256_file(local_path)
                    assert actual_hash == expected_hashes[rel_path], (
                        f"Hash mismatch for {rel_path}: "
                        f"expected {expected_hashes[rel_path][:16]}..., "
                        f"got {actual_hash[:16]}..."
                    )
                    print(f"  Downloaded + verified {rel_path} in {elapsed:.1f}s")

            run_test("Download and verify individual files", test_download_and_verify_single)

            # Test 4: Snapshot download (all files)
            def test_snapshot_download():
                from huggingface_hub import snapshot_download
                start = time.time()
                snapshot_path = snapshot_download(
                    repo_id=repo_name,
                    local_dir=str(download_dir / "snapshot"),
                )
                elapsed = time.time() - start
                print(f"  Snapshot downloaded to {snapshot_path} in {elapsed:.1f}s")

                for rel_path in test_files:
                    local_path = os.path.join(snapshot_path, rel_path)
                    assert os.path.exists(local_path), f"File missing from snapshot: {rel_path}"
                    actual_hash = sha256_file(local_path)
                    assert actual_hash == expected_hashes[rel_path], (
                        f"Hash mismatch for {rel_path}: "
                        f"expected {expected_hashes[rel_path][:16]}..., "
                        f"got {actual_hash[:16]}..."
                    )
                    print(f"  Verified {rel_path}")

            run_test("Snapshot download and verify", test_snapshot_download)

            # Test 5: Upload overwrite (re-upload with different content)
            def test_upload_overwrite():
                overwrite_path = upload_dir / "small.bin"
                new_hash = generate_test_file(str(overwrite_path), 2048)
                expected_hashes["small.bin"] = new_hash

                api.upload_file(
                    path_or_fileobj=str(overwrite_path),
                    path_in_repo="small.bin",
                    repo_id=repo_name,
                )

                local_path = hf_hub_download(
                    repo_id=repo_name,
                    filename="small.bin",
                    local_dir=str(download_dir / "overwrite"),
                    force_download=True,
                )
                actual_hash = sha256_file(local_path)
                assert actual_hash == new_hash, (
                    f"Overwrite hash mismatch: expected {new_hash[:16]}..., got {actual_hash[:16]}..."
                )
                print(f"  Overwrite verified: new content downloaded correctly")

            run_test("Upload overwrite and verify", test_upload_overwrite)

    finally:
        if not args.keep_repo:
            print(f"\nCleaning up: deleting {repo_name}...")
            try:
                api.delete_repo(repo_name)
                print("  Deleted.")
            except Exception as e:
                print(f"  Warning: failed to delete repo: {e}", file=sys.stderr)
        else:
            print(f"\n--keep-repo set, leaving {repo_name} intact")

    # Summary
    print(f"\n{'='*60}")
    print(f"RESULTS: {results['passed']} passed, {results['failed']} failed")
    print(f"{'='*60}")
    if results["errors"]:
        for name, err in results["errors"]:
            print(f"  FAIL: {name}: {err}")
        sys.exit(1)
    else:
        print("All smoke tests passed!")


if __name__ == "__main__":
    main()
