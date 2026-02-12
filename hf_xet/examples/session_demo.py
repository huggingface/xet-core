#!/usr/bin/env python3
"""
Session-based upload/download example for hf_xet.

This example demonstrates the new session-based API that provides:
- Batch uploads with UploadCommit
- Batch downloads with DownloadGroup
- GIL-free progress polling (no callbacks!)
- Per-session configuration
- Explicit resource management
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import List

try:
    import hf_xet
except ImportError:
    print("Error: hf_xet module not found. Please install it first:")
    print("  cd hf_xet && maturin develop")
    sys.exit(1)


def upload_files(file_paths: List[str], endpoint: str = None):
    """Upload files using XetSession and UploadCommit."""
    print("📤 Starting upload session...")

    # Create session with optional endpoint
    session = hf_xet.XetSession(
        config=hf_xet.XetConfig(),
        endpoint=endpoint,
    )
    print("✅ Session created")

    # Create an upload commit (groups related uploads)
    upload_commit = session.new_upload_commit()
    print("📦 Upload commit created")

    # Start uploading all files (tasks execute immediately)
    print(f"\n🚀 Starting uploads for {len(file_paths)} files...")
    task_ids = []
    for file_path in file_paths:
        task_id = upload_commit.upload_file(file_path)
        task_ids.append(task_id)
        print(f"  ⏫ Started upload: {file_path} (task: {task_id})")

    # Poll progress until all uploads complete
    print("\n📊 Monitoring progress...")
    last_percentage = -1
    while True:
        progress_list = upload_commit.get_progress()

        total_files = len(progress_list)
        completed = sum(1 for p in progress_list if p.status == "Completed")
        failed = sum(1 for p in progress_list if p.status == "Failed")

        total_bytes = sum(p.bytes_total for p in progress_list)
        completed_bytes = sum(p.bytes_completed for p in progress_list)

        percentage = int((completed_bytes / total_bytes * 100)) if total_bytes > 0 else 0

        # Only print when percentage changes to reduce output
        if percentage != last_percentage:
            print(f"  Progress: {completed}/{total_files} files | "
                  f"{completed_bytes:,}/{total_bytes:,} bytes ({percentage}%)")
            last_percentage = percentage

        # Check if all done
        if completed + failed == total_files:
            print()
            break

        time.sleep(0.1)

    # Commit - finalize uploads and get metadata
    print("💾 Committing uploads...")
    metadata = upload_commit.commit()

    print(f"\n✅ Upload complete! Metadata for {len(metadata)} files:")
    for m in metadata:
        print(f"  📄 {m.file_name} -> {m.hash} ({m.file_size:,} bytes)")

    # Save metadata to file for later download
    metadata_dict = [
        {
            "file_name": m.file_name,
            "hash": m.hash,
            "file_size": m.file_size,
        }
        for m in metadata
    ]

    with open("upload_metadata.json", "w") as f:
        json.dump(metadata_dict, f, indent=2)
    print("\n💾 Metadata saved to upload_metadata.json")

    # End the session
    session.end()
    print("🏁 Session ended")


def download_files(metadata_file: str, output_dir: str, endpoint: str = None):
    """Download files using metadata from a previous upload."""
    print("📥 Starting download session...")

    # Load metadata
    with open(metadata_file, "r") as f:
        metadata = json.load(f)

    # Create session
    session = hf_xet.XetSession(
        config=hf_xet.XetConfig(),
        endpoint=endpoint,
    )
    print("✅ Session created")

    # Create download group
    download_group = session.new_download_group()
    print("📦 Download group created")

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Start downloading all files
    print(f"\n🚀 Starting downloads for {len(metadata)} files...")
    task_ids = []
    for item in metadata:
        file_hash = item["hash"]
        file_size = item["file_size"]
        file_name = item["file_name"]

        dest_path = str(output_path / file_name)
        task_id = download_group.download_file(file_hash, file_size, dest_path)
        task_ids.append(task_id)
        print(f"  ⏬ Started download: {file_name} (task: {task_id})")

    # Poll progress
    print("\n📊 Monitoring progress...")
    last_percentage = -1
    while True:
        progress_list = download_group.get_progress()

        total_files = len(progress_list)
        completed = sum(1 for p in progress_list if p.status == "Completed")
        failed = sum(1 for p in progress_list if p.status == "Failed")

        total_bytes = sum(p.bytes_total for p in progress_list)
        completed_bytes = sum(p.bytes_completed for p in progress_list)

        percentage = int((completed_bytes / total_bytes * 100)) if total_bytes > 0 else 0

        if percentage != last_percentage:
            print(f"  Progress: {completed}/{total_files} files | "
                  f"{completed_bytes:,}/{total_bytes:,} bytes ({percentage}%)")
            last_percentage = percentage

        if completed + failed == total_files:
            print()
            break

        time.sleep(0.1)

    # Finish - wait for all downloads
    print("💾 Finishing downloads...")
    results = download_group.finish()

    print(f"\n✅ Download complete! {len(results)} files:")
    for r in results:
        print(f"  📄 {r.file_hash[:16]}... -> {r.dest_path}")

    # End session
    session.end()
    print("🏁 Session ended")


def round_trip(file_paths: List[str], output_dir: str):
    """Demonstrate full round-trip: upload then download."""
    print("🔄 Starting round-trip demo (upload then download)...\n")

    # === UPLOAD PHASE ===
    print("=== UPLOAD PHASE ===")
    session = hf_xet.XetSession()

    upload_commit = session.new_upload_commit()
    print("📦 Created upload commit")

    # Upload files
    for file_path in file_paths:
        upload_commit.upload_file(file_path)
    print(f"⏫ Started {len(file_paths)} uploads")

    # Wait for uploads
    while True:
        progress = upload_commit.get_progress()
        all_done = all(p.status in ("Completed", "Failed") for p in progress)
        if all_done:
            break
        time.sleep(0.1)

    metadata = upload_commit.commit()
    print(f"✅ Uploaded {len(metadata)} files\n")

    # === DOWNLOAD PHASE ===
    print("=== DOWNLOAD PHASE ===")
    download_group = session.new_download_group()
    print("📦 Created download group")

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Download files
    for m in metadata:
        dest_path = str(output_path / m.file_name)
        download_group.download_file(m.hash, m.file_size, dest_path)
    print(f"⏬ Started {len(metadata)} downloads")

    # Wait for downloads
    while True:
        progress = download_group.get_progress()
        all_done = all(p.status in ("Completed", "Failed") for p in progress)
        if all_done:
            break
        time.sleep(0.1)

    results = download_group.finish()
    print(f"✅ Downloaded {len(results)} files to {output_dir}\n")

    # End session
    session.end()
    print("🏁 Round-trip complete!")


def main():
    parser = argparse.ArgumentParser(
        description="Demonstrates the XetSession API for batch uploads/downloads"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Upload command
    upload_parser = subparsers.add_parser("upload", help="Upload files")
    upload_parser.add_argument("files", nargs="+", help="Files to upload")
    upload_parser.add_argument("--endpoint", help="CAS endpoint (optional)")

    # Download command
    download_parser = subparsers.add_parser("download", help="Download files")
    download_parser.add_argument("metadata_file", help="JSON file with metadata")
    download_parser.add_argument(
        "-o", "--output-dir",
        default="./downloads",
        help="Output directory (default: ./downloads)"
    )
    download_parser.add_argument("--endpoint", help="CAS endpoint (optional)")

    # Round-trip command
    roundtrip_parser = subparsers.add_parser(
        "round-trip",
        help="Upload then download files"
    )
    roundtrip_parser.add_argument("files", nargs="+", help="Files to upload/download")
    roundtrip_parser.add_argument(
        "-o", "--output-dir",
        default="./downloads",
        help="Output directory (default: ./downloads)"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        if args.command == "upload":
            upload_files(args.files, args.endpoint)
        elif args.command == "download":
            download_files(args.metadata_file, args.output_dir, args.endpoint)
        elif args.command == "round-trip":
            round_trip(args.files, args.output_dir)
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
