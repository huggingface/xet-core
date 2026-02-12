#!/usr/bin/env python3
"""
Basic usage examples for the hf_xet session-based API.

This script shows simple, focused examples of each key operation.
"""

import time
import hf_xet


def example_upload():
    """Simple upload example."""
    print("=== Upload Example ===\n")

    # 1. Create a session
    session = hf_xet.XetSession()

    # 2. Create an upload commit
    upload_commit = session.new_upload_commit()

    # 3. Upload files (returns immediately, executes in background)
    task_id = upload_commit.upload_file("test.txt")
    print(f"Started upload, task ID: {task_id}")

    # 4. Poll progress (GIL-free - no Python callbacks!)
    while True:
        progress = upload_commit.get_progress()
        for p in progress:
            print(f"  {p.file_name}: {p.bytes_completed}/{p.bytes_total} bytes - {p.status}")

        # Check if done
        if all(p.status in ("Completed", "Failed") for p in progress):
            break

        time.sleep(0.5)

    # 5. Commit to finalize
    metadata = upload_commit.commit()
    print(f"\nUpload complete!")
    for m in metadata:
        print(f"  File: {m.file_name}")
        print(f"  Hash: {m.hash}")
        print(f"  Size: {m.file_size} bytes")

    # 6. Clean up
    session.end()


def example_download():
    """Simple download example."""
    print("\n=== Download Example ===\n")

    # Example metadata from a previous upload
    file_hash = "your-file-hash-here"
    file_size = 1024
    dest_path = "downloaded_file.txt"

    # 1. Create a session
    session = hf_xet.XetSession()

    # 2. Create a download group
    download_group = session.new_download_group()

    # 3. Start download (returns immediately)
    task_id = download_group.download_file(file_hash, file_size, dest_path)
    print(f"Started download, task ID: {task_id}")

    # 4. Poll progress
    while True:
        progress = download_group.get_progress()
        for p in progress:
            print(f"  {p.dest_path}: {p.bytes_completed}/{p.bytes_total} bytes - {p.status}")

        if all(p.status in ("Completed", "Failed") for p in progress):
            break

        time.sleep(0.5)

    # 5. Finish to wait for completion
    results = download_group.finish()
    print(f"\nDownload complete!")
    for r in results:
        print(f"  Saved to: {r.dest_path}")

    # 6. Clean up
    session.end()


def example_batch_upload():
    """Batch upload example - upload multiple files."""
    print("\n=== Batch Upload Example ===\n")

    session = hf_xet.XetSession()
    upload_commit = session.new_upload_commit()

    # Upload multiple files
    files = ["file1.txt", "file2.txt", "file3.txt"]
    task_ids = []

    for file_path in files:
        task_id = upload_commit.upload_file(file_path)
        task_ids.append(task_id)
        print(f"Started: {file_path} (task: {task_id})")

    # Monitor all uploads
    print("\nMonitoring progress...")
    while True:
        progress = upload_commit.get_progress()

        # Calculate totals
        total_bytes = sum(p.bytes_total for p in progress)
        completed_bytes = sum(p.bytes_completed for p in progress)
        completed_count = sum(1 for p in progress if p.status == "Completed")

        percentage = (completed_bytes / total_bytes * 100) if total_bytes > 0 else 0
        print(f"  Progress: {completed_count}/{len(files)} files, "
              f"{completed_bytes}/{total_bytes} bytes ({percentage:.1f}%)")

        if completed_count == len(files):
            break

        time.sleep(0.5)

    metadata = upload_commit.commit()
    print(f"\nAll {len(metadata)} files uploaded!")

    session.end()


def example_configuration():
    """Example showing different configuration options."""
    print("\n=== Configuration Example ===\n")

    # Option 1: Default configuration
    session1 = hf_xet.XetSession()
    print("Created session with default config")
    session1.end()

    # Option 2: With custom config
    config = hf_xet.XetConfig()
    # Note: XetConfig fields can be set via environment variables
    # or the config object (when that functionality is added)
    session2 = hf_xet.XetSession(config=config)
    print("Created session with custom config")
    session2.end()

    # Option 3: With endpoint
    session3 = hf_xet.XetSession(
        endpoint="https://my-cas-server.com"
    )
    print("Created session with custom endpoint")
    session3.end()

    # Option 4: With authentication
    session4 = hf_xet.XetSession(
        endpoint="https://my-cas-server.com",
        token_info=("my-token", 1234567890),  # (token, expiry)
    )
    print("Created session with authentication")
    session4.end()


def example_progress_details():
    """Example showing detailed progress information."""
    print("\n=== Progress Details Example ===\n")

    session = hf_xet.XetSession()
    upload_commit = session.new_upload_commit()

    task_id = upload_commit.upload_file("large_file.bin")

    print("Detailed progress monitoring:")
    while True:
        progress_list = upload_commit.get_progress()

        for p in progress_list:
            print(f"\nTask ID: {p.task_id}")
            print(f"  File: {p.file_name}")
            print(f"  Status: {p.status}")
            print(f"  Progress: {p.bytes_completed:,} / {p.bytes_total:,} bytes")
            print(f"  Speed: {p.speed_bps:.2f} bytes/sec")

            if p.bytes_total > 0:
                percentage = (p.bytes_completed / p.bytes_total) * 100
                print(f"  Percentage: {percentage:.1f}%")

        if all(p.status in ("Completed", "Failed") for p in progress_list):
            break

        time.sleep(1)

    upload_commit.commit()
    session.end()


def example_error_handling():
    """Example showing error handling."""
    print("\n=== Error Handling Example ===\n")

    try:
        session = hf_xet.XetSession()
        upload_commit = session.new_upload_commit()

        # This will fail if file doesn't exist
        upload_commit.upload_file("nonexistent_file.txt")

        # Wait a bit for the error to appear
        time.sleep(1)

        progress = upload_commit.get_progress()
        for p in progress:
            if p.status == "Failed":
                print(f"Upload failed for {p.file_name}")

        # Try to commit anyway (will include only successful uploads)
        metadata = upload_commit.commit()
        print(f"Committed {len(metadata)} successful uploads")

        session.end()

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    print("hf_xet Session API Examples\n")
    print("=" * 50)

    # Run examples
    # Uncomment the examples you want to run:

    # example_upload()
    # example_download()
    # example_batch_upload()
    # example_configuration()
    # example_progress_details()
    # example_error_handling()

    print("\nNote: Uncomment the examples you want to run in the main block!")
