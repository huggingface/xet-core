# hf_xet Python Examples

This directory contains examples demonstrating the new session-based API for hf_xet.

## Installation

First, build and install hf_xet in development mode:

```bash
# Activate Python virtual environment
. ~/.venv/bin/activate

# Build and install hf_xet
cd hf_xet
maturin develop

# To deactivate when done
deactivate
```

## Examples

### 1. Session Demo (`session_demo.py`)

A complete command-line application demonstrating batch uploads and downloads with real-time progress monitoring.

**Upload files:**
```bash
python examples/session_demo.py upload file1.txt file2.bin file3.dat
```

This will:
- Create a session
- Upload all files in parallel
- Show real-time progress
- Save metadata to `upload_metadata.json`

**Download files:**
```bash
python examples/session_demo.py download upload_metadata.json -o ./downloads
```

This will:
- Load metadata from JSON
- Download all files in parallel
- Show real-time progress
- Save files to output directory

**Round-trip test:**
```bash
python examples/session_demo.py round-trip test.txt data.bin -o ./restored
```

This demonstrates the complete workflow: upload → download.

**With custom endpoint:**
```bash
python examples/session_demo.py upload file.txt --endpoint https://my-server.com
```

### 2. Basic Usage (`basic_usage.py`)

Simple, focused examples showing individual API features. Uncomment the examples you want to run:

```python
# In basic_usage.py, uncomment one or more:
example_upload()           # Simple single file upload
example_download()         # Simple single file download
example_batch_upload()     # Upload multiple files
example_configuration()    # Different config options
example_progress_details() # Detailed progress info
example_error_handling()   # Error handling patterns
```

Run it:
```bash
python examples/basic_usage.py
```

## Key Features

### 1. Session-Based API

```python
import hf_xet

# Create a session (manages runtime and config)
session = hf_xet.XetSession(
    config=hf_xet.XetConfig(),
    endpoint="https://my-server.com",  # optional
    token_info=("token", expiry),      # optional
)
```

### 2. Batch Uploads with UploadCommit

```python
# Create upload commit (groups related uploads)
upload_commit = session.new_upload_commit()

# Upload files (executes immediately in background)
task_id1 = upload_commit.upload_file("file1.txt")
task_id2 = upload_commit.upload_file("file2.txt")

# Poll progress (GIL-free - no callbacks!)
progress = upload_commit.get_progress()
for p in progress:
    print(f"{p.file_name}: {p.bytes_completed}/{p.bytes_total} - {p.status}")

# Commit to finalize
metadata = upload_commit.commit()
```

### 3. Batch Downloads with DownloadGroup

```python
# Create download group
download_group = session.new_download_group()

# Start downloads (executes immediately)
task_id = download_group.download_file(
    file_hash="abc123...",
    file_size=1024,
    dest_path="output.txt"
)

# Poll progress
progress = download_group.get_progress()

# Wait for completion
results = download_group.finish()
```

### 4. GIL-Free Progress Polling

Unlike callback-based approaches, progress polling doesn't hold the GIL:

```python
# Fast, efficient progress checking
progress = upload_commit.get_progress()

# No Python callbacks means:
# - Better performance
# - No GIL contention
# - Simpler code
# - You control polling frequency
```

### 5. Per-Session Configuration

Each session can have different settings:

```python
# Development session (local)
dev_session = hf_xet.XetSession()

# Production session (remote)
prod_session = hf_xet.XetSession(
    endpoint="https://prod-server.com",
    token_info=get_prod_token(),
)
```

### 6. Explicit Resource Management

```python
# Sessions manage their own runtime
session = hf_xet.XetSession()

# ... do work ...

# Clean up when done
session.end()
```

## Progress Information

Each upload/download provides detailed progress:

```python
progress = upload_commit.get_progress()
for p in progress:
    print(f"Task ID: {p.task_id}")
    print(f"File: {p.file_name}")
    print(f"Status: {p.status}")  # Queued, Running, Completed, Failed
    print(f"Progress: {p.bytes_completed}/{p.bytes_total} bytes")
    print(f"Speed: {p.speed_bps} bytes/sec")
```

## File Metadata

After committing uploads, you get metadata for each file:

```python
metadata = upload_commit.commit()
for m in metadata:
    print(f"File: {m.file_name}")
    print(f"Hash: {m.hash}")      # Content-addressed hash
    print(f"Size: {m.file_size}")  # File size in bytes
```

This metadata can be saved and used later for downloads:

```python
import json

# Save metadata
with open("metadata.json", "w") as f:
    json.dump([{
        "file_name": m.file_name,
        "hash": m.hash,
        "file_size": m.file_size,
    } for m in metadata], f)

# Load and use for downloads
with open("metadata.json", "r") as f:
    metadata = json.load(f)

for item in metadata:
    download_group.download_file(
        item["hash"],
        item["file_size"],
        f"./downloads/{item['file_name']}"
    )
```

## Error Handling

```python
try:
    session = hf_xet.XetSession()
    upload_commit = session.new_upload_commit()

    task_id = upload_commit.upload_file("file.txt")

    # Check for failures
    progress = upload_commit.get_progress()
    for p in progress:
        if p.status == "Failed":
            print(f"Upload failed: {p.file_name}")

    metadata = upload_commit.commit()
    session.end()

except Exception as e:
    print(f"Error: {e}")
```

## Benefits Over Old API

### Old API (Deprecated)
```python
# Old: Single file at a time with callbacks
results = hf_xet.upload_files(
    ["file.txt"],
    progress_callback=my_callback  # GIL contention!
)
```

### New API (Recommended)
```python
# New: Batch operations with polling
session = hf_xet.XetSession()
upload_commit = session.new_upload_commit()

# Upload multiple files
upload_commit.upload_file("file1.txt")
upload_commit.upload_file("file2.txt")

# Poll progress (GIL-free!)
progress = upload_commit.get_progress()

# Commit all at once
metadata = upload_commit.commit()
session.end()
```

**Advantages:**
1. ✅ **Better Performance** - GIL-free progress polling
2. ✅ **Batch Operations** - Group related uploads/downloads
3. ✅ **Flexible Configuration** - Per-session settings
4. ✅ **Simpler Code** - No callback management
5. ✅ **Better Control** - You decide polling frequency

## Common Patterns

### Pattern 1: Upload Multiple Files

```python
session = hf_xet.XetSession()
upload_commit = session.new_upload_commit()

files = ["file1.txt", "file2.txt", "file3.txt"]
for f in files:
    upload_commit.upload_file(f)

# Wait for completion
while True:
    progress = upload_commit.get_progress()
    if all(p.status in ("Completed", "Failed") for p in progress):
        break
    time.sleep(0.5)

metadata = upload_commit.commit()
session.end()
```

### Pattern 2: Progress Bar

```python
import time

upload_commit.upload_file("large_file.bin")

while True:
    progress = upload_commit.get_progress()
    p = progress[0]

    if p.bytes_total > 0:
        pct = (p.bytes_completed / p.bytes_total) * 100
        bar = "=" * int(pct / 2) + " " * (50 - int(pct / 2))
        print(f"\r[{bar}] {pct:.1f}%", end="", flush=True)

    if p.status in ("Completed", "Failed"):
        print()
        break

    time.sleep(0.1)
```

### Pattern 3: Download from Saved Metadata

```python
import json

# Load metadata from previous upload
with open("upload_metadata.json") as f:
    metadata = json.load(f)

# Download all files
session = hf_xet.XetSession()
download_group = session.new_download_group()

for item in metadata:
    download_group.download_file(
        item["hash"],
        item["file_size"],
        f"./restored/{item['file_name']}"
    )

results = download_group.finish()
session.end()
```

## Troubleshooting

**Import Error:**
```
ModuleNotFoundError: No module named 'hf_xet'
```
Solution: Run `maturin develop` in the hf_xet directory.

**Permission Errors:**
Make sure the virtual environment is activated:
```bash
. ~/.venv/bin/activate
```

**Runtime Errors:**
Check that you're calling `session.end()` when done to clean up resources.

## Next Steps

- See the [implementation plan](../../../.claude/plans/) for architecture details
- Check the [Rust examples](../../../xet_session/examples/) for Rust usage
- Read the source code for advanced usage patterns
