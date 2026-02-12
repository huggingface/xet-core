# XetSession Examples

This directory contains examples demonstrating the session-based API for XetHub.

## Session Demo

The `session_demo` example demonstrates the complete session-based workflow for uploading and downloading files.

### Features Demonstrated

- **Session Management**: Creating and managing a XetSession
- **Batch Uploads**: Using UploadCommit to group related uploads
- **Batch Downloads**: Using DownloadGroup to group related downloads
- **Progress Polling**: GIL-free progress tracking without callbacks
- **Round-trip**: Full upload → download workflow

### Usage

#### Upload Files

Upload one or more files and save their metadata:

```bash
cargo run --example session_demo -- upload file1.txt file2.bin file3.dat
```

This will:
1. Create a session
2. Create an upload commit
3. Upload all files (in parallel)
4. Poll progress until complete
5. Commit and save metadata to `upload_metadata.json`

#### Download Files

Download files using metadata from a previous upload:

```bash
cargo run --example session_demo -- download upload_metadata.json --output-dir ./downloads
```

This will:
1. Create a session
2. Load metadata from the JSON file
3. Create a download group
4. Download all files (in parallel)
5. Save files to the output directory

#### Round-Trip Test

Test both upload and download in one command:

```bash
cargo run --example session_demo -- round-trip file1.txt file2.bin --output-dir ./restored
```

This demonstrates:
1. Upload files using UploadCommit
2. Download the same files using DownloadGroup
3. Verify the complete workflow

### Key API Patterns

```rust
// 1. Create a session
let config = XetConfig::new();
let session = XetSession::new(config)?;

// 2. Upload files using a commit
let upload_commit = session.new_upload_commit()?;
upload_commit.upload_file("file.txt".to_string()).await?;

// 3. Poll progress (GIL-free, no callbacks)
let progress = upload_commit.get_progress();
for p in progress {
    println!("File: {:?}, Progress: {}/{} bytes, Status: {:?}",
             p.file_name, p.bytes_completed, p.bytes_total, p.status);
}

// 4. Commit to finalize
let metadata = upload_commit.commit().await?;

// 5. Download files using a group
let download_group = session.new_download_group()?;
download_group.download_file(hash, size, "dest.txt".to_string()).await?;

// 6. Finish to wait for completion
let results = download_group.finish().await?;

// 7. End the session
session.end().await?;
```

### Advanced Options

#### Custom Endpoint

Specify a custom CAS endpoint:

```bash
cargo run --example session_demo -- upload file.txt --endpoint https://my-cas-server.com
```

#### Output Directory

Specify where to save downloaded files:

```bash
cargo run --example session_demo -- download metadata.json --output-dir /path/to/output
```

## Benefits of Session-Based API

1. **Batch Operations**: Group related uploads/downloads together
2. **GIL-Free Progress**: Poll progress without Python callback overhead
3. **Per-Session Configuration**: Each session can have different settings
4. **Independent Finalization**: Commits/groups finalize independently
5. **Resource Management**: Explicit session lifecycle management
