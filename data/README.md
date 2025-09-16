# data

A high-level data translation layer for Xet's content-addressable storage (CAS). This crate handles:

- **Cleaning (uploading)** regular files into deduplicated CAS objects (xorbs + shards) and producing lightweight pointers (`XetFileInfo`).
- **Smudging (downloading)** pointer metadata back into materialized files.

## Core APIs

- **High-level async functions** in `data::data_client`:
  - `upload_async(file_paths, endpoint, token_info, token_refresher, progress_updater) -> Vec<XetFileInfo>`
  - `download_async(files: Vec<(XetFileInfo, String)>, endpoint, token_info, token_refresher, progress_updaters) -> Vec<String>`

- **Sessions and primitives** (re-exported at the crate root):
  - `FileUploadSession` – multi-file, deduplicated upload session. Handles chunking, xorb/shard production, and finalization.
  - `FileDownloader` – smudges files from CAS given a `MerkleHash`/`XetFileInfo`.
  - `XetFileInfo` – compact pointer describing a file by its hash and size.

Both high-level functions create sensible defaults (cache paths, progress aggregation, endpoint separation) via `data_client::default_config` and enforce bounded concurrency.

## How `hf_xet` uses this crate

The `hf_xet` Python extension exposes thin wrappers around these async functions and types. In `hf_xet/src/lib.rs`:

- `upload_files(...)` calls `data::data_client::upload_async`.
- `upload_bytes(...)` calls `data::data_client::upload_bytes_async`.
- `download_files(...)` calls `data::data_client::download_async`.
