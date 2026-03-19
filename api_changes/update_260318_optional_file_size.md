# `XetFileInfo.file_size` is now `Option<u64>`

**Date**: 2026-03-18
**Crate**: `xet-data` (`xet_data::processing::XetFileInfo`)

## What changed

`XetFileInfo.file_size` changed from `u64` to `Option<u64>`.
The `file_size()` accessor now returns `Option<u64>`.

Downstream API surfaces that consume `XetFileInfo` were updated accordingly:

- `xet_pkg` session/examples/tests now construct download `XetFileInfo` values
  with `file_size: Some(size)` when known.
- `hf_xet::PyXetDownloadInfo.file_size` is now `Option<u64>`, and converting
  from Python download metadata supports hash-only downloads.
- `hf_xet::PyPointerFile.filesize` getter also returns `Option<u64>` now.
- `hf_xet::PyXetUploadInfo` still exposes `file_size: u64`; upload metadata is
  expected to always provide a known size.

## Why

The download path no longer requires callers to know the file size upfront.
When `file_size` is `None`, the reconstruction discovers the actual size
from the remote and progress tracking updates incrementally.

## Migration

### Struct literal construction

```rust
// Before
XetFileInfo { hash: h, file_size: s, sha256: None }

// After
XetFileInfo { hash: h, file_size: Some(s), sha256: None }
```

### Using the constructor (no change needed)

```rust
// XetFileInfo::new wraps in Some internally
XetFileInfo::new(hash, file_size)
```

### New: hash-only constructor for unknown size

```rust
XetFileInfo::new_hash_only(hash)
```

### Reading file_size

```rust
// Before
let size: u64 = info.file_size();

// After
let size: Option<u64> = info.file_size();
// or when size is known to be present:
let size: u64 = info.file_size().expect("size should be set");
```

### Serde

`Some(n)` serializes as `n` (backward compatible).
`None` omits the field. Missing field deserializes as `None`.

## New error variant

`DataError::SizeMismatch { expected, actual }` is returned when a download
completes but the actual byte count differs from the specified `file_size`.

This check runs after full-file reconstruction and works for both larger and
smaller actual byte counts relative to the caller-provided value.
