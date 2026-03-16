# API Update: SHA-256 in XetFileInfo and FileMetadata (2026-03-16)

## Overview

`XetFileInfo` and the session-layer `FileMetadata` now include the SHA-256
hash of uploaded file content.  This was previously computed internally but
not surfaced.  Downstream consumers (e.g. OpenDAL's Hugging Face backend)
need the SHA-256 to commit files to the Hub API.

---

## 1. `XetFileInfo` (crate: `xet-data`)

**Path:** `xet_data::processing::XetFileInfo`

### New field

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub sha256: Option<String>,
```

The field is `None` for downloads and when `Sha256Policy::Skip` is used.
It is populated automatically when `Sha256Policy::Compute` or
`Sha256Policy::Provided` is used during upload.

Serde attributes ensure backward-compatible (de)serialisation: the field
is omitted from JSON when `None`, and absent keys deserialise as `None`.

### New constructor

```rust
pub fn new_with_sha256(hash: String, file_size: u64, sha256: String) -> Self
```

### New accessor

```rust
pub fn sha256(&self) -> Option<&str>
```

### Existing constructor unchanged

`XetFileInfo::new(hash, file_size)` still works and sets `sha256: None`.

### Struct literal construction

All code that constructs `XetFileInfo` with struct literal syntax must now
include the `sha256` field:

```rust
// Before
XetFileInfo { hash: h, file_size: s }

// After
XetFileInfo { hash: h, file_size: s, sha256: None }
// or, when propagating from FileMetadata:
XetFileInfo { hash: m.hash.clone(), file_size: m.file_size, sha256: m.sha256.clone() }
```

---

## 2. `SingleFileCleaner::finish()` (crate: `xet-data`)

**Path:** `xet_data::processing::SingleFileCleaner::finish`

The returned `XetFileInfo` now has `sha256` populated when a SHA-256 was
computed or provided during the upload.  No signature change.

---

## 3. `FileMetadata` (crate: `xet` / session layer)

**Path:** `xet::xet_session::FileMetadata`

### New field

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub sha256: Option<String>,
```

Populated from `XetFileInfo::sha256()` during `UploadCommit::commit()`.

---

## 4. `PyXetUploadInfo` (crate: `hf_xet` / Python bindings)

### New field

```rust
#[pyo3(get)]
pub sha256: Option<String>,
```

Populated from `XetFileInfo::sha256()` in the `From<XetFileInfo>` conversion.
Python callers can access it as `upload_info.sha256`.
