# API Update: Unified auth group builder replaces per-type builder structs (2026-04-02)

## Overview

`UploadCommitBuilder`, `FileDownloadGroupBuilder`, and `DownloadStreamGroupBuilder` have
been replaced by `XetUploadCommitBuilder`, `XetFileDownloadGroupBuilder`, and
`XetDownloadStreamGroupBuilder`.  All three share the same four configuration methods
(`with_endpoint`, `with_custom_headers`, `with_token_info`, `with_token_refresh_url`);
`build` and `build_blocking` remain type-specific.

---

## Breaking Changes

### Removed types

- `UploadCommitBuilder`
- `FileDownloadGroupBuilder`
- `DownloadStreamGroupBuilder`

### `with_endpoint` removed from `XetSessionBuilder`

The CAS endpoint is no longer set on the session; it is now set per-operation on the
builder.  Any call to `XetSessionBuilder::with_endpoint` must be moved to the builder
chain of each individual operation:

```rust
// Before
let session = XetSessionBuilder::new()
    .with_endpoint("https://cas.example.com")
    .build()?;
let commit = session.new_upload_commit()?.build_blocking()?;

// After
let session = XetSessionBuilder::new().build()?;
let commit = session.new_upload_commit()?
    .with_endpoint("https://cas.example.com")
    .build_blocking()?;
```

### Changed return types on `XetSession`

All three factory methods now return the named type aliases instead of the old builder
types:

```rust
// Before
pub fn new_upload_commit(&self) -> Result<UploadCommitBuilder, SessionError>
pub fn new_file_download_group(&self) -> Result<FileDownloadGroupBuilder, SessionError>
pub fn new_download_stream_group(&self) -> Result<DownloadStreamGroupBuilder, SessionError>

// After
pub fn new_upload_commit(&self) -> Result<XetUploadCommitBuilder, SessionError>
pub fn new_file_download_group(&self) -> Result<XetFileDownloadGroupBuilder, SessionError>
pub fn new_download_stream_group(&self) -> Result<XetDownloadStreamGroupBuilder, SessionError>
```

---

## Migration Guide

Method names and call sites are unchanged — only the concrete type used in type
annotations needs updating:

```rust
// Before
let builder: UploadCommitBuilder = session.new_upload_commit()?;
let builder: FileDownloadGroupBuilder = session.new_file_download_group()?;
let builder: DownloadStreamGroupBuilder = session.new_download_stream_group()?;

// After
let builder: XetUploadCommitBuilder = session.new_upload_commit()?;
let builder: XetFileDownloadGroupBuilder = session.new_file_download_group()?;
let builder: XetDownloadStreamGroupBuilder = session.new_download_stream_group()?;
```

In practice most call sites use method chaining and never name the builder type, so
no changes are needed:

```rust
// Unchanged — no type annotation, chaining still works
let commit = session.new_upload_commit()?
    .with_token_refresh_url(url, headers)
    .with_token_info(token, expiry)
    .build_blocking()?;
```

---

## New capability: `with_endpoint` and `with_custom_headers` on stream groups

These methods were previously unavailable on `DownloadStreamGroupBuilder`.  They are
now available on all three builder variants:

```rust
let stream_group = session.new_download_stream_group()?
    .with_endpoint("https://cas.example.com")
    .with_custom_headers(headers)
    .with_token_refresh_url(url, refresh_headers)
    .build().await?;
```

---

## Endpoint resolution during `build`

`create_translator_config` resolves the CAS endpoint in this order:

1. `with_endpoint(...)` — used as-is if provided.
2. **If `with_endpoint` is omitted but `with_token_refresh_url` is set**, the refresher
   is called once eagerly during `build` to obtain the CAS URL.  The token from that
   response is stored as the initial `token_info` **only if `with_token_info` was not
   already called** — a pre-seeded token is preserved as-is.
3. The session's `default_cas_endpoint` from `XetConfig` — used when neither of the
   above applies (local or pre-configured deployments).

The common patterns are:

```rust
// Pattern A: endpoint known ahead of time — no eager refresh, token_info is used as-is
session.new_upload_commit()?
    .with_endpoint(cas_url)
    .with_token_info(token, expiry)
    .with_token_refresh_url(refresh_url, refresh_headers)
    .build_blocking()?;

// Pattern B: endpoint unknown — first build call fetches it; token_info seeded from response
session.new_upload_commit()?
    .with_token_refresh_url(refresh_url, refresh_headers)
    .build_blocking()?;
```

---

## Affected Files

- `xet_pkg/src/xet_session/auth_group_builder.rs` — new shared builder implementation
- `xet_pkg/src/xet_session/common.rs` — endpoint resolution logic updated
- `xet_pkg/src/xet_session/session.rs` — factory method return types updated
- `xet_pkg/src/xet_session/upload_commit.rs` — exports `XetUploadCommitBuilder`
- `xet_pkg/src/xet_session/file_download_group.rs` — exports `XetFileDownloadGroupBuilder`; `with_endpoint`/`with_custom_headers` bug fixed
- `xet_pkg/src/xet_session/download_stream_group.rs` — exports `XetDownloadStreamGroupBuilder`; gains `with_endpoint` and `with_custom_headers`
