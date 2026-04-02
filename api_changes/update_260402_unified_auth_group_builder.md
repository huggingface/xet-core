# API Update: Unified `AuthGroupBuilder<G>` replaces per-type builder structs (2026-04-02)

## Overview

`UploadCommitBuilder`, `FileDownloadGroupBuilder`, and `DownloadStreamGroupBuilder` have
been replaced by a single generic `AuthGroupBuilder<G>`.  The five shared configuration
methods (`with_endpoint`, `with_custom_headers`, `with_token_info`,
`with_token_refresh_url`) are now implemented once on `AuthGroupBuilder<G>`; `build` and
`build_blocking` remain type-specific via separate `impl` blocks per product type.

---

## Breaking Changes

### Removed types

- `UploadCommitBuilder`
- `FileDownloadGroupBuilder`
- `DownloadStreamGroupBuilder`

### `with_endpoint` removed from `XetSessionBuilder`

The CAS endpoint is no longer set on the session; it is now set per-operation on the
`AuthGroupBuilder`.  Any call to `XetSessionBuilder::with_endpoint` must be moved to the
builder chain of each individual operation:

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

All three factory methods now return `AuthGroupBuilder<G>` instead of the named builder
types:

```rust
// Before
pub fn new_upload_commit(&self) -> Result<UploadCommitBuilder, SessionError>
pub fn new_file_download_group(&self) -> Result<FileDownloadGroupBuilder, SessionError>
pub fn new_download_stream_group(&self) -> Result<DownloadStreamGroupBuilder, SessionError>

// After
pub fn new_upload_commit(&self) -> Result<AuthGroupBuilder<XetUploadCommit>, SessionError>
pub fn new_file_download_group(&self) -> Result<AuthGroupBuilder<XetFileDownloadGroup>, SessionError>
pub fn new_download_stream_group(&self) -> Result<AuthGroupBuilder<XetDownloadStreamGroup>, SessionError>
```

### New shared configuration methods on `AuthGroupBuilder<G>`

`DownloadStreamGroupBuilder` previously lacked `with_endpoint` and
`with_custom_headers`.  Both are now present on `AuthGroupBuilder<G>` and apply to all
three operation types.

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
let builder: AuthGroupBuilder<XetUploadCommit> = session.new_upload_commit()?;
let builder: AuthGroupBuilder<XetFileDownloadGroup> = session.new_file_download_group()?;
let builder: AuthGroupBuilder<XetDownloadStreamGroup> = session.new_download_stream_group()?;
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
now available on all three builder variants via `AuthGroupBuilder<G>`:

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

## Internal Changes

### New type: `AuthOptions`

The four shared fields (`endpoint`, `custom_headers`, `token_info`, `token_refresh`) have
been consolidated into `pub(super) struct AuthOptions` in `auth_group_builder.rs`.
`AuthOptions` is passed directly to `create_translator_config` in `common.rs`.

### `create_translator_config` signature change

```rust
// Before
pub(super) async fn create_translator_config(
    session: &XetSession,
    token_info: Option<(String, u64)>,
    token_refresh: Option<&(String, Arc<HeaderMap>)>,
) -> Result<TranslatorConfig, XetError>

// After
pub(super) async fn create_translator_config(
    session: &XetSession,
    auth_options: AuthOptions,
) -> Result<TranslatorConfig, XetError>
```

---

## Affected Files

- `xet_pkg/src/xet_session/auth_group_builder.rs` — new `AuthOptions` struct; `AuthGroupBuilder<G>` replaces the three separate builder structs
- `xet_pkg/src/xet_session/common.rs` — `create_translator_config` now takes `AuthOptions`
- `xet_pkg/src/xet_session/session.rs` — factory method return types updated
- `xet_pkg/src/xet_session/upload_commit.rs` — builder impl moved to `impl AuthGroupBuilder<XetUploadCommit>`
- `xet_pkg/src/xet_session/file_download_group.rs` — builder impl moved to `impl AuthGroupBuilder<XetFileDownloadGroup>`; `with_endpoint`/`with_custom_headers` bug fixed (fields were declared on the struct but missing from the struct literal)
- `xet_pkg/src/xet_session/download_stream_group.rs` — builder impl moved to `impl AuthGroupBuilder<XetDownloadStreamGroup>`; gains `with_endpoint` and `with_custom_headers`
