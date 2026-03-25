# API Update: Per-commit/group token auth replaces session-level auth (2026-03-24)

## Overview

`XetSessionBuilder::with_token_refresher(Arc<dyn TokenRefresher>)` has been removed.
Auth tokens are now configured per-[`UploadCommit`], per-[`FileDownloadGroup`], and
per-[`DownloadStreamGroup`] via builder methods, so uploads, file downloads, and
streaming downloads can each carry a different access-level token from the same session.

The low-level `TokenRefresher` trait in `xet_client::cas_client::auth` is retained for
use by legacy API functions (`upload_bytes`, `upload_files`, `download_files` in
`hf_xet`) and by `git_xet`.

---

## Breaking Changes

### Removed from `XetSessionBuilder`

- `XetSessionBuilder::with_token_refresher(refresher: Arc<dyn TokenRefresher>) -> Self`
- `XetSessionBuilder::with_token_info(token: impl Into<String>, expiry: u64) -> Self`
- `XetSessionBuilder::with_token_refresh_url(url: impl Into<String>, headers: HeaderMap) -> Self`

### Removed fields from `XetSessionInner`

- `token_info: Option<(String, u64)>`
- `token_refresher: Option<Arc<dyn TokenRefresher>>`

### Changed factory methods on `XetSession`

`new_upload_commit` and `new_file_download_group` are now **synchronous** and return
builder types instead of directly constructing the commit/group:

```
// Before
pub async fn new_upload_commit(&self) -> Result<UploadCommit, SessionError>
pub fn new_upload_commit_blocking(&self) -> Result<UploadCommit, SessionError>
pub async fn new_file_download_group(&self) -> Result<FileDownloadGroup, SessionError>
pub fn new_file_download_group_blocking(&self) -> Result<FileDownloadGroup, SessionError>

// After
pub fn new_upload_commit(&self) -> Result<UploadCommitBuilder, SessionError>
pub fn new_file_download_group(&self) -> Result<FileDownloadGroupBuilder, SessionError>
```

### New builder types

**`UploadCommitBuilder`** and **`FileDownloadGroupBuilder`** — both have:

```rust
pub fn with_token_info(self, token: impl Into<String>, expiry: u64) -> Self
pub fn with_token_refresh_url(self, url: impl Into<String>, headers: HeaderMap) -> Self
pub async fn build(self) -> Result<UploadCommit / FileDownloadGroup, SessionError>
pub fn build_blocking(self) -> Result<UploadCommit / FileDownloadGroup, SessionError>
```

---

## Migration Guide

```rust
// Old — token on the session
let session = XetSessionBuilder::new()
    .with_endpoint("https://cas.example.com")
    .with_token_refresher(refresher)
    .build()?;
let commit = session.new_upload_commit_blocking()?;
let group  = session.new_file_download_group_blocking()?;

// New — token on each commit/group (can differ per operation)
let session = XetSessionBuilder::new()
    .with_endpoint("https://cas.example.com")
    .build()?;

let mut write_headers = HeaderMap::new();
write_headers.insert("Authorization", "Bearer write-token".parse().unwrap());
let commit = session.new_upload_commit()?
    .with_token_refresh_url("https://huggingface.co/api/repos/token/write", write_headers)
    .build_blocking()?;

let mut read_headers = HeaderMap::new();
read_headers.insert("Authorization", "Bearer read-token".parse().unwrap());
let group = session.new_file_download_group()?
    .with_token_refresh_url("https://huggingface.co/api/repos/token/read", read_headers)
    .build_blocking()?;
```

An initial token can optionally be seeded to skip the first refresh round-trip:

```rust
let commit = session.new_upload_commit()?
    .with_token_refresh_url(refresh_url, refresh_headers)
    .with_token_info(current_token, expiry_timestamp)
    .build_blocking()?;
```

### Token refresh URL contract

The URL must accept an unauthenticated HTTP GET and return JSON with the shape:

```json
{ "accessToken": "<string>", "exp": <unix_timestamp_seconds>, "casUrl": "<string>" }
```

---

## Internal Changes

### New type: `DirectRefreshRouteTokenRefresher` (moved to `xet_client`)

`DirectRefreshRouteTokenRefresher` was previously defined in `git_xet::token_refresher`.
It has been moved to `xet_client::cas_client::auth` and its constructor refactored:

```rust
// New constructor (in xet_client::cas_client::auth)
pub fn new(
    refresh_route: impl Into<String>,
    client: ClientWithMiddleware,
    cred_helper: Option<Arc<dyn CredentialHelper>>,
) -> Self
```

- Pass `None` for `cred_helper` when no additional credential decoration is needed
  (e.g. the XetSession path).
- Pass a `CredentialHelper` implementation when git credentials must be attached
  (e.g. the git_xet path).

### `git_xet::token_refresher`

`DirectRefreshRouteTokenRefresher` is no longer defined here. A factory function
`new_git_token_refresher(...)` replaces the old constructor and handles git credential
resolution internally:

```rust
// git_xet::token_refresher
pub fn new_git_token_refresher(
    repo: &GitRepo,
    remote_url: Option<GitUrl>,
    refresh_route: &str,
    operation: Operation,
    session_id: &str,
    custom_headers: Option<Arc<HeaderMap>>,
) -> Result<DirectRefreshRouteTokenRefresher>
```

### `xet_runtime::RuntimeError` — new variants

Two new variants were added to handle errors from the HTTP client cache:

```rust
#[error("Reqwest error: {0}")]
ReqwestError(#[from] reqwest::Error),

#[error("Mutex poison error: {0}")]
PoisonError(String),
```

`XetRuntime::get_or_create_reqwest_client` now returns `xet_runtime::Result<reqwest::Client>`
instead of `std::result::Result<reqwest::Client, reqwest::Error>`.

### `xet_pkg::xet_session::common::create_translator_config` — merged helper

The internal `build_token_refresher` helper function has been merged into
`create_translator_config`.  The function now accepts `token_refresh:
Option<&(String, Arc<HeaderMap>)>` directly and builds the `TokenRefresher`
internally, eliminating the need for callers to construct it separately.

### `xet_pkg::xet_session::download_stream_group` — stream types merged in

`XetDownloadStream` and `XetUnorderedDownloadStream` were previously defined in a
separate `xet_pkg::xet_session::download_streams` module.  That module has been
deleted and both types are now defined directly in `download_stream_group.rs`
alongside `DownloadStreamGroup` and `DownloadStreamGroupBuilder`.

The public API of both types is unchanged.  The only visible change is that their
Rust module path is now `xet_pkg::xet_session::download_stream_group::Xet*Stream`
(though they continue to be re-exported at `xet_pkg::xet_session::Xet*Stream`).

### `xet_client::common::http_client` — new shared module

HTTP client construction code has been moved from `xet_client::cas_client::http_client`
to a new shared module `xet_client::common::http_client` so that both `cas_client` and
`hub_client` can use it directly.

- The old path `xet_client::cas_client::http_client` no longer exists.
- `xet_client::cas_client` re-exports `Api`, `build_http_client`, `build_auth_http_client`,
  and `ResponseErrorLogger` from `xet_client::common::http_client` for backwards
  compatibility of the public API.
- All internal callers have been updated to import from `xet_client::common::http_client`.

The client cache tag now incorporates both the transport (`"tcp"` or unix socket path)
**and** a serialized representation of the `custom_headers`, so that clients with
different header sets get separate connection pools. Header key-value pairs are sorted
for a stable tag independent of insertion order.

---

## New types: `DownloadStreamGroup` / `DownloadStreamGroupBuilder`

Streaming downloads are now surfaced through a new `DownloadStreamGroup` type rather
than directly on `XetSession`.

### `XetSession::new_download_stream_group`

```rust
pub fn new_download_stream_group(&self) -> Result<DownloadStreamGroupBuilder, SessionError>
```

Returns a builder that can be configured with per-group auth before constructing the
group.  Returns `Err(SessionError::Aborted)` if the session has been aborted.

### `DownloadStreamGroupBuilder`

```rust
pub fn with_token_info(self, token: impl Into<String>, expiry: u64) -> Self
pub fn with_token_refresh_url(self, url: impl Into<String>, headers: HeaderMap) -> Self
pub async fn build(self) -> Result<DownloadStreamGroup, SessionError>
pub fn build_blocking(self) -> Result<DownloadStreamGroup, SessionError>
```

### `DownloadStreamGroup`

```rust
pub async fn download_stream(&self, file_info: XetFileInfo, range: Option<Range<u64>>) -> Result<XetDownloadStream, SessionError>
pub fn download_stream_blocking(&self, file_info: XetFileInfo, range: Option<Range<u64>>) -> Result<XetDownloadStream, SessionError>
pub async fn download_unordered_stream(&self, file_info: XetFileInfo, range: Option<Range<u64>>) -> Result<XetUnorderedDownloadStream, SessionError>
pub fn download_unordered_stream_blocking(&self, file_info: XetFileInfo, range: Option<Range<u64>>) -> Result<XetUnorderedDownloadStream, SessionError>
```

Multiple streams can be active concurrently from the same group; they share a single
CAS connection pool and auth token.

### Removed from `XetSession`

- `download_stream`
- `download_stream_blocking`
- `download_unordered_stream`
- `download_unordered_stream_blocking`
- `get_or_init_streaming_session` (internal)

The lazily-initialised `streaming_download_session` field on `XetSessionInner` has been
removed and replaced with `active_download_stream_groups`.

---

## Affected Areas

- `xet_client::cas_client::auth` — added `DirectRefreshRouteTokenRefresher`
- `xet_client::common::http_client` — **new module**; contains HTTP client construction code previously in `xet_client::cas_client::http_client`
- `xet_runtime::error::RuntimeError` — added `ReqwestError` and `PoisonError` variants
- `xet_runtime::core::XetRuntime::get_or_create_reqwest_client` — return type changed to `xet_runtime::Result<Client>`
- `xet_client::error::ClientError` — added `From<xet_runtime::error::RuntimeError>`
- `xet_pkg::xet_session::{session, common, upload_commit, file_download_group, download_stream_group}` — auth moved from session to per-commit/group builders; streaming downloads moved to `DownloadStreamGroup`; `XetDownloadStream` and `XetUnorderedDownloadStream` merged from the now-deleted `download_streams` module into `download_stream_group`
- `git_xet::token_refresher` — now a thin factory delegating to `xet_client`
- `git_xet::app::xet_agent` — updated to call `new_git_token_refresher`
- Legacy `hf_xet` Python functions (`upload_bytes`, `upload_files`, `download_files`) are **unchanged**
