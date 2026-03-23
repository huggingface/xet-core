# API Update: Token Refresh URL replaces TokenRefresher in XetSession (2026-03-24)

## Overview

`XetSessionBuilder::with_token_refresher(Arc<dyn TokenRefresher>)` has been removed.
Callers now pass a plain URL string via `with_token_refresh_url(url)`. The session
makes an HTTP GET to that URL whenever the access token is about to expire.

The low-level `TokenRefresher` trait in `xet_client::cas_client::auth` is retained for
use by legacy API functions (`upload_bytes`, `upload_files`, `download_files` in
`hf_xet`) and by `git_xet`.

---

## Breaking Changes

### Removed method on `XetSessionBuilder`

- `XetSessionBuilder::with_token_refresher(refresher: Arc<dyn TokenRefresher>) -> Self`

### New method on `XetSessionBuilder`

- `XetSessionBuilder::with_token_refresh_url(url: impl Into<String>, headers: Arc<HeaderMap>) -> Self`

  `headers` are sent exclusively with token-refresh requests. They are independent of
  the CAS headers supplied via `with_custom_headers`.

### Removed field on `XetSessionInner`

- `token_refresher: Option<Arc<dyn TokenRefresher>>`

### New field on `XetSessionInner`

- `token_refresh: Option<(String, Arc<HeaderMap>)>`

  The URL and its refresh-specific headers are stored as a single `Option` tuple,
  enforcing at the type level that they are always set together.

---

## Migration Guide

### `XetSessionBuilder` callers

```rust
// Old
let refresher: Arc<dyn TokenRefresher> = Arc::new(MyRefresher::new(...));
let session = XetSessionBuilder::new()
    .with_endpoint("https://cas.example.com")
    .with_token_refresher(refresher)
    .build()?;

// New — headers are sent only with token-refresh requests, not CAS requests
let mut refresh_headers = HeaderMap::new();
refresh_headers.insert("Authorization", "Bearer hub-token".parse().unwrap());
let session = XetSessionBuilder::new()
    .with_endpoint("https://cas.example.com")
    .with_token_refresh_url(
        "https://huggingface.co/api/repos/token",
        Arc::new(refresh_headers),
    )
    .build()?;
```

An initial token can optionally be seeded to skip the first refresh round-trip:

```rust
let session = XetSessionBuilder::new()
    .with_endpoint("https://cas.example.com")
    .with_token_refresh_url(refresh_url, Arc::new(refresh_headers))
    .with_token_info(current_token, expiry_timestamp)
    .build()?;
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

## Affected Areas

- `xet_client::cas_client::auth` — added `DirectRefreshRouteTokenRefresher`
- `xet_client::common::http_client` — **new module**; contains HTTP client construction code previously in `xet_client::cas_client::http_client`
- `xet_runtime::error::RuntimeError` — added `ReqwestError` and `PoisonError` variants
- `xet_runtime::core::XetRuntime::get_or_create_reqwest_client` — return type changed to `xet_runtime::Result<Client>`
- `xet_client::error::ClientError` — added `From<xet_runtime::error::RuntimeError>`
- `xet_pkg::xet_session::{session, common}` — `with_token_refresh_url` replaces `with_token_refresher`
- `git_xet::token_refresher` — now a thin factory delegating to `xet_client`
- `git_xet::app::xet_agent` — updated to call `new_git_token_refresher`
- Legacy `hf_xet` Python functions (`upload_bytes`, `upload_files`, `download_files`) are **unchanged**
