# WASM `newUploadCommit` / `newDownloadStreamGroup` accept a token refresh URL

**Date**: 2026-07-28
**Crate**: `hf_xet_wasm` (`wasm/hf_xet_wasm`, JS surface only)

## What changed

Both `XetSession` factory methods gained two trailing optional arguments and
their first three arguments became optional:

```typescript
// Before
newUploadCommit(endpoint: string, token: string, tokenExpiry: number): Promise<XetUploadCommit>;
newDownloadStreamGroup(endpoint: string, token: string, tokenExpiry: number): Promise<XetDownloadStreamGroup>;

// After
newUploadCommit(
  endpoint?: string | null,
  token?: string | null,
  tokenExpiry?: number | null,
  tokenRefreshUrl?: string | null,
  tokenRefreshHeaders?: Record<string, string> | null,
): Promise<XetUploadCommit>;
// newDownloadStreamGroup takes the same argument list.
```

`tokenRefreshUrl` + `tokenRefreshHeaders` map onto the existing native
`AuthGroupBuilder::with_token_refresh_url(url, headers)`; the headers object is
converted to an `http::HeaderMap`. No Rust API changed — this is JS-surface
only, and `hf_xet_wasm` gained no new Cargo dependencies (`HeaderMap` /
`HeaderValue` / `HeaderName` come from the existing `xet::xet_session`
re-exports).

Internally `common.rs::validate_session_inputs` was replaced by
`resolve_auth_inputs` (returning an `AuthInputs` struct) plus
`parse_header_map`.

## Why

The wasm wrapper previously wired no token refresher at all, so a browser
upload or download failed with an auth error once `tokenExpiry` passed, and
callers had to build a new commit / group. It now refreshes in place.

The URL-based refresher was chosen over a JS-callback refresher because
`TokenRefresher` requires `Send + Sync`, which a `js_sys::Function` cannot
satisfy without unsafe wrapper types that have no precedent in this repo. The
refresh GET goes through reqwest's wasm backend, so the route must allow the
page's origin via CORS; the Hub `xet-read-token` / `xet-write-token` routes do.

## Migration

Existing three-argument calls are unchanged and keep working:

```js
const group = await session.newDownloadStreamGroup(casUrl, accessToken, exp);
```

To refresh in place, add the Hub token route and the Hub credentials:

```js
const group = await session.newDownloadStreamGroup(
  casUrl, accessToken, exp,
  `https://huggingface.co/api/models/${ns}/${repo}/xet-read-token/${rev}`,
  { Authorization: `Bearer ${hfToken}` },
);
```

Because the refresh route also returns `casUrl`, the endpoint and initial token
can be omitted entirely — the wrapper calls the route once during `build()` to
resolve whichever of the three were not supplied:

```js
const group = await session.newDownloadStreamGroup(
  null, null, null,
  `https://huggingface.co/api/models/${ns}/${repo}/xet-read-token/${rev}`,
  { Authorization: `Bearer ${hfToken}` },
);
```

## Validation rules

`resolve_auth_inputs` rejects, before any network call:

- `token` without `tokenExpiry`, or `tokenExpiry` without `token`
  ("must be provided together"). `tokenExpiry` must still be finite and positive.
- no `endpoint` and no `tokenRefreshUrl`.
- no `token`/`tokenExpiry` pair and no `tokenRefreshUrl`.
- a non-http(s) `endpoint` or `tokenRefreshUrl`.
- `tokenRefreshHeaders` without `tokenRefreshUrl`.
- `tokenRefreshHeaders` that is not an object, has a non-string value, or has a
  name/value `http` rejects.

`undefined`, `null`, `""` and whitespace-only strings are all treated as "not
provided" for `endpoint`, `token` and `tokenRefreshUrl`, so JS callers can pass
a positional placeholder. Note this is a behavior change for `endpoint: ""` and
`token: ""`: previously each produced its own error, now they are absent values
and the error names what is missing instead.

## Tests

- `wasm/ci-smoke/scenarios/token-refresh.mjs` (new) — builds a download stream
  group from a `tokenRefreshUrl` alone against the pinned `READ_REPO` file and
  asserts the pinned size + SHA-256, proving the eager `casUrl` + token bootstrap
  and the CORS-visible refresh GET.
- `wasm/ci-smoke/scenarios/invalid-inputs.mjs` — extended from 9 to 18 cases,
  each run against both factory methods.
