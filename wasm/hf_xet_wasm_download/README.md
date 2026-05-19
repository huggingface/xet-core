# hf_xet_wasm_download — streaming Xet downloads from the browser

`cdylib` crate that wraps `xet::xet_session::XetSession` with `#[wasm_bindgen]`
and exposes a download-only API to JavaScript.

> [!IMPORTANT]
> **This crate is an example / smoke-test wrapper, not a published browser
> SDK.** It exists so the wasm build of `xet_pkg` is exercised end-to-end in
> CI and so we have a hand-runnable browser page for manual testing. The
> JS surface here is not versioned, not on npm, and may change without
> notice. Real browser consumers should depend on `hf-xet` (with the
> `wasm32-unknown-unknown` target) directly via their own `#[wasm_bindgen]`
> wrapper, or use a downstream SDK such as `huggingface.js`.
>
> The companion crate [`hf_xet_wasm_upload`](../hf_xet_wasm_upload) is the
> upload-side counterpart with the same positioning.

## JS API

The JS surface mirrors the Rust builder pattern in `xet::xet_session`. A
`XetSession` is auth-free; auth lives on the per-group builder, so one
session can hand out many groups, each with its own endpoint / token pair.

```typescript
class XetSession {
  // Auth-free session. Mirrors `XetSessionBuilder::new().build()`.
  constructor();

  // Build an authenticated download stream group.
  //   endpoint:    CAS server URL — typically the `casUrl` field of the
  //                Hugging Face Hub `xet-read-token` response
  //   token:       CAS access token (the `accessToken` field of the same
  //                response)
  //   tokenExpiry: Unix timestamp in seconds (the `exp` field). The
  //                wrapper does not wire a token refresher, so any value
  //                at or before "now" causes an auth error on the first
  //                request. The sentinel `0` is mapped to "no expiry"
  //                (u64::MAX) for placeholder / local-only flows; in
  //                normal use pass the real `exp` from the Hub.
  newDownloadStreamGroup(
    endpoint: string,
    token: string,
    tokenExpiry: number,
  ): Promise<XetDownloadStreamGroup>;
}

class XetDownloadStreamGroup {
  // fileInfo must be a plain JS object of the shape
  //   { hash: string, file_size: number }
  // where `hash` is the xet file ID — obtained from either the
  // `X-Xet-Hash` response header on the resolve URL or the `xetHash`
  // field of the `POST /api/{repo_type}s/{repo}/paths-info/{rev}` response.
  //
  // When both `byteRangeStart` and `byteRangeEnd` are provided, only that
  // half-open byte range is downloaded.
  downloadStream(
    fileInfo: { hash: string; file_size: number },
    byteRangeStart?: number,
    byteRangeEnd?: number,
  ): Promise<XetDownloadStream>;
}

class XetDownloadStream {
  // Resolves to the next chunk, or `undefined` at end-of-stream.
  // Borrows the stream mutably for the lifetime of the Promise — do not
  // call `next()` or `cancel()` again until this Promise has resolved, or
  // wasm-bindgen will throw "recursive use of an object detected".
  next(): Promise<Uint8Array | undefined>;

  // Cancels the in-progress download. Must not be called while a `next()`
  // Promise is still pending.
  cancel(): void;
}
```

The full token / file-id derivation is described in the
[Xet Protocol Specification](https://huggingface.co/docs/xet/index)
(see `auth.md` and `file-id.md`).

## Token refresh

This wrapper does **not** expose
`XetDownloadStreamGroupBuilder::with_token_refresh_url`. The CAS token
passed to `newDownloadStreamGroup` is used as-is and is **not** refreshed
mid-stream. If `tokenExpiry` is reached during a download the underlying
request will fail with an auth error; callers must fetch a new
`xet-read-token` from the Hub and construct a fresh group before expiry.

Wiring an automatic refresh through `wasm_bindgen` would need either a
JS-callback bridge (so JS can mint and return a token via `Promise`) or
a URL-based refresher backed by a route the wasm `reqwest` client can hit
directly; both are out of scope for this example wrapper.

## Build

```bash
./build_wasm.sh
```

Outputs `pkg/{hf_xet_wasm_download.js, hf_xet_wasm_download.d.ts,
hf_xet_wasm_download_bg.wasm}`.

Requires the same nightly toolchain + `wasm-bindgen-cli` 0.2.121 as the
other `wasm/*` crates in this repo.

## Manual browser test

```bash
./build_wasm.sh
# Serve with COOP/COEP headers (SharedArrayBuffer is required by the
# threaded wasm build). The CI smoke server at `wasm/ci-smoke/server.mjs`
# sets the necessary headers; any equivalent static server works.
```

Open `examples/download.html`, fill in a HF Hub token, repo path, and
file path, click Download. The page calls:

1. `POST /api/{repo_type}s/{namespace}/{repo}/paths-info/{rev}` &mdash;
   returns `{ xetHash, size, ... }` for the file
2. `GET  /api/{repo_type}s/{namespace}/{repo}/xet-read-token/{rev}` &mdash;
   returns `{ accessToken, exp, casUrl }`

then constructs `new XetSession()`, builds a group via
`session.newDownloadStreamGroup(casUrl, accessToken, exp)`, and streams
the file via `group.downloadStream({ hash: xetHash, file_size: size })`.

## Maintainer note

The download path goes through `xet_pkg`, `xet_client`, `xet_data`,
`xet_core_structures`, and `xet_runtime`. Changes to any of those crates
must keep the wasm build green &mdash; see the
["WebAssembly compatibility" section in the root README](../../README.md)
for the patterns this codebase relies on (`web_time::Instant`,
`tokio_with_wasm::alias`, conditional `?Send` async-traits, filesystem
gating). CI checks `cargo +nightly check --target wasm32-unknown-unknown -p hf-xet`
and runs `./build_wasm.sh` on every push.
