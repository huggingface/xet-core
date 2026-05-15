# hf_xet_wasm_download: streaming Xet downloads from the browser

`cdylib + rlib` crate that wraps `xet_pkg::XetSession` with `#[wasm_bindgen]` and
exposes a download-only API to JavaScript.

This complements [`hf_xet_wasm_upload`](../hf_xet_wasm_upload), which is upload-only.

## JS API

```typescript
class XetSession {
  // endpoint: CAS server URL (e.g. value of the `casUrl` field in the
  //   Hugging Face Hub xet-read-token response)
  // token: CAS access token (the `accessToken` field of the same response)
  // tokenExpiry: unix timestamp in seconds (the `exp` field); 0 means "never"
  constructor(endpoint: string, token: string, tokenExpiry: number);

  // fileInfo must be a plain JS object of the shape
  //   { hash: string, file_size: number }
  // where `hash` is the xet file ID — obtained from either the `X-Xet-Hash`
  // response header on the resolve URL or the `xetHash` field of the
  // `POST /api/{repo_type}s/{repo}/paths-info/{rev}` response.
  downloadStream(fileInfo: object, byteRangeStart?: number, byteRangeEnd?: number): Promise<XetDownloadStream>;
}

class XetDownloadStream {
  next(): Promise<Uint8Array | undefined>;  // undefined signals EOF
  cancel(): void;
}
```

The full token / file-id derivation is described in the
[Xet Protocol Specification](https://huggingface.co/docs/xet/index)
(see `auth.md` and `file-id.md`).

## Build

```bash
./build_wasm.sh
```

Outputs `pkg/{hf_xet_wasm_download.js, hf_xet_wasm_download.d.ts,
hf_xet_wasm_download_bg.wasm}`.

Requires the same nightly toolchain + `wasm-bindgen-cli` 0.2.121 as the
other `wasm/*` crates in this repo.

## Token refresh

This wrapper does **not** expose `XetDownloadStreamGroupBuilder::with_token_refresh_url`.
The CAS token passed to `new XetSession(...)` is used as-is and is **not** refreshed
mid-stream. If `tokenExpiry` is reached during a download the underlying request will
fail with an auth error; callers must fetch a new `xet-read-token` from the Hub and
construct a fresh `XetSession` before expiry.

Wiring an automatic refresh through `wasm_bindgen` would need either a JS-callback
bridge (so JS can mint and return a token via `Promise`) or a URL-based refresher
backed by a route the wasm `reqwest` client can hit directly; both are out of scope
for the initial wrapper.

## Manual browser test

```bash
./build_wasm.sh
# Serve with COOP/COEP headers (SharedArrayBuffer is required by the
# threaded wasm). `sfz --coi` works, or use any server that sets:
#   Cross-Origin-Opener-Policy: same-origin
#   Cross-Origin-Embedder-Policy: require-corp
```

Open `examples/download.html`, fill in a HF Hub token, repo path, and
file path, click Download. The page calls:

1. `POST /api/{repo_type}s/{namespace}/{repo}/paths-info/{rev}` &mdash;
   returns `{ xetHash, size, ... }` for the file
2. `GET  /api/{repo_type}s/{namespace}/{repo}/xet-read-token/{rev}` &mdash;
   returns `{ accessToken, exp, casUrl }`

then constructs `new XetSession(casUrl, accessToken, exp)` and streams the
file via `session.downloadStream({ hash: xetHash, file_size: size })`.

## Maintainer note

The download path goes through `xet_pkg`, `xet_client`, `xet_data`,
`xet_core_structures`, and `xet_runtime`. Changes to any of those crates
must keep the wasm build green &mdash; see the
["WebAssembly compatibility" section in the root README](../../README.md)
for the patterns this codebase relies on (`web_time::Instant`,
`tokio_with_wasm::alias`, conditional `?Send` async-traits, filesystem
gating). CI checks `cargo +nightly check --target wasm32-unknown-unknown -p hf-xet`
and runs `./build_wasm.sh` on every push.
