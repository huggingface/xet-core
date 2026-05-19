# hf_xet_wasm_upload — Xet uploads from the browser

`cdylib` crate that wraps `xet::xet_session::XetSession` with `#[wasm_bindgen]`
and exposes an upload-only API to JavaScript.

> [!IMPORTANT]
> **This crate is an example / smoke-test wrapper, not a published browser
> SDK.** It exists so the wasm build of the upload data-prep path is
> exercised end-to-end in CI (the regression guard for the
> `XetRuntime::spawn_blocking` panic on wasm) and so we have a hand-runnable
> browser page for manual testing. The JS surface here is not versioned,
> not on npm, and may change without notice. Real browser consumers should
> depend on `hf-xet` (with the `wasm32-unknown-unknown` target) directly
> via their own `#[wasm_bindgen]` wrapper, or use a downstream SDK such as
> `huggingface.js`.
>
> The companion crate [`hf_xet_wasm_download`](../hf_xet_wasm_download) is
> the download-side counterpart with the same positioning.

## JS API

The JS surface mirrors the Rust builder pattern in `xet::xet_session`. A
`XetSession` is auth-free; auth lives on the per-commit builder, so one
session can hand out many independent commits, each with its own endpoint
/ token pair.

```typescript
type Sha256Policy =
  | "compute"               // hash the bytes during ingestion
  | "skip"                  // omit sha256 metadata
  | { provided: string };   // 64-char lowercase hex

class XetSession {
  // Auth-free session. Mirrors `XetSessionBuilder::new().build()`.
  constructor();

  // Begin a new upload commit. Resolves to an `XetUploadCommit` to which
  // you can `uploadBytes(...)` / `uploadStream(...)` and finally `commit()`.
  //
  //   endpoint:    CAS server URL — typically the `casUrl` field of the
  //                Hugging Face Hub `xet-write-token` response.
  //   token:       CAS access token (the `accessToken` field of the same
  //                response).
  //   tokenExpiry: Unix timestamp in seconds (the `exp` field). The
  //                wrapper does not wire a token refresher, so any value
  //                at or before "now" causes an auth error on the first
  //                request. The sentinel `0` is mapped to "no expiry"
  //                (u64::MAX) for placeholder / local-only flows (e.g.
  //                the CI upload smoke); in normal use pass the real
  //                `exp` from the Hub.
  newUploadCommit(
    endpoint: string,
    token: string,
    tokenExpiry: number,
  ): Promise<XetUploadCommit>;
}

class XetUploadCommit {
  // Upload a complete byte buffer. Returns per-file metadata (the
  // `XetFileMetadata` shape from `xet_session`, serialized to a JS
  // object). The bytes are chunked, deduped, and serialized into xorbs
  // locally; xorb upload happens during `commit()`.
  uploadBytes(
    bytes: Uint8Array,
    sha256Policy?: Sha256Policy,
    trackingName?: string,
  ): Promise<object>;

  // Begin an incremental streaming upload. Call `write(chunk)` repeatedly,
  // then `finish()` to finalize ingestion. `finish` must be called before
  // `commit()`.
  uploadStream(
    trackingName?: string,
    sha256Policy?: Sha256Policy,
  ): Promise<XetStreamUpload>;

  // Push xorbs + shard to CAS and finalize. Returns a commit report with
  // `dedup_metrics` and `uploads` (keyed by stringified task id). The
  // internal progress snapshot is omitted because `GroupProgressReport`
  // is not serde-serializable.
  commit(): Promise<object>;

  // Cancel all active uploads in this commit.
  abort(): void;
}

class XetStreamUpload {
  write(chunk: Uint8Array): Promise<void>;
  finish(): Promise<object>;  // returns per-file metadata
  abort(): void;
}
```

The full token / file-id derivation is described in the
[Xet Protocol Specification](https://huggingface.co/docs/xet/index).

## Wasm-only caveats

- **No global deduplication.** The wasm `SessionShardInterface` is an
  in-memory `MDBInMemoryShard` (no disk staging, no resume). Global
  dedup queries against the CAS server are stubbed out because there is
  no cache shard manager to import the result into. Repeated uploads of
  similar files from the browser will push more bytes than the native
  client would.
- **Local-only `uploadBytes` data-prep.** Chunking + sha256 + xorb
  serialization happens locally before any CAS round-trip. The CI upload
  smoke exercises this path with a placeholder endpoint + token — see
  `wasm/ci-smoke/run-upload.mjs`.
- **No `_blocking` variants, no `upload_from_path`.** Wasm cannot block
  the host thread and has no filesystem; the JS surface omits both.
- **No automatic token refresh.** This wrapper does not expose
  `XetUploadCommitBuilder::with_token_refresh_url`; if the supplied
  `tokenExpiry` is reached mid-upload the underlying request will fail
  with an auth error. Callers must fetch a fresh `xet-write-token` from
  the Hub and build a new commit before expiry.
- **One bulk progress event per xorb.** reqwest's wasm backend does not
  support streaming request bodies, so the wasm xorb upload sends the
  raw `Bytes` and fires a single `report_progress(n_transfer_bytes)`
  event on success rather than the chunked progress stream native
  produces.

## Build

```bash
./build_wasm.sh
```

Outputs `pkg/{hf_xet_wasm_upload.js, hf_xet_wasm_upload.d.ts,
hf_xet_wasm_upload_bg.wasm}`.

Requires the same nightly toolchain + `wasm-bindgen-cli` 0.2.121 as the
other `wasm/*` crates in this repo.

## Manual browser test

```bash
./build_wasm.sh
# Serve with COOP/COEP headers (SharedArrayBuffer is required by the
# threaded wasm build). The CI smoke server at `wasm/ci-smoke/server.mjs`
# sets the necessary headers; any equivalent static server works.
```

Open `examples/upload.html`, fill in a HF Hub write token, repo path,
and a local file. The page calls:

1. `GET /api/{repo_type}s/{namespace}/{repo}/xet-write-token/{rev}` &mdash;
   returns `{ accessToken, exp, casUrl }`

then constructs `new XetSession()`, opens a commit via
`session.newUploadCommit(casUrl, accessToken, exp)`, runs
`commit.uploadBytes(...)`, and finalizes with `commit.commit()`.

> [!WARNING]
> The manual upload page pushes xorbs + shard to CAS but does **not**
> commit them to a Hub repo. To actually land the data in a repo you
> would have to take the returned metadata and call the Hub commit API
> yourself.

## Maintainer note

The upload path goes through `xet_pkg`, `xet_client`, `xet_data`,
`xet_core_structures`, and `xet_runtime`. Changes to any of those crates
must keep the wasm build green &mdash; see the
["WebAssembly compatibility" section in the root README](../../README.md)
for the patterns this codebase relies on (`web_time::Instant`,
`tokio_with_wasm::alias`, conditional `?Send` async-traits, filesystem
gating). CI checks `cargo +nightly check --target wasm32-unknown-unknown -p hf-xet`
and runs `./build_wasm.sh` on every push.
