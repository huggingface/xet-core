# xet_pkg_napi — napi smoke test for hf-xet

A minimal [napi-rs](https://napi.rs) native addon that links against the
`xet_pkg` (`hf-xet`) crate. Verifies that `hf-xet` compiles, links, starts up,
and can actually pull a file from CAS — all from inside a Node.js native
module.

## What it exports

The Rust crate at `src/lib.rs` exposes three functions to Node:

- `initLogging(version: string)` — installs `xet`'s tracing subscriber.
- `smokeTest(): string` — builds a `XetSession` synchronously and constructs
  upload-commit + file-download-group builders. No I/O.
- `downloadFile(opts): { destPath, bytesDownloaded }` — actually downloads a
  Xet-stored file from the HuggingFace Hub. **Synchronous**: blocks the libuv
  main thread until the download finishes, so the JS event loop is paused for
  the duration. Acceptable for a smoke test; a real binding should wrap this
  in `napi::Task` / `tokio::task::spawn_blocking`.

This crate is **excluded from the workspace** (see the root `Cargo.toml`)
and carries its own `[workspace]` table because it has its own
`crate-type = ["cdylib"]` and ships under the `napi-rs/cli` build flow rather
than `cargo build`.

## Build & run

Requires Node ≥ 18, a Rust toolchain, and outbound network access to
`huggingface.co` and `cas-bridge.xethub.hf.co`.

```sh
cd examples/xet_pkg_napi
npm install
npm run build:debug   # or `npm run build` for release
npm run smoke
```

`napi build` writes two artifacts next to `package.json`:

- `xet-pkg-napi.<platform>-<arch>.node` — the compiled cdylib
- `index.js` / `index.d.ts` — a CJS shim that picks the right `.node` for the
  current platform

`smoke.mjs`:

1. Issues a `HEAD` against the HF Hub `resolve` URL with a non-default
   User-Agent (Cloudfront strips `X-Xet-Hash` on cache hits served to
   default UAs).
2. Reads `X-Xet-Hash`, `X-Linked-Size`, and `X-Linked-Etag` from the response.
3. Calls `downloadFile()` with the parsed metadata.
4. Verifies the on-disk size matches `X-Linked-Size`.

### Configuration

All env vars are optional. Defaults target a tiny (~540 KB) public Xet file
so the smoke test runs quickly without an HF token.

| Var            | Default                                            |
| -------------- | -------------------------------------------------- |
| `HF_ENDPOINT`  | `https://huggingface.co`                           |
| `HF_REPO_TYPE` | `model` (`model` \| `dataset` \| `space`)          |
| `HF_REPO`      | `hf-internal-testing/tiny-random-bert`             |
| `HF_BRANCH`    | `main`                                             |
| `HF_FILENAME`  | `pytorch_model.bin`                                |
| `HF_TOKEN`     | _unset_ (required for private repos)               |
| `HF_DEST_DIR`  | `./downloads`                                      |

## Expected output

```
loaded addon, exports: [ 'initLogging', 'smokeTest', 'downloadFile' ]

Fetching xet metadata for model:hf-internal-testing/tiny-random-bert/pytorch_model.bin@main
  https://huggingface.co/hf-internal-testing/tiny-random-bert/resolve/main/pytorch_model.bin
  xet-hash:    75402e74462600f62ca4a08b91c9218f36075860d5f6d7eb07f4c29ed7fa4ad6
  size:        540,217 bytes
  sha256:      9922e8996d0c7e24c7f4e7a5d9c5b7303549f4ee94de0f1138b103014b51be13
smokeTest: xet session built; runtime initialized

Downloading -> downloads/pytorch_model.bin

Result:
  bytes downloaded: 540,217
  on-disk size:     540,217
  elapsed:          1.23s

OK — file downloaded and size matches.
```

## Notes / caveats

- **Synchronous download.** A real binding should expose this as
  `#[napi]` async fn or wrap in `napi::Task` so the JS event loop isn't blocked
  while xet pulls bytes from CAS.
- **No double runtime.** `xet-runtime` owns its own tokio runtime; it doesn't
  piggyback on libuv. The blocking calls used here use `block_on` against
  xet's runtime, so napi's main thread is the only thread that gets parked.
- **Metadata source.** The xet hash + file size come from the HF Hub's
  `X-Xet-Hash` / `X-Linked-Size` headers. A non-default `User-Agent` is
  required because Cloudfront caches strip those headers on cache hits served
  to default UAs.
- **napi feature level.** Built against `napi8`. Bumping to `napi9`+ would
  unlock newer N-API surfaces if needed.
