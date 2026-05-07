# xet_pkg_napi — napi smoke test for hf-xet

A minimal [napi-rs](https://napi.rs) native addon that links against the
`xet_pkg` (`hf-xet`) crate. The point is to verify that `hf-xet` compiles and
its tokio-based runtime starts inside a Node.js native module — *not* to
expose a full JS API.

## What it does

The Rust crate at `src/lib.rs` exposes two functions to Node:

- `initLogging(version: string)` — installs `xet`'s tracing subscriber
- `smokeTest(): string` — builds a `XetSession` synchronously, then constructs
  upload-commit and file-download-group builders. Exercises the parts of
  `xet-runtime` that lazily spawn worker threads. Returns a status string.

If `node smoke.mjs` prints `xet session built; runtime initialized`, the
integration is ready for a more featureful binding (async upload/download,
progress callbacks via `ThreadsafeFunction`, etc.).

This crate is **excluded from the workspace** (see the root `Cargo.toml`)
because it has its own `crate-type = ["cdylib"]` and ships under the
`napi-rs/cli` build flow rather than `cargo build`.

## Build & run

Requires Node ≥ 16 and a Rust toolchain.

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

`smoke.mjs` loads `index.js` via `createRequire` and calls the two exported
functions.

## Expected output

```
loaded addon, exports: [ 'initLogging', 'smokeTest' ]
smokeTest: xet session built; runtime initialized
```

## Notes / caveats

- The binding is **sync only** for this smoke test. A real binding should mark
  upload/download as `#[napi]` async functions and use `ThreadsafeFunction`
  for progress callbacks so the libuv main thread isn't blocked.
- `xet-runtime` owns its own tokio runtime — it does not piggyback on libuv,
  so there's no double-runtime conflict. Async napi calls run on napi's own
  worker pool and call into xet via its blocking `*_blocking` entrypoints, or
  spawn onto xet's runtime directly.
- napi-rs `napi8` features are required for everything we use here. Bumping
  to `napi9`+ would unlock newer N-API surfaces if needed.
