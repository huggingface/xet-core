# hf_xet C API examples

> **Experimental:** the `hf_xet` C API is under active development. Its
> surface (function signatures, ownership rules, struct layouts) may change
> without notice until it stabilizes.

Each subdirectory is a self-contained program that performs the same
upload → commit → download round-trip against a Hugging Face Xet repo, using
only the `hf_xet` C API (`xet_capi/include/hf_xet.h`):

| Language | Directory | Build |
|----------|-----------|-------|
| C        | [`c/`](c/)       | `make run`     |
| C++      | [`cpp/`](cpp/)   | `make run`     |
| Go (cgo) | [`go/`](go/)     | `go run .`     |
| Swift    | [`swift/`](swift/) | `./build.sh run` |

## Prerequisites

1. **Build the shared library** (the examples link `libxet_capi`):

   ```bash
   cargo build -p xet_capi --release
   ```

   This produces `target/release/libxet_capi.dylib` (macOS) / `.so` (Linux),
   which is self-contained — no need to pull in Rust's transitive native
   dependencies. The examples set an rpath to `target/release`.

2. **Set `HF_TOKEN`** to a token with write access to the target repo:

   ```bash
   export HF_TOKEN=hf_...
   ```

3. **(Optional) pick a repo.** All examples default to the dataset
   `assafvayner/xet-c-api-test` and accept an override as the first argument,
   e.g. `./upload_download my-user/my-dataset`.

## What each example demonstrates

- Creating a `XetSession`.
- Deriving per-operation auth from a Hub **token-refresh URL**
  (`.../xet-write-token/<rev>` and `.../xet-read-token/<rev>`) plus an
  `Authorization: Bearer $HF_TOKEN` header — the library fetches the CAS
  endpoint and short-lived token itself.
- Uploading random bytes, finalizing, and reading back the content hash + size.
- Committing (registers the shard so the hash is reconstructable).
- Downloading by hash to a file and verifying the bytes round-trip.
- The handle-based polling model (`xet_op_poll` → `xet_op_take_*`) and the
  ownership rule that **every handle is freed exactly once** — in particular an
  op is always freed with `xet_op_free`, even after a `xet_op_take_*`.

The Go and Swift versions also show the small amount of glue each ecosystem
needs: cgo pointer-pinning for the auth-config struct, and a Clang module map to
import the C header into Swift.
