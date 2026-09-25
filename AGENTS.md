# Agent Guide for xet-core

## Project overview

Rust libraries for Xet Storage: chunk-based deduplication, uploads, and file reconstruction for the Hugging Face Hub. `hf_xet` exposes them to Python through `huggingface_hub`. Preserve data integrity, transfer throughput, and bounded memory use.

## Architecture documentation

**Read the documentation and implementation for the layer you are changing:**

- [Project overview](README.md) — setup and usage.
- [Session API](xet_pkg/README.md) — public Rust API and crate relationships.
- [Data processing](xet_data/README.md) — chunking, deduplication, and reconstruction.
- [Client](xet_client/README.md) — CAS and Hub communication.
- [Core structures](xet_core_structures/README.md) — hashes, shards, and Xorb objects.
- [Runtime](xet_runtime/README.md) — async runtime, configuration, and logging.
- [Python extension](hf_xet/README.md) — Python package usage.
- [Git extension](git_xet/README.md) — Git LFS transfer agent and installation.
- [Browser WASM wrapper](wasm/hf_xet_wasm/README.md) — upload/download APIs and smoke tests; an example, not a published SDK.
- [Thin WASM extension](wasm/hf_xet_thin_wasm/README.md) — chunking and hashing for JavaScript; [benchmarks](wasm/hf_xet_thin_wasm/CHUNKER_BENCHMARK.md).

`CLAUDE.md` links to this file. Keep shared agent guidance here. Component-specific build and test instructions live in [hf_xet/AGENTS.md](hf_xet/AGENTS.md), [git_xet/AGENTS.md](git_xet/AGENTS.md), and [wasm/AGENTS.md](wasm/AGENTS.md).

## Setup

- **Rust**: follow the version pinned in [CI](.github/workflows/ci.yml), currently 1.95.0. Use nightly `rustfmt`.
- **Python**: activate a virtualenv and install `maturin` and `pytest`. Build/install with `maturin develop`; produce wheels with `maturin build`.
- **Git tests**: install Git LFS and run `git lfs install`.
- **Separate manifests**: `hf_xet`, `simulation`, `wasm/*`, and `examples/xet_pkg_napi` are excluded from the root workspace. Check them separately when affected.

## Key commands

Run from the repository root unless noted.

| Command | Purpose |
| --- | --- |
| `cargo build` | Build the root workspace |
| `cargo test` | Test the root workspace with default features |
| `cargo test --package hf-xet` | Test one package; substitute the affected package |
| `cargo test --package xet-client test_name` | Run matching tests |
| `cargo build --package hf-xet --example example --features internal-tools` | Build the async example; use `example_sync` for sync |
| `cargo clippy -r --verbose -- -D warnings` | Lint the root workspace |
| `cargo clippy -r --verbose --manifest-path hf_xet/Cargo.toml -- -D warnings` | Lint Python bindings |
| `cargo +nightly fmt --manifest-path Cargo.toml --all` | Format the root workspace |
| `cargo +nightly fmt --manifest-path hf_xet/Cargo.toml --all` | Format Python bindings |
| `cargo test --manifest-path hf_xet/Cargo.toml --verbose --no-fail-fast` | Test bindings in Rust |
| `(cd hf_xet && maturin develop)` | Build/install the Python extension |
| `pytest hf_xet/tests/ -v` | Test the rebuilt Python extension |
| `cargo bench --no-run --workspace --exclude git_xet` | Check benchmark compilation |

Use Cargo for binding tests/lints and maturin for importable extensions. Keep PyO3's `auto-initialize` feature in test dependencies only.

The `simulation` feature enables workspace support, not the excluded `simulation/` package. The `cargo smoke-test` alias currently references that excluded package; use targeted tests instead.

## Verification

Run the checks that cover what you changed before claiming work is done. Documentation-only changes need no builds or tests. These mirror [CI](.github/workflows/ci.yml); everything here runs locally without network access or Hub credentials.

Every Rust change:

```bash
cargo +nightly fmt --manifest-path Cargo.toml --all -- --check
cargo +nightly fmt --manifest-path hf_xet/Cargo.toml --all -- --check
cargo clippy -r --verbose -- -D warnings
cargo clippy -r --verbose --manifest-path hf_xet/Cargo.toml -- -D warnings
cargo test --package <changed-package>
```

Full root workspace with CI features (required for Git, simulation, or cross-crate changes):

```bash
cargo test --verbose --no-fail-fast --features "strict simulation internal-tools git-xet-for-integration-test"
```

Per-component checks; each directory has its own `AGENTS.md` with setup details:

| Changed area | Run | Details |
| --- | --- | --- |
| `hf_xet/` or anything it depends on | `cargo test --manifest-path hf_xet/Cargo.toml --no-fail-fast`, then `(cd hf_xet && maturin develop) && pytest hf_xet/tests/ -v` | [hf_xet/AGENTS.md](hf_xet/AGENTS.md) |
| `git_xet/` | `cargo test --package git_xet --features git-xet-for-integration-test` | [git_xet/AGENTS.md](git_xet/AGENTS.md) |
| `wasm/*` or the crates they depend on | `(cd xet_pkg && ./build_wasm.sh)`, `(cd wasm/hf_xet_thin_wasm && ./build_wasm.sh)`, `(cd wasm/hf_xet_wasm && ./build_wasm.sh)` | [wasm/AGENTS.md](wasm/AGENTS.md) |
| `benches/` or bench dependencies | `cargo bench --no-run --workspace --exclude git_xet` | |
| Dependency changes | `cargo machete`, `cargo audit -D warnings`, then confirm `git status --porcelain '*Cargo.lock'` is empty | |

Lockfiles are committed for the root workspace, `hf_xet`, and both `wasm/*` crates; CI fails if a build leaves them modified.

Report which checks ran, which failed, and which were skipped.

## Code structure

API layers: `hf_xet` → `hf-xet` → `xet-data`, supported by `xet-client`, `xet-core-structures`, and `xet-runtime`.

| Path | Cargo package | Responsibility |
| --- | --- | --- |
| `xet_pkg/` | `hf-xet` | Public Rust API; library name `xet` |
| `xet_data/` | `xet-data` | Chunking, deduplication, uploads, reconstruction |
| `xet_client/` | `xet-client` | CAS/Hub clients, retries, connections, chunk cache |
| `xet_core_structures/` | `xet-core-structures` | Hashes, shard/Xorb formats, shared structures |
| `xet_runtime/` | `xet-runtime` | Runtime, configuration, logging, utilities |
| `git_xet/` | `git_xet` | Git CLI and integration |
| `hf_xet/` | `hf_xet` | PyO3 bindings; separate manifest |
| `simulation/` | `simulation` | Simulations/benchmarks; separate manifest |
| `wasm/` | Separate crates | Browser/WASM builds |

### Where to make changes

- **Sessions**: `xet_pkg/src/xet_session/` — commits, groups, handles, background tasks.
- **Uploads**: `xet_data/src/processing/` and `deduplication/`.
- **Downloads**: `xet_data/src/file_reconstruction/`.
- **Networking/cache**: `xet_client/src/cas_client/`, `hub_client/`, `chunk_cache/`.
- **Runtime/configuration**: `xet_runtime/src/core/`, `config/`, `logging/`.
- **Python**: `hf_xet/src/`; update related examples and `hf_xet/tests/`.

## Code style

Follow nearby conventions. Do not rename existing APIs or reformat unrelated code for uniformity.

### Simplicity is the #1 priority

- No premature abstractions or unnecessary generalization. Traits, generics, and macros need a concrete use case.
- Don't implement features until needed.
- Don't accept parameters, type parameters, or configuration flags without a use case.
- Prefer concrete types, early returns, and straightforward control flow.
- Prefer strictness now, relax later: narrow visibility, valid states encoded in types, explicit rejection of unsupported inputs.

### Formatting, imports, and comments

- Follow [rustfmt.toml](rustfmt.toml): 120-column code/comments, 100-column calls, 80-column chains, field-init shorthand, trailing commas after block match arms.
- Group imports: standard library, external/workspace crates, then `crate` / `super`. Group imports from the same module with braces.
- Prefer explicit imports. Existing exceptions include test `use super::*`, binding preludes, and module re-exports.
- Use `//!` for module docs and `///` for public items. Explain purpose, invariants, units, lifecycle, and failures; don't restate code.
- Keep comments, examples, defaults, and public names synchronized. Remove dead code and redundant checks; extract substantial validation into named helpers.

### Naming and modules

- Files/modules, functions, fields, and variables: `snake_case`. Types/traits/variants: `UpperCamelCase`. Constants/statics: `SCREAMING_SNAKE_CASE`.
- Name files by responsibility (`file_upload_session.rs`); related types can share a module.
- Use `lib.rs` / `mod.rs` for declarations and re-exports. Expose needed types without making implementation modules public.
- Errors usually live in `error.rs`; Git and session modules use `errors.rs`. Helpers use `utils`, `common`, or `test_utils`; keep domain helpers near consumers.
- Name types by role: `FileUploadSession`, `ShardFileManager`, `DataWriter`. Traits have no `I` prefix. Use `Builder`, `Inner`, `Report`, `Info`, and `State` suffixes where meaningful.
- Session types use `Xet...`; Python wrappers use `PyXet...` in `py_*.rs`, with `#[pyclass(name = "...")]`. Thin WASM wrappers use `Js...`.
- Use `new`, `with_*`, `build`, and `_blocking` for sync counterparts. Predicates use `is_*` / `has_*`; accessors describe their value (`progress`, `task_id`).
- Preserve existing acronym spelling (`XorbObject`, `MDBShardFile`, `URLProvider`, `IOError`). Include units in numeric names; use `Duration` / `ByteSize` where appropriate.
- `xet_pkg/` is package `hf-xet`, library `xet`; Python bindings alias it as `xet-pkg`, imported as `xet_pkg`.

### Types and ownership

- Place type definitions before implementations, with unit tests at the bottom.
- Derive useful standard traits; reserve `Copy` for suitable value types and serde derives for serialized data.
- Shared handles use `Arc<FooInner>`; cloning shares state. Keep locks and lifecycle state inside the inner type.
- Keep implementation fields private or narrowly visible. Public data/configuration fields follow local conventions.
- Builders usually consume and return `Self`. Borrow strings, paths, and slices when ownership is unnecessary; use `impl Into<String>` / `impl AsRef<Path>` where useful.
- Use enums for mutually exclusive states, such as `Sha256Policy::{Compute, Provided, Skip}`. Prefer named structs over opaque tuples.
- Reuse domain types and conversions. CAS response types belong in `cas_types`; use `HexMerkleHash` for serialized hash strings.

### Errors and `Result`

- Extend crate/subsystem error enums deriving `thiserror::Error` and `Debug`, with readable `#[error("...")]` messages.
- Use `#[from]` for direct conversions; manual `From` implementations for classification, context, redaction, or `Arc` wrapping. Follow existing `#[non_exhaustive]` policy.
- Lower layers commonly define `pub type Result<T> = std::result::Result<T, TheirError>`. Use the local alias or explicit error type; avoid new aliases such as `CoreResult`.
- Session APIs use `Result<T, SessionError>` / `Result<T, XetError>`; `SessionError` aliases `XetError`. Python boundaries use `PyResult<T>`.
- Propagate with `?` / `.await?`. Use `map_err` for conversion/context and `ok_or_else` for missing required values. Return final result expressions directly when possible.
- Keep typed library errors; `anyhow` is common in tools and test helpers. Preserve [XetError](xet_pkg/src/error.rs) categories and Python mappings through `convert_xet_error` / `From<XetError> for PyErr`.
- Keep `JsValue` / N-API error conversions at binding boundaries.
- Use `.unwrap()` / `.expect()` in tests or for explicit, justified invariants. Return errors for fallible I/O, parsing, network operations, and caller input.
- Ignore results only for intentionally best-effort work. Preserve original task errors across channels; distinguish cancellation from panic.
- Don't assume error cloning preserves its cause: reconstruction errors use `Arc`, while `ClientError` has a lossy clone fallback.

Example from [DataError](xet_data/src/error.rs):

```rust
#[derive(thiserror::Error, Debug)]
pub enum DataError {
    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),
    // Other variants omitted.
}

pub type Result<T> = std::result::Result<T, DataError>;
```

### Logging

- Use `tracing` in libraries; reserve `println!` / `eprintln!` for CLI output and diagnostics.
- Prefer structured fields: `%value` for `Display`, `?value` for `Debug`, plain values for scalar fields.
- Follow local severity policy. Expected retries are not terminal errors. Preserve CAS `INFORMATION_LOG_LEVEL` behavior and avoid duplicate logging across layers.
- Use `#[instrument(skip_all, fields(...))]` with selected metadata and `.instrument(span)` for async context. Don't capture credentials or full payloads.
- [ErrorPrinter](xet_runtime/src/error_printer/mod.rs) provides `log_error`, `warn_error`, `debug_error`, and `info_error`. They log failures and return the result unchanged; append `?` to propagate. `_fn` variants build messages lazily. `OptionPrinter` provides `*_none` helpers.
- Use `inspect_err` for logging without conversion. Preserve URL redaction in `ClientError::from(reqwest::Error)`; don't log raw signed URLs first.
- Emit one-time diagnostics after subscriber initialization. Distinguish logical, network/compressed, and deduplicated bytes in metrics. Show user operations, not shard bookkeeping, in progress output.

### Concurrency and configuration

- Reuse `XetRuntime`, `XetContext`, task helpers, and progress types.
- Keep synchronous lock scopes short and never across `.await`. Use async locks when guards must span awaits. Centralize lock ordering and check for nested acquisition.
- Update related state atomically. Concurrent deletion must not erase a fresh upload's metadata. Use `SafeFileCreator` or temp-file/rename patterns for atomic file replacement.
- Define failure, cancellation, drop, and shutdown behavior for all child tasks. Cancel/drain siblings as required before returning; cleanup failures must not skip remaining children.
- Release the Python GIL with `py.detach` for blocking work. Follow existing `blocking_call_with_signal_check` usage for responsive Ctrl-C.
- Use `config_group!` under `xet_runtime/src/config/groups/`, documented defaults, and `HF_XET_<GROUP>_<FIELD>` names. Avoid scattered environment reads and duplicate constants.
- Validate configuration at construction and activation after mutation; avoid hidden changes inside consumers.
- Preserve platform/feature gates. WASM async traits may need `?Send`. Keep lint exceptions narrow; `strict` builds deny warnings.

## Development guidelines

### Scope and compatibility

- Trace the real call path and active configuration before changing behavior. Establish the affected use case, including optional features.
- Keep diffs focused; exclude unrelated dependency bumps and generated changes. Design public APIs from concrete caller examples.
- Preserve wire/storage layouts, serde attributes, discriminants, canonical chunking, and hashes. Validate lengths, counts, and checked arithmetic before allocation/composition; don't let `zip` silently truncate protocol data.
- Coordinate endpoint/format changes with server rollout and test fallback behavior. Simulation clients must match observable production behavior, including streamed failures.
- Follow [api_changes/README.md](api_changes/README.md) for downstream-facing API changes. Scan relevant entries during merges/rebases; document migration steps.
- Reuse workspace dependencies, keep test/tool dependencies out of normal builds, and follow root manifest notes for WASM version synchronization.

### Performance

- Measure relevant workloads before optimizing. Prefer buffer/client reuse and fewer allocations before unsafe or architecture-specific complexity.
- Bound aggregate memory, tasks, requests, and teardown waits across concurrent transfers. Budgets must accommodate the largest valid work item and make progress at low settings.
- Check affected Windows, WASM, Python, and optional-feature builds; gate platform-specific dependencies.

### Tests

- Unit tests: `#[cfg(test)] mod tests`, usually at file end. Integration tests: `tests/test_<behavior>.rs`. Benchmarks: `benches/`.
- Follow nearby runtime setup. Reuse `tempfile`, seeded data, simulation clients, and local servers. Use `EnvVarGuard` to restore environment changes.
- Regression tests should fail on the original bug. Assert exact deterministic values, ordering, content types, and relevant error variants.
- Cover relevant boundaries, retries, duplicate/out-of-order events, interruption, and cleanup. Use realistic payload sizes for streaming tests.
- Keep fixtures and coverage claims accurate: fixed seeds repeat data, cached versions affect fallback paths, and parser tests don't exercise the retry pipeline.
- Python tests use pytest fixtures, `tmp_path`, local CAS, `Test...` classes, and `test_...` methods. Rebuild bindings first.
- WASM smoke tests use `.mjs`, camelCase identifiers, and kebab-case scenario files; follow local script style.
- Report checks run, failures, and checks not completed.

### Review and communication

- Focus on correctness, compatibility, data integrity, lifecycle, resource limits, and coverage.
- Explain concrete impacts; distinguish confirmed defects from questions and optional `nit:` suggestions.
- Describe the problem, resulting behavior, validation, and affected configuration. Include examples/measurements where useful; flag breaking changes, coordination needs, and limitations.

## Debugging

Use `RUST_BACKTRACE=full`, `RUST_LOG=info` (or `debug` / `trace`), and `HF_XET_LOG_FILE=/tmp/xet.log`.

See [diagnostic scripts](scripts/diag/README.md) for symbol/trace capture. The [smoke tests](scripts/smoke_tests/README.md) exercise a released or locally built `hf_xet` wheel against the real Hub and need an `HF_TOKEN`; they are a release check, not part of routine verification.
