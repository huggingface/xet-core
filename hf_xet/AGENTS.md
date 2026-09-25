# Agent Guide for hf_xet

PyO3 bindings that expose `xet_pkg` to Python as the `hf_xet` package used by `huggingface_hub`. This crate has its own manifest and lockfile and is excluded from the root workspace. Shared conventions are in the [root guide](../AGENTS.md).

## Setup

- Activate a virtualenv, then `pip install maturin pytest`.
- Rust unit tests link against `libpython`, so a Python development install must be available (CI installs `libpython<ver>-dev`).
- `pyo3`'s `auto-initialize` feature belongs in `[dev-dependencies]` only; `tests/pyo3_features.rs` enforces this. Do not add it to the regular dependency.

## Build and test

Run from the repository root unless noted.

| Command | Purpose |
| --- | --- |
| `cargo +nightly fmt --manifest-path hf_xet/Cargo.toml --all -- --check` | Format check |
| `cargo clippy -r --verbose --manifest-path hf_xet/Cargo.toml -- -D warnings` | Lint |
| `cargo test --manifest-path hf_xet/Cargo.toml --verbose --no-fail-fast` | Rust unit tests and manifest invariants |
| `(cd hf_xet && maturin develop)` | Build and install the extension into the active virtualenv |
| `pytest hf_xet/tests/ -v` | Python integration tests against the rebuilt extension |
| `(cd hf_xet && maturin build)` | Produce a wheel |

Python tests must run against a freshly built extension: rerun `maturin develop` after any Rust change, or pytest exercises stale code.

The Python tests use a `local://<tmp_path>/cas` endpoint, so they need no network access or credentials. Fixtures and upload helpers live in `tests/conftest.py`; add new tests as `test_...` functions or `Test...` classes using `tmp_path` and the `endpoint` fixture.

`hf_xet/Cargo.lock` is committed. Commit it when dependency changes update it; CI fails otherwise.

## Downstream tests

The [hf-xet-tests workflow](../.github/workflows/hf-xet-tests.yml) builds this crate and runs `huggingface_hub`'s `tests/test_xet_*.py` against it. For changes to the Python API surface, run those tests locally against a checkout of `huggingface_hub` installed with `pip install -e 'huggingface_hub[testing]'`.
