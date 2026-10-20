# Agent Guide for git_xet

Git LFS custom transfer agent (`git-xet`) built on `xet_pkg`. Part of the root workspace. Shared conventions are in the [root guide](../AGENTS.md); user-facing installation is in the [README](README.md).

## Setup

- `git` and `git-lfs` must be on `PATH`, and run `git lfs install` once. Many unit tests create real repositories with `TestRepo` / `TempHome` under a temporary `HOME` so your global Git config is untouched.
- The integration tests in `tests/test_ssh.rs` start a local SSH server through `russh`; no external SSH service is needed.

## Build and test

Run from the repository root.

| Command | Purpose |
| --- | --- |
| `cargo test --package git_xet` | Unit tests |
| `cargo test --package git_xet --features git-xet-for-integration-test` | Unit tests plus SSH and process-invocation integration tests |
| `cargo clippy -r --verbose -- -D warnings` | Lint (with the root workspace) |
| `cargo build --package git_xet --release` | Build the `git-xet` binary |

The `git-xet-for-integration-test` feature also compiles test-only hooks into the binary (`src/app.rs`, `src/test_utils/`); it is never enabled for release builds. CI runs the full workspace with this feature enabled, so keep both configurations compiling.

The integration tests invoke the freshly built `git-xet` binary through `git xet run-any`, so they run as a child of a real `git` process, the same way `git-lfs` launches the agent. Their main purpose is Windows behavior (Git for Windows adds MinGW/MSYS utilities such as `ssh` and `sh` to the child `PATH`), so check the Windows CI job when touching process spawning or SSH handling. Tests that mutate environment variables use `#[serial(...)]` from `serial_test`; follow that pattern for new ones.

`git_xet` is excluded from `cargo bench`; do not add benchmarks here.
