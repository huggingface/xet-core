# Shared Git support

`git_extension_common` contains Git support shared by `git-xet` and the planned
`git-hf` client. It is an unpublished workspace crate, with no dependency on the
LFS transfer-agent protocol or CLI.

The crate exports `GitRepo`, `GitUrl`, `Scheme`, `AccessMode`, `get_credential`,
`new_git_token_refresher`, `GitExtensionError`, and `Result`. The
`process_wrapping` module provides captured subprocess helpers.

This extraction preserves existing behavior:

- Repository discovery uses libgit2. Default remote selection checks the tracked
  branch, `remote.lfsdefault`, a single configured remote, then `origin`.
- Credentials respect `lfs.<url>.access`, URL credentials, `HF_TOKEN`, netrc,
  SSH `git-lfs-authenticate`, and Git credential helpers, in that order.
- SSH command construction respects Git's existing environment/configuration
  precedence. Token refresh uses the same credential helper and HTTP client.

These defaults retain git-xet's LFS conventions. Callers needing different
native-Git policies should introduce those explicitly in subsequent changes.

`test-utils` exposes temporary-home/repository fixtures, SSH response types, and
`start_local_ssh_server` for dependent test targets. Its `anyhow`, `tempfile`,
`russh`, and `tokio` dependencies are optional; normal builds do not enable this
feature. The process integration tests remain in `git_xet` and import the shared
fixtures directly through a dev-dependency.

Run from the repository root:

```sh
cargo test -p git_extension_common
cargo test -p git_xet --features git-xet-for-integration-test
```

`git2-vendored-openssl` forwards to libgit2's vendored OpenSSL feature.
