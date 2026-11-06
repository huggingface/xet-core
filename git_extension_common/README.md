# Shared Git support

`git_extension_common` contains Git support shared by `git-xet` and the planned
`git-hf` client. It is an unpublished workspace crate, with no dependency on the
LFS transfer-agent protocol, CLI, or credential selection policy.

The crate exports `GitRepo`, `GitUrl`, `Scheme`, `GitCredentialHelper`,
`SSHCredentialHelper`, `GitExtensionError`, and `Result`. The `process_wrapping`
module provides captured subprocess helpers.

This extraction preserves existing behavior:

- Repository discovery uses libgit2. Default remote selection checks the tracked
  branch, `remote.lfsdefault`, a single configured remote, then `origin`.
- `GitUrl` derives the HTTP(S) host of SSH and HTTP(S) remotes and parses Hugging
  Face repo types.
- `GitCredentialHelper` reads credentials for the remote's HTTP(S) host through
  `git credential fill`. `SSHCredentialHelper` runs `git-lfs-authenticate` over
  SSH, constructing the SSH command with Git's environment/configuration precedence.

Choosing a credential source stays in each client. git-xet's `get_credential`
follows Git LFS conventions, including `lfs.<url>.access`, which other clients
should not inherit implicitly.

`test-utils` exposes temporary-home/repository fixtures, SSH response types, and
`start_local_ssh_server` for dependent test targets. Its `anyhow`, `tempfile`,
`russh`, and `tokio` dependencies are optional; normal builds do not enable this
feature.

`TempHome::new()` isolates Git environment overrides and disables system config
before any setup commands run. `TestRepo` inherits this behavior. The original
environment is restored when the last fixture clone is dropped; tests using these
fixtures must use `#[serial(env_var_write_read)]`. Set intentional Git environment
overrides after constructing the fixture.

Run from the repository root:

```sh
cargo test -p git_extension_common
cargo test -p git_xet --features git-xet-for-integration-test
```

`git2-vendored-openssl` forwards to libgit2's vendored OpenSSL feature.
