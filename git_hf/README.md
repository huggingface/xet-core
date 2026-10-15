# git-hf (development foundation)

`git-hf` is the native Xet V2 Git client.

## Installation

Run `git hf install` (or `git-hf install`) to register the filter globally, or
`git hf install --local` inside a repository to register it in the repository's
shared config, or `git hf install --system` for system-wide registration. `--global` is the explicit
default. Global installation works outside a repository and follows Git's global
configuration location rules.

Installation sets:

```gitconfig
[filter "hf"]
    process = git-hf filter-process --smudge-passthrough
    required = true
```

Installation rejects conflicting `filter.hf.process` values before changing either
setting. When the process is absent or matches, it sets `filter.hf.required=true`,
replacing any existing values. Repeated installation leaves the same settings. Other scopes and included files are
left untouched; Git's usual configuration precedence still applies.

`git hf uninstall` removes the entire `filter.hf` section from global config,
including modified values. Use `--system` or `--local` to select another scope,
or `--all` to attempt removal from system, global, and current repository config.
`--local` uses the repository Git selects for the current directory, as
`git config --local` does. Removal is best effort: Git configuration command errors are ignored, including missing sections.

Neither command modifies attributes, hooks, `core.hooksPath`, or diff/merge
configuration. Hook installation belongs with the pre-push implementation.

## Command contracts

Run `git-hf --help`, or put the binary on PATH and use `git hf --help`.

| Command | Contract for subsequent implementation PRs |
| --- | --- |
| `add <paths...>` | Stage whole files concurrently using automatic Xet selection, leaving worktree contents unchanged. |
| `track` | Explain automatic file selection; no manual tracking is needed. |
| `install [--local \| --global \| --system]` | Register the required `hf` filter; default scope is global. |
| `uninstall [--local \| --global \| --system \| --all]` | Remove the hf filter section; default scope is global. |
| `clone <git-clone-args...>` | Accept Git clone options, repository, and destination as an opaque argument list. |
| `fetch [remote] [revision]` | Cache reconstruction metadata and xorb ranges for a local revision (default HEAD); do not fetch Git refs or reconstruct files. |
| `checkout [--force] [paths...]` | Reconstruct selected index entries from local data only. |
| `pull [remote] [--force]` | Fetch content for the current checkout, then reconstruct; do not run `git pull`. |
| `filter-process [--smudge-passthrough]` | Internal required filter; supports `--smudge-passthrough` for identity smudge. |
| `pre-push <remote> <location>` | Internal hook receiving Git's ref updates on stdin. |

Internal commands are hidden from top-level help but have their own `--help`.
Use `--` before paths beginning with `-`. Paths and clone locations retain native
OS strings; they are not interpolated into shell commands.

Clone retains its original argument list, including `--` and non-UTF-8 paths,
for eventual forwarding to Git. `clone --help` and `clone --version` currently
show git-hf's help/version. Git option validation, destination discovery, and
checkout/materialization policy for options such as `--bare` and `--no-checkout`
will be handled when clone execution is implemented.

## Pointer API

The pointer format is implemented in the `xet_pointer` module of
`xet-core-structures`; git-hf re-exports `XetPointer`, `PointerError`, and
`MAX_POINTER_SIZE`.

`XetPointer::new(MerkleHash, size)` creates a pointer with the current version. `to_string()` and `write_to()` serialize it as:

```text
version https://hf.co/spec/xet/v1
oid xet:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
size 12345
```

`XetPointer::parse` returns `Ok(None)` for ordinary file data, a validated
pointer for native content, or a typed `PointerError` for recognized malformed,
unsupported-version, or LFS pointers. Input longer than 4096 bytes is always
ordinary data. Otherwise it enforces UTF-8/LF
syntax with a final newline, unique required fields, exact version, case-insensitive
64-digit Xet hash, and a u64 decimal size with no leading zeroes except `0`.
The `hash`, `size`, and `version` fields are directly accessible. Empty lines and unknown
extension keys are accepted and discarded. `to_string()` and `write_to()` serialize
the stored version, hash, and size in canonical order.

`XetPointer::parse_file(path)` opens a file and returns `Ok(None)` without reading
it when its metadata shows a regular file larger than 4096 bytes. Otherwise,
including when metadata is unavailable, it reads at most 4097 bytes (the file may
grow, or report no size) before calling the byte parser. It returns `CoreError::Io` for file errors and
`CoreError::XetPointer` for pointer validation errors, or `Ok(None)` for ordinary data.

Recognition scans at most 4096 bytes for a native version URL or `oid xet:` line
(or an LFS version URL), so probing stays bounded even for large file input. A
future streaming filter must supply the complete small blob or at least 4097 bytes
of a larger input, so the parser can recognize oversized input as ordinary data.
The size field represents logical file size; selection/upload size limits are
separate policy, to be implemented with clean.

## Development

```sh
cargo run -p git_hf -- --help
cargo test -p git_hf
cargo +nightly fmt -p git_hf -- --check
```

Unit tests cover pointer boundaries, malformed inputs, and canonical serialization.
Install/uninstall unit tests live beside their implementations and call the handlers
directly, using shared `TestRepo`/`TempHome` fixtures and serialized environment guards.
They cover filter registration/removal, process conflicts, required-value replacement,
idempotence, uninstall scopes, and installation error propagation. Tests need Git, but no Hub
credentials or network access.
