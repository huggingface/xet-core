# git-hf (development foundation)

`git-hf` is the native Xet V2 Git client.

## Installation

Run `git hf install` (or `git-hf install`) to register the filter globally, or
`git hf install --local` inside a repository to register it in the repository's
shared config. Use `--local --path <repository>` to select a repository explicitly,
or `git hf install --system` for system-wide registration. `--global` is the explicit
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
`--local --path <repository>` selects a repository explicitly. Removal is best
effort: Git configuration command errors are ignored, including missing sections.

Neither command modifies attributes, hooks, `core.hooksPath`, or diff/merge
configuration. Hook installation belongs with the pre-push implementation.

## Process filter and local staging

`filter-process --smudge-passthrough` handles Git's version 2 filter protocol.
Smudge returns the original bytes. Clean preserves valid native pointers and
ordinary files byte-for-byte, and selects files larger than 10 MiB or binary
files larger than 100,000 bytes for Xet storage. Binary detection samples the first
512 bytes. Selected files larger than 500,000,000,000 bytes are rejected.
Recognized malformed, unsupported-version, and LFS pointers fail cleaning.

The filter consumes Git's input rather than reopening worktree paths. Selected
content is chunked and deduplicated without SHA-256 computation. Remote dedup
is optional: only local URL, environment, or netrc credentials are used, with
anonymous requests when none are available. Credential helpers, SSH authentication,
and prompts are never invoked. Token discovery and each lookup have a two-second
timeout; any lookup error or timeout disables remote lookup for that process. Once the CAS
endpoint is resolved, protected remote shards are imported into the normal global
Xet cache for that endpoint, and existing cached shards also participate in dedup.
If initial endpoint discovery fails, cleaning uses repository staging alone.
Local staging errors fail the operation. Any failed request receives `status=error`
and ends the filter without finalizing. With `filter.hf.required=true`, Git aborts the
whole command on that error before writing the index, so pointers already returned for
earlier files are never staged and their data is not needed. A failed file's in-flight
work is abandoned with the process. Xorbs already written in full remain in staging,
unreferenced by any shard, until cleanup.

A `CleanSession` owns one lazily initialized `FileUploadSession`. Each
`SelectingFileCleaner` handles a single file using that shared state; cleaners can
run concurrently and contribute to the same xorb aggregation. The session owner
finalizes after all cleaners have finished and Git has closed the input stream.

All processes share `<common-git-dir>/xet/staging/xorbs/` and `shards/`, with
content-addressed objects published atomically. Each process keeps temporary
metadata in its own `staging/session-*/` directory and maintains its own in-memory
xorb aggregation. Completed staged shards are registered for local deduplication
before each selected file, so a running process can reuse another process's
completed staging work. They are read directly from staging through a separate
shard manager owned by `git_hf`, never copied into the global cache. `xet_data`
consults it through the optional `DeduplicationSource` interface. Persistent shards are never
subject to cache expiration. Successful local staging writes do not promote
shards into the remote cache.

Shards remain in temporary session storage until Git closes the input stream.
Finalization writes the remaining xorb and publishes reconstruction shards under
`shards/`. These shards map pointer file hashes to xorb dependencies for later
push. Each shard's xorb table lists only xorbs its session wrote, so the union of
staged xorb tables defines local dependencies: push uploads them, and a missing
`xorbs/<hash>.xorb` for one of them is a local error. Referenced xorbs absent from
every table came from remote dedup data.
Cleaning sends no xorb or shard uploads to the remote service.

Staging survives normal process exit and is shared by linked worktrees.
A pointer response can precede persistence of its partial xorb and reconstruction
metadata. Git closes the filter only after writing the index (or commit) and ignores
its exit status, so a failed finalization cannot fail the Git command. Instead the
filter prints the affected paths and writes the originating worktree root followed
by those paths, NUL-terminated, to `staging/finalize-failed/<session>`. Staging is
shared by linked worktrees, while the paths are relative to that worktree. The pre-push hook will report these markers.
A process killed during finalization leaves no marker; its pointers have no local
staging record and are rejected by server validation at push. In both cases, run
`git add --renormalize -- <paths>` in the originating worktree while its files still exist. No automatic
staging cleanup is performed.

## Command contracts

Run `git-hf --help`, or put the binary on PATH and use `git hf --help`.

| Command | Contract for subsequent implementation PRs |
| --- | --- |
| `add <paths...>` | Stage whole files concurrently using automatic Xet selection, leaving worktree contents unchanged. |
| `track` | Explain automatic file selection; no manual tracking is needed. |
| `install [--local \| --global \| --system] [--path <repository>]` | Register the required `hf` filter; default scope is global. |
| `uninstall [--local \| --global \| --system \| --all] [--path <repository>]` | Remove the hf filter section; default scope is global. |
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

`XetPointer::new(MerkleHash, size)` creates a pointer with the current version. `to_string()` and `write_to()` serialize it as:

```text
version https://hf.co/spec/xet/v1
oid xet:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
size 12345
```

`XetPointer::parse` returns `Ok(None)` for ordinary file data, a validated
pointer for native content, or a typed `PointerError` for recognized malformed,
unsupported-version, or LFS pointers. It enforces the 4096-byte limit, UTF-8/LF
syntax with a final newline, unique required fields, exact version, lowercase
64-digit Xet hash, and a u64 decimal size with no leading zeroes except `0`.
The `hash`, `size`, and `version` fields are directly accessible. Empty lines and unknown
extension keys are accepted and discarded. `to_string()` and `write_to()` serialize
the stored version, hash, and size in canonical order.

`XetPointer::parse_file(path)` opens a file and reads at most 4097 bytes before
calling the byte parser. It returns `GitHfError::IO` for file errors and
`GitHfError::Pointer` for pointer validation errors, or `Ok(None)` for ordinary data.

Recognition scans only the first 4096 bytes for a native version URL or `oid xet:`
line (or an LFS version URL). This keeps probing bounded even for large file input.
The streaming filter supplies the complete small blob or at least 4097 bytes of
a larger input, so the parser can reject oversized recognized pointers. The size
field represents logical file size; file selection uses the actual input size.

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
