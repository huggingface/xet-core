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

`git hf install --local` also installs the repository's `pre-push` hook, which uploads
staged data when you push; `git hf uninstall --local` removes it. Because hooks belong to
a repository, git-hf also installs the hook the first time it stores a file as a Xet
pointer, so a global installation needs no extra step. An existing `pre-push` hook is
kept and still runs first. Neither command modifies attributes or diff/merge
configuration.

## Process filter and local staging

`git-hf filter-process` is the required filter that installation registers.

- **Smudge** (checkout) leaves content unchanged, so Xet-stored files are checked out
  as pointer files.
- **Clean** (`git add`, `git commit -a`, and similar) decides per file:
  - valid Xet pointers and small files are stored in Git unchanged;
  - files larger than 10 MiB, or binary files larger than 100,000 bytes, are stored as
    Xet pointers. This is the same rule the Hub uses, so a file is stored the same way
    however it is uploaded;
  - files larger than 500 GB, and malformed or Git LFS pointers, make the command fail.
- The content of files stored as Xet pointers is deduplicated and saved under
  `.git/xet/staging/`, which linked worktrees share. Nothing is uploaded until push.
- Deduplication against data already on the Hub is optional and never prompts. It uses
  the first credential found: a token in the remote URL, `HF_TOKEN` (or
  `HUGGING_FACE_HUB_TOKEN`), the token saved by `hf auth login`, `.netrc`, your SSH key
  for SSH remotes, or a credential already stored in Git's credential helper. Public
  repositories need no credential. If the Hub cannot be reached quickly, git-hf
  deduplicates against local data only.

If git-hf cannot save the staged data when a command finishes, the command still
succeeds, because Git has already updated the index. git-hf then prints the affected
files; run `git add --renormalize -- <paths>` in that worktree to stage them again.
Staging is not cleaned up automatically.

## Pushing

`git push` works as usual. Before Git sends commits, the `pre-push` hook uploads the
staged data for Xet pointers in the commits being pushed, including earlier commits of
a new branch. A push that references no staged data never contacts the Hub, so pushing
to a mirror or another non-Hub remote is unaffected.

After a successful upload, the uploaded data is removed from the repository's local
staging area. Later `git add` runs that can reach the Hub still deduplicate against it
through the shared Xet cache. Staged files that were not part of the push stay staged. Because the removed data
now exists only on the Hub that received the push, push those commits to that Hub
repository first if you also push them to another one.

Uploading needs write access to the Hub repository. git-hf uses the same credentials as
remote deduplication, and may prompt through Git's credential helper. If authentication
fails, the push stops with a message naming the repository and how to provide
credentials.

The push also stops, before anything is uploaded, if earlier staged data is missing:
either git-hf previously reported that it could not save files, or staged files were
deleted. Re-stage the reported files with `git add --renormalize -- <paths>` and push
again. If the upload fails, staged data is kept and the next push uploads it again.

## Command contracts

Run `git-hf --help`, or put the binary on PATH and use `git hf --help`.

| Command | Contract for subsequent implementation PRs |
| --- | --- |
| `add <paths...>` | Stage whole files concurrently using automatic Xet selection, leaving worktree contents unchanged. |
| `track` | Explain automatic file selection; no manual tracking is needed. |
| `install [--local \| --global \| --system]` | Register the required `hf` filter; default scope is global. `--local` also installs the pre-push hook. |
| `uninstall [--local \| --global \| --system \| --all]` | Remove the hf filter section; default scope is global. `--local` also removes the pre-push hook. |
| `clone <git-clone-args...>` | Accept Git clone options, repository, and destination as an opaque argument list. |
| `fetch [remote] [revision]` | Cache reconstruction metadata and xorb ranges for a local revision (default HEAD); do not fetch Git refs or reconstruct files. |
| `checkout [--force] [paths...]` | Reconstruct selected index entries from local data only. |
| `pull [remote] [--force]` | Fetch content for the current checkout, then reconstruct; do not run `git pull`. |
| `filter-process [--smudge-passthrough]` | Internal required filter; smudge returns stored bytes unchanged. `--smudge-passthrough` is accepted and ignored. |
| `pre-push <remote> <location>` | Internal hook receiving Git's ref updates on stdin; uploads the staged data they reference. |

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
(or an LFS version URL), so probing stays bounded even for large file input. The
streaming filter supplies the complete small blob or at least 4097 bytes of a larger
input, so the parser can recognize oversized input as ordinary data. The size field
represents logical file size; file selection uses the actual input size.

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
