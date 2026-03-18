# hf-xet Smoke Tests

End-to-end tests that exercise the full hf-xet upload/download path against the
real HuggingFace Hub. They use the `hf` CLI for all Hub operations and verify
content integrity with SHA-256 checksums.

## Prerequisites

- [`uv`](https://docs.astral.sh/uv/)
- [`hf` CLI](https://huggingface.co/docs/huggingface_hub/en/guides/cli) — `uv tool install huggingface_hub`
- `HF_TOKEN` environment variable with write access

## Usage

```bash
# Test latest hf_xet from PyPI
./scripts/smoke_tests/run.sh

# Test a specific version (bypasses uv cache, fetches directly from PyPI)
./scripts/smoke_tests/run.sh --hf-xet-version 1.4.0

# Test a local wheel
HF_XET_WHEEL=./dist/hf_xet-1.4.0.whl ./scripts/smoke_tests/run.sh

# Skip storage bucket tests
./scripts/smoke_tests/run.sh --skip-buckets

# Keep the test repo/bucket after the run (useful for debugging)
./scripts/smoke_tests/run.sh --keep-repo
```

## What's tested

### Repository tests (`hf upload` / `hf download`)

Uploads test files of varying sizes (1 KB → 100 MB) to a temporary private
model repo, then downloads and verifies every file's SHA-256 hash.

| Test | Description |
|------|-------------|
| Upload single file | `hf upload` of a single file |
| Upload folder | `hf upload` of an entire directory tree |
| Download individual files | Per-file `hf download` + hash check |
| Download all files | Full-repo `hf download` + hash check |
| Overwrite and re-download | Confirms updated content is served after overwrite |
| Delete file | `hf repos delete-files` + confirms file is absent |

### Bucket tests (`hf buckets`)

Creates a temporary private bucket and exercises the full bucket lifecycle.

| Test | Description |
|------|-------------|
| cp upload | `hf buckets cp` single file upload |
| sync upload | `hf buckets sync` directory upload |
| list | Recursive listing confirms all expected paths |
| cp download | `hf buckets cp` download + hash check |
| sync download | `hf buckets sync` directory download + hash check |
| Overwrite | cp overwrite + re-download confirms new content |
| sync --delete | Extraneous remote files removed when absent locally |
| rm | `hf buckets rm` + confirms file absent from listing |

All temporary repos and buckets are deleted after the run unless `--keep-repo` is set.
