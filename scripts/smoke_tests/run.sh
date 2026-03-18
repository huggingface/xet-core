#!/bin/bash
set -euo pipefail

# Smoke test runner for hf-xet upload/download via the hf CLI.
#
# Prerequisites:
#   - uv        (https://docs.astral.sh/uv/)
#   - hf CLI    (pip install huggingface_hub, or uv tool install huggingface_hub)
#   - HF_TOKEN  env var with write access
#
# Usage:
#   ./scripts/smoke_tests/run.sh                              # latest hf_xet from PyPI
#   ./scripts/smoke_tests/run.sh --hf-xet-version 1.4.0      # specific version (bypasses uv cache)
#   ./scripts/smoke_tests/run.sh --skip-buckets               # skip storage bucket tests
#   ./scripts/smoke_tests/run.sh --keep-repo                  # leave test repo/bucket after run
#   HF_XET_WHEEL=./dist/hf_xet-1.4.0.whl ./scripts/smoke_tests/run.sh  # local wheel

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -z "${HF_TOKEN:-}" ]; then
    echo "ERROR: HF_TOKEN environment variable is required" >&2
    echo "  export HF_TOKEN=hf_..." >&2
    exit 1
fi

if ! command -v uv &> /dev/null; then
    echo "ERROR: uv is required. Install: curl -LsSf https://astral.sh/uv/install.sh | sh" >&2
    exit 1
fi

if ! command -v hf &> /dev/null; then
    echo "ERROR: hf CLI is required. Install: uv tool install huggingface_hub" >&2
    exit 1
fi

# Parse --hf-xet-version from args (if present)
HF_XET_VERSION=""
for arg in "$@"; do
    if [[ "${prev_arg:-}" == "--hf-xet-version" ]]; then
        HF_XET_VERSION="$arg"
    fi
    prev_arg="$arg"
done

echo "Running hf-xet smoke tests..."
echo ""

if [ -n "${HF_XET_WHEEL:-}" ]; then
    echo "Using local wheel: ${HF_XET_WHEEL}"
    uv run --with "${HF_XET_WHEEL}" "${SCRIPT_DIR}/test_upload_download.py" "$@"
elif [ -n "${HF_XET_VERSION}" ]; then
    echo "Using hf_xet version: ${HF_XET_VERSION} (fetching from PyPI)"
    uv run --with "hf_xet==${HF_XET_VERSION}" --refresh-package hf_xet "${SCRIPT_DIR}/test_upload_download.py" "$@"
else
    uv run "${SCRIPT_DIR}/test_upload_download.py" "$@"
fi
