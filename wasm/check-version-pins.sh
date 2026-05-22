#!/usr/bin/env bash
# Asserts that every place pinning wasm-bindgen / wasm-pack agrees on a single
# version. The pinned wasm-bindgen runtime crate MUST match the wasm-bindgen-cli
# version that emits the JS glue, otherwise wasm modules break at load time in
# ways that are easy to miss locally and a pain to debug from a CI log.
#
# wasm-bindgen is duplicated across:
#   - workspace Cargo.toml ([workspace.dependencies])
#   - wasm/hf_xet_wasm/Cargo.toml (separate workspace)
#   - wasm/hf_xet_thin_wasm/Cargo.toml (separate workspace)
#   - wasm/hf_xet_wasm/build_wasm.sh ($WASM_BINDGEN_VERSION)
#   - .github/actions/build-wasm/action.yml (cargo install + cache key)
#
# wasm-pack is pinned in:
#   - .github/actions/build-wasm/action.yml (cargo install + cache key)
#
# Cargo doesn't allow templating versions from an external file and the wasm
# subcrates are intentionally separate workspaces, so dedup-by-construction
# isn't possible without restructuring. This script is the next-best thing:
# the source of truth is the workspace Cargo.toml; everything else is checked
# against it.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

# Extract the source-of-truth version from the workspace Cargo.toml.
WBG_VERSION="$(awk -F'"' '/^wasm-bindgen = "=[0-9]/ {print $2; exit}' Cargo.toml | tr -d '=')"
if [[ -z "$WBG_VERSION" ]]; then
  fail "could not read workspace wasm-bindgen pin from Cargo.toml"
fi

# Pull wasm-pack from the action.yml cache key (no Cargo.toml equivalent).
WP_VERSION="$(awk -F'-' '/cargo-tools-wbg-/ {for (i=1;i<=NF;i++) if ($i=="wp") {print $(i+1); exit}}' \
  .github/actions/build-wasm/action.yml)"
if [[ -z "$WP_VERSION" ]]; then
  fail "could not read wasm-pack version from .github/actions/build-wasm/action.yml"
fi

echo "Expected wasm-bindgen: $WBG_VERSION"
echo "Expected wasm-pack:    $WP_VERSION"

errors=0

check_contains() {
  local file="$1"
  local needle="$2"
  local label="$3"
  if ! grep -qF -- "$needle" "$file"; then
    echo "MISMATCH: $file does not contain expected $label ($needle)" >&2
    errors=$((errors + 1))
  fi
}

check_contains "wasm/hf_xet_wasm/Cargo.toml"      "wasm-bindgen = \"=$WBG_VERSION\"" "wasm-bindgen pin"
check_contains "wasm/hf_xet_thin_wasm/Cargo.toml" "wasm-bindgen = \"=$WBG_VERSION\"" "wasm-bindgen pin"
check_contains "wasm/hf_xet_wasm/build_wasm.sh"   "WASM_BINDGEN_VERSION=\"$WBG_VERSION\"" "WASM_BINDGEN_VERSION"
check_contains ".github/actions/build-wasm/action.yml" \
  "cargo install --version $WBG_VERSION wasm-bindgen-cli" "wasm-bindgen-cli install"
check_contains ".github/actions/build-wasm/action.yml" \
  "cargo install --version $WP_VERSION wasm-pack" "wasm-pack install"
check_contains ".github/actions/build-wasm/action.yml" \
  "wbg-$WBG_VERSION-wp-$WP_VERSION" "cache key"

if (( errors > 0 )); then
  fail "$errors wasm-bindgen / wasm-pack pin location(s) out of sync — bump them together."
fi

echo "OK: all wasm-bindgen / wasm-pack pins agree."
