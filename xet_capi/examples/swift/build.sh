#!/usr/bin/env bash
# Build the Swift example against the hf_xet C API.
#
#   ./build.sh          # build ./upload_download
#   ./build.sh run      # build and run (requires $HF_TOKEN)
set -euo pipefail

cd "$(dirname "$0")"
LIBDIR="../../../target/release"

# -I CHfXet   : directory holding module.modulemap (the CHfXet module)
# -L/-lxet_capi + rpath : link the self-contained shared library
swiftc main.swift \
  -I CHfXet \
  -L "$LIBDIR" -lxet_capi \
  -Xlinker -rpath -Xlinker "$LIBDIR" \
  -o upload_download

if [ "${1:-}" = "run" ]; then
  ./upload_download
fi
