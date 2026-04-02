#!/usr/bin/env bash
set -euo pipefail

RAVEN="raven.local"
REMOTE_DIR="~/workspace/hf/xet-core"
REMOTE_ENV=". ~/.cargo/env && export TMPDIR=~/tmp"

echo "=== Syncing to raven ==="
rsync -az --delete \
    --exclude target/ \
    --exclude .git/ \
    --exclude '*.pyc' \
    --exclude __pycache__/ \
    . "${RAVEN}:${REMOTE_DIR}/"

CMD="${1:-test}"

case "$CMD" in
  test)
    echo "=== Running cargo test on raven ==="
    ssh "$RAVEN" "${REMOTE_ENV} && cd ${REMOTE_DIR} && cargo test -p xet-data --lib data_writer 2>&1"
    ;;
  check)
    echo "=== Running cargo check on raven ==="
    ssh "$RAVEN" "${REMOTE_ENV} && cd ${REMOTE_DIR} && cargo check 2>&1"
    ;;
  clippy)
    echo "=== Running clippy on raven ==="
    ssh "$RAVEN" "${REMOTE_ENV} && cd ${REMOTE_DIR} && cargo clippy -r --verbose -- -D warnings 2>&1"
    ;;
  fmt)
    echo "=== Running fmt check on raven ==="
    ssh "$RAVEN" "${REMOTE_ENV} && cd ${REMOTE_DIR} && cargo +nightly fmt -- --check 2>&1"
    ;;
  bench)
    echo "=== Running reconstruction benchmark on raven ==="
    ssh "$RAVEN" "${REMOTE_ENV} && cd ${REMOTE_DIR} && cargo bench -p xet-data --bench reconstruction_bench 2>&1"
    ;;
  full)
    echo "=== Running full verification on raven ==="
    ssh "$RAVEN" "${REMOTE_ENV} && cd ${REMOTE_DIR} && cargo +nightly fmt -- --check && cargo clippy -r --verbose -- -D warnings && cargo test -p xet-data --lib data_writer 2>&1"
    ;;
  *)
    echo "Usage: $0 {test|check|clippy|fmt|bench|full}"
    exit 1
    ;;
esac
