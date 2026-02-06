#!/bin/bash

# Run full XET scenario matrix (same as cas_client/tests/adaptive_concurrency/run_full_xet_simulation.sh).
# Uses script location to find target/release and writes to adaptive_concurrency/results/.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# From simulation/src/adaptive_concurrency/scripts -> repo root
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../../" && pwd)"
BIN="$REPO_ROOT/target/release/run_ac_simulation"
RESULTS_DIR="$(cd "$SCRIPT_DIR/../" && pwd)/results"

SCENARIO_BIN="$REPO_ROOT/target/release/run_ac_scenario"
if [ ! -x "$BIN" ]; then
    echo "Error: run_ac_simulation not found or not executable at $BIN" >&2
    echo "Build with: cargo build --release -p simulation" >&2
    exit 1
fi
if [ ! -x "$SCENARIO_BIN" ]; then
    echo "Error: run_ac_scenario not found at $SCENARIO_BIN (required by run_ac_simulation)" >&2
    echo "Build with: cargo build --release -p simulation" >&2
    exit 1
fi

mkdir -p "$RESULTS_DIR"

# Same matrix as run_full_xet_simulation.sh: bandwidth, latency, congestion.
# "realistic" (varying latency/bandwidth over time) is not supported; would require
# a background profile-update loop and is dropped for now.
# Optional: set MAX_PARALLEL (default 1) to run scenarios in parallel
"$BIN" \
    --scenario=single_upload \
    --out-dir="$RESULTS_DIR" \
    --bandwidth="10mbps,50mbps,100mbps,1gbps,10gbps" \
    --latency="20ms,250ms" \
    --congestion="none,medium,heavy" \
    --max-parallel="${MAX_PARALLEL:-4}" \
    "$@"
