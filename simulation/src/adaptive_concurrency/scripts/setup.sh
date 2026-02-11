#!/bin/bash

# Common setup for adaptive-concurrency simulation scripts.
# Source this from each runner script: source "$(dirname "$0")/setup.sh"

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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
