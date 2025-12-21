#!/bin/bash

# Run XET simulation with comprehensive test matrix
# This script runs all non-test scenarios with various network conditions

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Get the path to run_scenarios.sh
RUN_SCENARIOS_SCRIPT="$SCRIPT_DIR/run_scenarios.sh"

if [ ! -f "$RUN_SCENARIOS_SCRIPT" ]; then
    echo "Error: run_scenarios.sh not found at $RUN_SCENARIOS_SCRIPT" >&2
    exit 1
fi

# Run all scenarios from scenarios/ directory (non-test scenarios)
# with the specified network conditions
"$RUN_SCENARIOS_SCRIPT" \
    --out-dir="results/sanity_check" \
    --bandwidth="100mbps,10gbps" \
    --latency="50ms" \
    --congestion="realistic" \
    test_scenarios/sanity_check

