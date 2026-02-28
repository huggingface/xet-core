#!/bin/bash

# Run sanity-check scenarios with minimal parameters.

source "$(dirname "$0")/setup.sh"

"$BIN" \
    --scenario=sanity_check \
    --out-dir="$RESULTS_DIR" \
    --bandwidth="10mbps,1gbps" \
    --latency="20ms" \
    --congestion="none" \
    --server-latency-profile="realistic,none" \
    --max-parallel="${MAX_PARALLEL:-4}" \
    "$@"
