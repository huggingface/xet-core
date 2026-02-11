#!/bin/bash

# Run a short XET scenario matrix with a reduced parameter sweep.

source "$(dirname "$0")/setup.sh"

"$BIN" \
    --scenario="single_upload,gitxet_upload_burst,added_uploads" \
    --out-dir="$RESULTS_DIR" \
    --bandwidth="100mbps,10gbps" \
    --latency="50ms" \
    --congestion="light" \
    --server-latency-profile="realistic" \
    --max-parallel="${MAX_PARALLEL:-8}" \
    "$@"
