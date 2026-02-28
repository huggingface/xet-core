#!/bin/bash

# Run the full XET scenario matrix across bandwidth, latency, and congestion dimensions.

source "$(dirname "$0")/setup.sh"

"$BIN" \
    --scenario="single_upload,gitxet_upload_burst,added_uploads" \
    --out-dir="$RESULTS_DIR" \
    --bandwidth="10mbps,100mbps,1gbps,10gbps" \
    --latency="20ms,250ms" \
    --congestion="none,medium,heavy" \
    --server-latency-profile="realistic" \
    --max-parallel="${MAX_PARALLEL:-4}" \
    "$@"
