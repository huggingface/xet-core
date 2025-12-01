#!/bin/bash

set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

# Default Configuration
DOCKER_IMAGE_NAME="xet-adaptive-concurrency-test"
DOCKER_CONTAINER_PREFIX="xet-test-container"
PROJECT_ROOT="/Users/hoytak/workspace/hf/xet-core-2"
TEST_DIR="cas_client/test/adaptive_concurrency"
DOCKER_DIR="$PROJECT_ROOT/$TEST_DIR/docker"
SCRIPTS_DIR="$PROJECT_ROOT/$TEST_DIR/scripts"
RESULTS_BASE_DIR="${RESULTS_BASE_DIR:-$PROJECT_ROOT/$TEST_DIR/results}"
MAX_PARALLEL=16

print_status() { >&2 echo "[INFO] $1"; }
print_success() { >&2 echo "[SUCCESS] $1"; }
print_warning() { >&2 echo "[WARNING] $1"; }
print_error() { >&2 echo "[ERROR] $1"; }

# Internal subcommand: run a single scenario (used by xargs parallelization)
if [[ "$1" == "--run-one" ]]; then
    shift
    combo_string="$1"; shift
    timestamp="$1"; shift || true
    scenario_index="$1"; shift || true
    total_scenarios="$1"; shift || true
    log_path="$1"

    # RESULTS_BASE_DIR and RUN_TIMESTAMP are set from environment (exported before xargs) or default
    # Use RUN_TIMESTAMP from environment if available, otherwise use passed timestamp for backward compatibility
    run_timestamp="${RUN_TIMESTAMP:-$timestamp}"

    # Parse the combination string
    IFS='|' read -r scenario_path bandwidth latency congestion combined_name <<< "$combo_string"

    # Derive log path if not provided
    if [[ -z "$log_path" ]]; then
        output_dir="$RESULTS_BASE_DIR/$run_timestamp/$combined_name"
        log_path="$output_dir/run.log"
    else
        output_dir="$RESULTS_BASE_DIR/$run_timestamp/$combined_name"
    fi

    # Ensure log directory exists
    mkdir -p "$(dirname "$log_path")"
    
    # Output scenario details to stderr with progress counter
    if [[ -n "$scenario_index" && -n "$total_scenarios" ]]; then
        echo "Running scenario $scenario_index/$total_scenarios: $combined_name ($bandwidth, $latency, $congestion) in $output_dir" >&2
    else
        echo "Running scenario: $combined_name ($bandwidth, $latency, $congestion) in $output_dir" >&2
    fi

    # Run scenario execution in a function
    run_scenario() {
        local container_name="${DOCKER_CONTAINER_PREFIX}_${combined_name}_${run_timestamp}"
        local output_dir="$RESULTS_BASE_DIR/$run_timestamp/$combined_name"

        print_status "Starting scenario: $combined_name (Container: $container_name)"
        print_status "Network: ${bandwidth}, ${latency}, ${congestion}"

        if [ ! -d "$output_dir" ]; then
            mkdir -p "$output_dir" || { print_error "Failed to create output directory: $output_dir"; return 1; }
        fi

        cleanup_container() {
            print_status "Cleaning up container $container_name..."
            docker stop "$container_name" >/dev/null 2>&1 || true
            docker rm "$container_name" >/dev/null 2>&1 || true
        }
        trap cleanup_container EXIT

        print_status "Starting Docker container for $combined_name..."
        docker run -d \
            --name "$container_name" \
            --cap-add=NET_ADMIN \
            --cpus="8.0" \
            -v "$PROJECT_ROOT:/workspace" \
            -v "$output_dir:/output" \
            "$DOCKER_IMAGE_NAME" \
            sleep infinity || { print_error "Failed to start Docker container $container_name"; return 1; }

        print_status "Copying network setup script for $combined_name..."
        docker cp "$SCRIPTS_DIR/setup_network" "$container_name:/usr/local/bin/setup_network" || { print_error "Failed to copy network setup script to $container_name"; cleanup_container; return 1; }

        print_status "Compiling test binaries for $combined_name..."
        docker exec "$container_name" bash -c "
            cd /workspace
            CARGO_TARGET_DIR=target/docker_linux cargo build --profile=opt-test --bin simulation_server --bin simulation_client --bin generate_scenario_report --bin generate_summary
        " || { print_error "Failed to compile test binaries for $combined_name"; cleanup_container; return 1; }

        print_status "Copying scenario script for $combined_name..."
        docker cp "$PROJECT_ROOT/$TEST_DIR/$scenario_path" "$container_name:/workspace/scenario.sh" || { print_error "Failed to copy scenario script to $container_name"; cleanup_container; return 1; }
        # Ensure the script has execute permissions and correct line endings
        docker exec "$container_name" bash -c "chmod +x /workspace/scenario.sh && sed -i 's/\r$//' /workspace/scenario.sh" || { print_error "Failed to set permissions on scenario script"; cleanup_container; return 1; }

        print_status "Setting up network for $combined_name: ${bandwidth}, ${latency}, ${congestion}..."
        
        # Parse bandwidth value and convert to Mbps
        # Supports: kbps, mbps, gbps (case-insensitive)
        local bandwidth_lower=$(echo "$bandwidth" | tr '[:upper:]' '[:lower:]')
        local bandwidth_num=$(echo "$bandwidth_lower" | sed 's/[^0-9.]//g')
        local bandwidth_unit=$(echo "$bandwidth_lower" | sed 's/[0-9.]//g')
        
        # Convert to Mbps
        local bandwidth_mbps
        case "$bandwidth_unit" in
            *kbps|*kb)
                # kbps to Mbps: divide by 1000
                bandwidth_mbps=$(echo "$bandwidth_num" | awk '{printf "%.2f", $1 / 1000}')
                ;;
            *mbps|*mb)
                # Already in Mbps
                bandwidth_mbps="$bandwidth_num"
                ;;
            *gbps|*gb)
                # Gbps to Mbps: multiply by 1000
                bandwidth_mbps=$(echo "$bandwidth_num" | awk '{printf "%.2f", $1 * 1000}')
                ;;
            *)
                # Default: assume Mbps if no unit specified
                bandwidth_mbps="$bandwidth_num"
                ;;
        esac
        
        # Parse latency value (remove units, assume ms)
        local latency_value=$(echo "$latency" | sed 's/[^0-9.]//g')
        
        docker exec "$container_name" bash -c "
            cd /workspace
            export PATH=\"/workspace/target/docker_linux/opt-test:$PATH\"
            setup_network --clear
            cd /output
            setup_network --latency='${latency_value}' --mbps='${bandwidth_mbps}' --congestion='${congestion}'
        " || { print_error "Failed to setup network for $combined_name"; cleanup_container; return 1; }

        print_status "Executing scenario script $combined_name..."
        docker exec "$container_name" bash -c "
            cd /workspace
            export PATH=\"/workspace/target/docker_linux/opt-test:\$PATH\"
            # Write directly to mounted output directory
            cd /output
            bash /workspace/scenario.sh
        " || { print_error "Scenario script $combined_name failed"; cleanup_container; return 1; }

        print_status "Results are written directly to output directory (no copying needed)"

        print_status "Generating report for $combined_name..."
        if docker exec "$container_name" bash -c "
            cd /output
            export PATH=\"/workspace/target/docker_linux/opt-test:$PATH\"
            generate_scenario_report .
            generate_summary .
        "; then
            print_success "Report generated successfully for $combined_name"
        else
            print_warning "Failed to generate report for $combined_name in container, trying on host..."
            if command -v generate_scenario_report >/dev/null 2>&1 && command -v generate_summary >/dev/null 2>&1; then
                (cd "$output_dir" && generate_scenario_report . && generate_summary .) && print_success "Reports generated on host for $combined_name" || print_warning "Could not generate reports for $combined_name"
            else
                print_warning "generate_scenario_report or generate_summary not available on host. Reports not generated for $combined_name"
            fi
        fi

        cleanup_container
        print_success "Scenario $combined_name completed successfully"
        return 0
    }

    # Execute the scenario with output redirected to log file
    if run_scenario >"$log_path" 2>&1; then
        if [[ -n "$scenario_index" && -n "$total_scenarios" ]]; then
            echo "Completed $scenario_index/$total_scenarios: $combined_name" >&2
        else
            echo "Completed: $combined_name" >&2
        fi
        exit 0
    else
        if [[ -n "$scenario_index" && -n "$total_scenarios" ]]; then
            echo "Failed $scenario_index/$total_scenarios: $combined_name" >&2
        else
            echo "Failed: $combined_name" >&2
        fi
        exit 1
    fi
fi

# Parse arguments
OUT_DIR="results"
SCENARIOS=()
BUILD_IMAGE=true
BANDWIDTHS=("100mbps")
LATENCIES=("1ms")
CONGESTIONS=("none")

while [[ $# -gt 0 ]]; do
    case $1 in
        --out-dir=*)
            OUT_DIR="${1#*=}"
            shift
            ;;
        --out-dir)
            OUT_DIR="$2"
            shift 2
            ;;
        --no-build)
            BUILD_IMAGE=false
            shift
            ;;
        --max-parallel=*)
            MAX_PARALLEL="${1#*=}"
            shift
            ;;
        --max-parallel)
            MAX_PARALLEL="$2"
            shift 2
            ;;
        --bandwidth=*)
            IFS=',' read -ra BANDWIDTHS <<< "${1#*=}"
            shift
            ;;
        --bandwidth)
            IFS=',' read -ra BANDWIDTHS <<< "$2"
            shift 2
            ;;
        --latency=*)
            IFS=',' read -ra LATENCIES <<< "${1#*=}"
            shift
            ;;
        --latency)
            IFS=',' read -ra LATENCIES <<< "$2"
            shift 2
            ;;
        --congestion=*)
            IFS=',' read -ra CONGESTIONS <<< "${1#*=}"
            shift
            ;;
        --congestion)
            IFS=',' read -ra CONGESTIONS <<< "$2"
            shift 2
            ;;
        -*)
            print_error "Unknown option: $1"
            print_error "Usage: $0 [--out-dir=<dir>] [--no-build] [--max-parallel=<n>] [--bandwidth=<list>] [--latency=<list>] [--congestion=<list>] <scenario1> [scenario2] ..."
            print_error "Example: $0 --out-dir=results/sim_1 scenarios/sanity_check"
            print_error "Example: $0 --bandwidth=\"10kbps,100kbps,1mbps\" --latency=\"10ms,50ms\" --congestion=none scenarios/sanity_check"
            print_error "Example: $0 --out-dir=results/sim_1 scenarios/*"
            exit 1
            ;;
        *)
            SCENARIOS+=("$1")
            shift
            ;;
    esac
done

# Check for scenario arguments
if [ ${#SCENARIOS[@]} -eq 0 ]; then
    print_error "Usage: $0 [--out-dir=<dir>] [--no-build] [--max-parallel=<n>] [--bandwidth=<list>] [--latency=<list>] [--congestion=<list>] <scenario1> [scenario2] ..."
    print_error "Example: $0 --out-dir=results/sim_1 scenarios/sanity_check"
    print_error "Example: $0 --bandwidth=\"10kbps,100kbps,1mbps\" --latency=\"10ms,50ms\" --congestion=none scenarios/sanity_check"
    print_error "Example: $0 --out-dir=results/sim_1 scenarios/*"
    exit 1
fi

# Normalize output directory path
if [[ "$OUT_DIR" = /* ]]; then
    RESULTS_BASE_DIR="$OUT_DIR"
else
    RESULTS_BASE_DIR="$PROJECT_ROOT/$TEST_DIR/$OUT_DIR"
fi

print_status "Starting simulation with ${#SCENARIOS[@]} scenario(s)..."
print_status "Results directory: $RESULTS_BASE_DIR"
print_status "Maximum parallel scenarios: $MAX_PARALLEL"

# Step 1: Build Docker image if needed
if [ "$BUILD_IMAGE" = true ]; then
    print_status "Building Docker image..."
    cd "$DOCKER_DIR"
    docker build -t "$DOCKER_IMAGE_NAME" . || {
        print_error "Failed to build Docker image"
        exit 1
    }
    print_success "Docker image $DOCKER_IMAGE_NAME built successfully."
else
    # Check if image exists
    if ! docker image inspect "$DOCKER_IMAGE_NAME" >/dev/null 2>&1; then
        print_error "Docker image $DOCKER_IMAGE_NAME not found. Please build it first or remove --no-build flag."
        exit 1
    fi
    print_status "Using existing Docker image: $DOCKER_IMAGE_NAME"
fi

# Step 2: Generate network parameter matrix
print_status "Generating network parameter matrix..."
print_status "Bandwidths: ${BANDWIDTHS[*]}"
print_status "Latencies: ${LATENCIES[*]}"
print_status "Congestions: ${CONGESTIONS[*]}"

# Create matrix of network parameters
NETWORK_COMBINATIONS=()
for congestion in "${CONGESTIONS[@]}"; do
    for latency in "${LATENCIES[@]}"; do
        for bandwidth in "${BANDWIDTHS[@]}"; do
            NETWORK_COMBINATIONS+=("${bandwidth}|${latency}|${congestion}")
        done
    done
done

print_status "Generated ${#NETWORK_COMBINATIONS[@]} network combinations"

# Step 3: Validate scenario scripts
print_status "Validating scenario scripts..."
VALID_SCENARIOS=()
for scenario in "${SCENARIOS[@]}"; do
    # Handle glob patterns
    if [[ "$scenario" == *"*"* ]]; then
        for expanded in $scenario; do
            if [ -f "$PROJECT_ROOT/$TEST_DIR/$expanded" ]; then
                VALID_SCENARIOS+=("$expanded")
            else
                print_warning "Scenario not found: $expanded"
            fi
        done
    else
        if [ -f "$PROJECT_ROOT/$TEST_DIR/$scenario" ]; then
            VALID_SCENARIOS+=("$scenario")
        else
            print_warning "Scenario not found: $scenario"
        fi
    fi
done

if [ ${#VALID_SCENARIOS[@]} -eq 0 ]; then
    print_error "No valid scenario scripts found"
    exit 1
fi

print_status "Found ${#VALID_SCENARIOS[@]} valid scenario(s) to run:"
for scenario in "${VALID_SCENARIOS[@]}"; do
    print_status "  - $scenario"
done

# Step 4: Generate scenario-network combinations
print_status "Generating scenario-network combinations..."
COMBINED_SCENARIOS=()
for scenario in "${VALID_SCENARIOS[@]}"; do
    for network_combo in "${NETWORK_COMBINATIONS[@]}"; do
        IFS='|' read -r bandwidth latency congestion <<< "$network_combo"
        scenario_name=$(basename "$scenario")
        combined_name="${scenario_name}-${bandwidth}-${latency}-${congestion}"
        COMBINED_SCENARIOS+=("${scenario}|${bandwidth}|${latency}|${congestion}|${combined_name}")
    done
done

print_status "Generated ${#COMBINED_SCENARIOS[@]} total combinations:"
for combo in "${COMBINED_SCENARIOS[@]}"; do
    IFS='|' read -r scenario bandwidth latency congestion combined_name <<< "$combo"
    print_status "  - $combined_name (${bandwidth}, ${latency}, ${congestion})"
done

# Step 5: Run scenarios in parallel via xargs
print_status "Scheduling scenarios with xargs..."

# Generate a single timestamp for this run
RUN_TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

JOBS_FILE="$(mktemp)"
trap 'rm -f "$JOBS_FILE"' EXIT

# Calculate total number of scenarios
TOTAL_SCENARIOS=${#COMBINED_SCENARIOS[@]}

# Generate jobs file with index and total
scenario_index=1
for combo in "${COMBINED_SCENARIOS[@]}"; do
    printf "%s\t%s\t%s\t%s\n" "$combo" "$RUN_TIMESTAMP" "$scenario_index" "$TOTAL_SCENARIOS" >> "$JOBS_FILE"
    ((scenario_index++))
done

# Pass arguments (scenario, timestamp, index, total) to this script's --run-one
# Export RESULTS_BASE_DIR and RUN_TIMESTAMP so --run-one can use it
export RESULTS_BASE_DIR
export RUN_TIMESTAMP
# Get the absolute path to this script - use $0 which should work when script is executed directly
# Capture exit status but continue even if some scenarios fail
set +e  # Temporarily disable exit on error
xargs -P "$MAX_PARALLEL" -n 4 -- "$SCRIPT_PATH" --run-one < "$JOBS_FILE"
XARGS_EXIT=$?
set -e  # Re-enable exit on error

if [ $XARGS_EXIT -ne 0 ]; then
    print_warning "Some scenarios failed (exit code: $XARGS_EXIT), but continuing to generate report..."
fi

# Generate CSV summary for all scenarios in the timestamp directory
print_status "Generating CSV summary for all scenarios..."

# Try to run generate_summary
SUMMARY_GENERATED=false

# First, try running it directly on the host (for native builds)
if command -v generate_summary >/dev/null 2>&1; then
    print_status "Using generate_summary from PATH"
    (cd "$RESULTS_BASE_DIR/$RUN_TIMESTAMP" && generate_summary .) && SUMMARY_GENERATED=true
elif [ -f "$PROJECT_ROOT/target/opt-test/generate_summary" ] && [ -x "$PROJECT_ROOT/target/opt-test/generate_summary" ]; then
    print_status "Using generate_summary from target/opt-test"
    (cd "$RESULTS_BASE_DIR/$RUN_TIMESTAMP" && "$PROJECT_ROOT/target/opt-test/generate_summary" .) && SUMMARY_GENERATED=true
fi

# If that didn't work, run it in Docker (containers are cleaned up after scenarios, so create a new one)
if [ "$SUMMARY_GENERATED" = false ]; then
    print_status "Running generate_summary in a temporary Docker container..."
    if docker run --rm \
        -v "$PROJECT_ROOT:/workspace" \
        -v "$RESULTS_BASE_DIR/$RUN_TIMESTAMP:/output" \
        "$DOCKER_IMAGE_NAME" \
        bash -c "
            cd /workspace
            export PATH=\"/workspace/target/docker_linux/opt-test:\$PATH\"
            if [ ! -f \"/workspace/target/docker_linux/opt-test/generate_summary\" ]; then
                CARGO_TARGET_DIR=target/docker_linux cargo build --profile=opt-test --bin generate_summary
            fi
            cd /output
            generate_summary .
        "; then
        SUMMARY_GENERATED=true
        print_success "CSV summary generated successfully using temporary container"
    else
        print_warning "Failed to generate summary in Docker container"
    fi
fi

if [ "$SUMMARY_GENERATED" = true ]; then
    if [ -f "$RESULTS_BASE_DIR/$RUN_TIMESTAMP/summary.csv" ]; then
        print_success "CSV summary generated successfully at $RESULTS_BASE_DIR/$RUN_TIMESTAMP/summary.csv"
    else
        print_warning "generate_summary reported success but summary.csv not found"
    fi
else
    print_warning "Could not generate CSV summary - generate_summary binary not available or failed"
fi

print_success "All scenarios completed!"
print_success "Results saved to: $RESULTS_BASE_DIR/$RUN_TIMESTAMP"
