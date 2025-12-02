#!/bin/bash

set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
PROJECT_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
TEST_DIR="cas_client/test/adaptive_concurrency"
DOCKER_IMAGE_NAME="xet-adaptive-concurrency-test"

print_status() { >&2 echo "[INFO] $1"; }
print_success() { >&2 echo "[SUCCESS] $1"; }
print_warning() { >&2 echo "[WARNING] $1"; }
print_error() { >&2 echo "[ERROR] $1"; }

# Check if simulation directory is provided
if [ $# -lt 1 ]; then
    print_error "Usage: $0 <simulation_directory>"
    print_error "  Example: $0 results/full_xet_simulation/20251118_232850"
    exit 1
fi

SIMULATION_DIR="$1"

# Convert to absolute path
if [ ! -d "$SIMULATION_DIR" ]; then
    print_error "Directory does not exist: $SIMULATION_DIR"
    exit 1
fi

SIMULATION_DIR="$(cd "$SIMULATION_DIR" && pwd)"

print_status "Regenerating summaries for: $SIMULATION_DIR"

# Find all scenario subdirectories
SCENARIO_DIRS=()
for entry in "$SIMULATION_DIR"/*; do
    if [ -d "$entry" ]; then
        SCENARIO_DIRS+=("$entry")
    fi
done

if [ ${#SCENARIO_DIRS[@]} -eq 0 ]; then
    print_error "No scenario directories found in $SIMULATION_DIR"
    exit 1
fi

print_status "Found ${#SCENARIO_DIRS[@]} scenario directories"

# Try to run on host first (for native builds)
USE_DOCKER=false

# Check if we can run on host
if command -v generate_scenario_report >/dev/null 2>&1 && command -v generate_summary >/dev/null 2>&1; then
    print_status "Using binaries from PATH"
    USE_DOCKER=false
elif [ -f "$PROJECT_ROOT/target/opt-test/generate_scenario_report" ] && [ -x "$PROJECT_ROOT/target/opt-test/generate_scenario_report" ] \
     && [ -f "$PROJECT_ROOT/target/opt-test/generate_summary" ] && [ -x "$PROJECT_ROOT/target/opt-test/generate_summary" ]; then
    print_status "Using binaries from target/opt-test"
    USE_DOCKER=false
else
    print_status "Binaries not found on host, will use Docker"
    USE_DOCKER=true
fi

if [ "$USE_DOCKER" = false ]; then
    # Run on host
    print_status "Generating timelines on host..."
    FAILED_SCENARIOS=0
    for scenario_dir in "${SCENARIO_DIRS[@]}"; do
        scenario_name="$(basename "$scenario_dir")"
        print_status "Generating timeline.csv for: $scenario_name"
        if command -v generate_scenario_report >/dev/null 2>&1; then
            (cd "$scenario_dir" && generate_scenario_report .) || ((FAILED_SCENARIOS++))
        else
            (cd "$scenario_dir" && "$PROJECT_ROOT/target/opt-test/generate_scenario_report" .) || ((FAILED_SCENARIOS++))
        fi
    done
    
    if [ $FAILED_SCENARIOS -gt 0 ]; then
        print_warning "$FAILED_SCENARIOS scenario(s) failed to generate timelines, but continuing..."
    fi
    
    print_status "Generating summary.csv on host..."
    if command -v generate_summary >/dev/null 2>&1; then
        (cd "$SIMULATION_DIR" && generate_summary .) || {
            print_error "Failed to generate summary.csv"
            exit 1
        }
    else
        (cd "$SIMULATION_DIR" && "$PROJECT_ROOT/target/opt-test/generate_summary" .) || {
            print_error "Failed to generate summary.csv"
            exit 1
        }
    fi
else
    # Run in Docker - build once, run all operations
    print_status "Building binaries and running all operations in Docker..."
    
    # Build list of scenario directory names (relative to simulation directory)
    SCENARIO_NAMES=()
    for scenario_dir in "${SCENARIO_DIRS[@]}"; do
        SCENARIO_NAMES+=("$(basename "$scenario_dir")")
    done
    
    # Create a script to run inside Docker
    # Build the script content with scenario names
    DOCKER_SCRIPT="set -e
cd /workspace
export PATH=\"/workspace/target/docker_linux/opt-test:\$PATH\"

# Build both binaries once
print_status() { echo \"[INFO] \$1\" >&2; }
print_success() { echo \"[SUCCESS] \$1\" >&2; }
print_warning() { echo \"[WARNING] \$1\" >&2; }

print_status \"Building generate_scenario_report and generate_summary...\"
CARGO_TARGET_DIR=target/docker_linux cargo build --profile=opt-test --bin generate_scenario_report --bin generate_summary

# Generate timeline.csv for each scenario
cd /simulation
FAILED_SCENARIOS=0
set +e"

    # Add commands for each scenario
    for scenario_name in "${SCENARIO_NAMES[@]}"; do
        DOCKER_SCRIPT="$DOCKER_SCRIPT
print_status \"Generating timeline.csv for: $scenario_name\"
if cd \"$scenario_name\" 2>/dev/null; then
    # Check if client_stats files exist before running
    if ls client_stats_*.json 1>/dev/null 2>&1; then
        (generate_scenario_report . 2>&1) || {
            print_warning \"Failed to generate timeline for $scenario_name\"
            ((FAILED_SCENARIOS++))
        }
    else
        print_warning \"No client_stats files found in $scenario_name, skipping\"
        ((FAILED_SCENARIOS++))
    fi
    cd /simulation
else
    print_warning \"Scenario directory $scenario_name not found\"
    ((FAILED_SCENARIOS++))
fi"
    done

    DOCKER_SCRIPT="$DOCKER_SCRIPT

set -e
if [ \$FAILED_SCENARIOS -gt 0 ]; then
    print_warning \"\$FAILED_SCENARIOS scenario(s) failed to generate timelines, but continuing...\"
fi

# Generate summary.csv
print_status \"Generating summary.csv...\"
cd /simulation
generate_summary .

print_success \"All summaries regenerated successfully!\""
    
    # Run the script in Docker
    if docker run --rm \
        -v "$PROJECT_ROOT:/workspace" \
        -v "$SIMULATION_DIR:/simulation" \
        "$DOCKER_IMAGE_NAME" \
        bash -c "$DOCKER_SCRIPT"; then
        print_success "All operations completed in Docker"
    else
        print_error "Failed to complete operations in Docker"
        exit 1
    fi
fi

print_success "All summaries regenerated successfully!"
print_success "Results in: $SIMULATION_DIR"

