# Adaptive Concurrency Test Suite

This test suite provides comprehensive testing for the adaptive concurrency controller using Docker containers and network simulation. It tests the adaptive concurrency system under various network conditions and server configurations.

## Overview

The test suite consists of:

- **Simulation Server**: An HTTP server that accepts data uploads and simulates network/server conditions
- **Simulation Client**: A client that sends data uploads using the adaptive concurrency controller
- **Docker Environment**: Containerized test environment with network simulation capabilities using `tc` (traffic control)
- **Scenario Scripts**: Predefined test scenarios with different network conditions and server parameters
- **Run Script**: Orchestrates the entire test process, running multiple scenarios in parallel
- **Report Generator**: Creates summary reports and CSV files with aggregated statistics

## Components

### Simulation Server (`simulation_server.rs`)

An HTTP server that accepts POST requests to `/data` and simulates various server behaviors.

**Parameters:**
- `--min-reply-delay-ms=<ms>`: Minimum delay before responding (default: 0)
- `--max-reply-delay-ms=<ms>`: Maximum delay before responding (default: 0)
- `--test-duration-seconds=<seconds>`: Test duration in seconds. Server returns 503 after this time (default: 0, no limit)
- `--congested-bytes-per-second=<bytes>`: Congestion threshold in bytes per second (default: 0, disabled)
- `--min-congested-penalty-ms=<ms>`: Minimum delay penalty when congested (default: 0)
- `--max-congested-penalty-ms=<ms>`: Maximum delay penalty when congested (default: 0)
- `--congested-error-rate=<rate>`: Probability (0.0-1.0) of returning 504 error when congested (default: 0.0)

**Congestion Simulation:**
- Tracks bytes per second using a sliding window counter
- Decreases counter by 1/10th of capacity every 100ms (minimum 0)
- When counter exceeds threshold:
  - Returns 504 Gateway Timeout with probability `congested_error_rate`
  - Otherwise applies random delay penalty between `min-congested-penalty-ms` and `max-congested-penalty-ms`

**Output:**
- `server_parameters.json`: Server configuration parameters
- `server_stats.json`: JSON lines with server metrics every 200ms

### Simulation Client (`simulation_client.rs`)

A client that sends data uploads using the adaptive concurrency controller.

**Parameters:**
- `--min-data-kb=<kb>`: Minimum payload size in KB (default: 65536, which is 64MB)
- `--max-data-kb=<kb>`: Maximum payload size in KB (default: 65536, which is 64MB)
- `--repeat-duration-seconds=<seconds>`: How long to run the client (default: 120)
- `--server-addr=<addr>`: Server address (default: 127.0.0.1:8080)

**Behavior:**
- Randomly chooses payload size between `min-data-kb` and `max-data-kb` (in KB) for each upload
- Internally converts KB to bytes (multiplies by 1024) for the actual payload
- Uses adaptive concurrency controller to manage concurrent uploads
- Reports metrics every 200ms

**Output:**
- `client_parameters_<id>.json`: Client configuration parameters
- `client_stats_<id>.json`: JSON lines with client metrics every 200ms

### Run Scenarios Script (`run_scenarios.sh`)

Orchestrates running multiple scenarios with different network conditions in parallel.

**Parameters:**
- `--out-dir=<dir>`: Output directory for results (default: `results/`)
- `--no-build`: Skip Docker image build
- `--max-parallel=<n>`: Maximum number of parallel scenarios (default: 16)
- `--bandwidth=<list>`: Comma-separated list of bandwidths (e.g., `"10kbps,100kbps,1mbps"`)
- `--latency=<list>`: Comma-separated list of latencies (e.g., `"10ms,50ms,100ms"`)
- `--congestion=<list>`: Comma-separated list of congestion modes (e.g., `"none,realistic,heavy"`)

**Usage:**
```bash
./run_scenarios.sh [options] <scenario1> [scenario2] ...
```

**Examples:**
```bash
# Run a single scenario with default network conditions
./run_scenarios.sh scenarios/sanity_check

# Run with specific network conditions
./run_scenarios.sh --bandwidth="100kbps,10mbps" --latency="1ms,10ms" --congestion="none" scenarios/sanity_check

# Run multiple scenarios
./run_scenarios.sh --out-dir=results/my_test scenarios/*

# Run with custom output directory
./run_scenarios.sh --out-dir=results/sim_run_01 scenarios/sanity_check scenarios/gitxet_upload_burst
```

**Progress Tracking:**
- Displays progress counter: "Running scenario X/Y: ..."
- Shows completion status: "Completed X/Y: ..." or "Failed X/Y: ..."

### Report Generator (`generate_report.rs`)

Generates summary reports from simulation results.

**Usage:**
```bash
# Generate report for a single scenario
generate_report <scenario_directory>

# Generate CSV summary for all scenarios in a timestamp directory
generate_report --summary-mode <timestamp_directory>
```

**Output:**
- `summary.json`: Summary statistics in JSON format
- `report.txt`: Human-readable text report
- `summary.csv`: CSV file with all scenarios (when using `--summary-mode`)

## Scenario Scripts

Scenario scripts are bash scripts located in the `scenarios/` directory. Each scenario:

1. Starts the simulation server with specific parameters
2. Runs one or more simulation clients with specific parameters
3. Waits for the test duration to complete

**Example Scenario Structure:**
```bash
#!/bin/bash

# Start the server
simulation_server --min-reply-delay-ms=10 --max-reply-delay-ms=100 --test-duration-seconds=600 &

echo "Testing basic 48MB to 64MB payload uploads for 10 minutes..."
simulation_client --min-data-kb=49152 --max-data-kb=65536 --repeat-duration-seconds=600 &

sleep 600

echo "Scenario completed"
```

**Available Scenarios:**
- `sanity_check`: Basic functionality test
- `single_upload`: Single client upload test
- `gitxet_upload_burst`: Multiple simultaneous uploads
- `added_uploads`: Staggered client starts
- `download_single`: Single client download test
- `download_multiple`: Multiple client download test
- `flakey_server`: Server with congestion simulation

## Network Simulation

Network simulation is performed using Linux `tc` (traffic control) within Docker containers.

**Network Parameters:**
- **Bandwidth**: Rate limiting (e.g., `10kbps`, `100kbps`, `1mbps`, `10mbps`)
- **Latency**: Added delay (e.g., `1ms`, `10ms`, `50ms`, `100ms`, `200ms`)
- **Congestion**: Congestion mode (`none`, `realistic`, `heavy`)

**Setup Script:**
The `scripts/setup_network` script configures network conditions:
```bash
setup_network --latency=<ms> --mbps=<bandwidth> --congestion=<mode>
```

## Output Structure

Results are organized in a timestamped directory structure:

```
results/
└── <output_dir>/
    └── <timestamp>/              # e.g., 20251105_144714
        ├── summary.csv            # CSV summary of all scenarios
        └── <scenario_name>/       # e.g., sanity_check-100kbps-1ms-none
            ├── run.log            # Execution log
            ├── server_parameters.json
            ├── server_stats.json
            ├── client_parameters_<id>.json
            ├── client_stats_<id>.json
            ├── network_stats.json
            ├── summary.json       # Summary statistics
            └── report.txt         # Human-readable report
```

## Report Format

### CSV Summary (`summary.csv`)

Columns (in order):
1. `scenario_name`: Name of the scenario
2. `total_duration`: Test duration in seconds
3. `network_utilization_percent`: Network utilization percentage
4. `total_retries`: Total retries
5. `average_max_concurrency`: Average max concurrency (concurrency factor from AdaptiveConcurrencyController)
6. `total_data_transmitted_bytes`: Total data transmitted
7. `average_round_trip_time_ms`: Average round trip time

### Text Report (`report.txt`)

Contains detailed statistics including:
- Performance summary (data transmitted, server calls, retries, RTT)
- Network utilization
- Average concurrent connections
- Network configuration
- Server parameters
- Client parameters

## Running Simulations

### Basic Usage

```bash
# Run a single scenario with default network conditions
./run_scenarios.sh scenarios/sanity_check

# Run with specific network conditions
./run_scenarios.sh --bandwidth="100kbps,10mbps" --latency="1ms,10ms" --congestion="none" scenarios/sanity_check

# Run multiple scenarios
./run_scenarios.sh scenarios/sanity_check scenarios/gitxet_upload_burst

# Run all scenarios in a directory
./run_scenarios.sh scenarios/*

# Custom output directory
./run_scenarios.sh --out-dir=results/my_test scenarios/sanity_check
```

### Advanced Usage

```bash
# Control parallelism
./run_scenarios.sh --max-parallel=8 scenarios/*

# Skip Docker build (if already built)
./run_scenarios.sh --no-build scenarios/sanity_check

# Combine all options
./run_scenarios.sh \
  --out-dir=results/comprehensive_test \
  --max-parallel=16 \
  --bandwidth="100kbps,1mbps,10mbps" \
  --latency="1ms,10ms,50ms" \
  --congestion="none,realistic" \
  scenarios/*
```

### Generating Reports

```bash
# Generate report for a single scenario
cd results/sim_run_01/20251105_144714/sanity_check-100kbps-1ms-none
generate_report .

# Generate CSV summary for all scenarios in a timestamp directory
cd results/sim_run_01/20251105_144714
generate_report --summary-mode .
```

## Requirements

- **Docker**: For containerized test environment
- **Linux host**: For network simulation (Docker must have `--cap-add=NET_ADMIN`)
- **Rust toolchain**: Installed in Docker container
- **Bash**: For running scenario scripts

## Building

The Docker image is built automatically when running `run_scenarios.sh`. To build manually:

```bash
cd scripts
./build_container.sh
```

## Troubleshooting

### Server fails to start
- Check if port 8080 is available
- Verify Docker container has proper permissions
- Check server logs in `run.log`

### Network simulation not working
- Ensure Docker container has `--cap-add=NET_ADMIN`
- Verify `tc` command is available in container
- Check if network rules are applied correctly in `network_stats.json`

### Client connection fails
- Verify server is running (check `run.log`)
- Check network connectivity
- Verify server address is correct
- Check if test duration has expired

### Results not generated
- Check if `generate_report` binary is available in PATH
- Verify results directory exists and contains required JSON files
- Check `run.log` for errors

### High values in reports
- Average concurrent connections and RTT are calculated using time-window approach (300ms windows)
- Only active clients within the time window are included in averages
- Values are averaged across time windows, not just summed

## Configuration

### Default Constants

The simulation uses full realistic constants (no scaling). The adaptive concurrency controller uses:
- Default retry delays and durations
- Default concurrency control parameters
- Default latency tracking and success tracking half-lives

### Test Duration

All scenarios are configured for 10-minute (600 second) test durations by default. Server test duration controls when the server stops accepting requests.

### Server Latencies

Server reply delays are set to realistic values:
- Minimum: 10ms (multiplied by 10 from original 1ms)
- Maximum: 100ms (multiplied by 10 from original 10ms)

### Congestion Simulation

When congestion is enabled:
- Counter tracks bytes per second
- Decreases by 1/10th of capacity every 100ms
- Applies penalty or returns error when threshold exceeded