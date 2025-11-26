# Adaptive Concurrency Simulations

This testing suite simulates different network conditions and client behavior patterns to test 
how the adaptive concurrency controller behavies.  It uses Docker containers with configurable 
network parameters (bandwidth, latency, congestion) and a mock server and clients to test how 
things behave.  The output for each test is a csv table showing a timeline of the controller 
behavior across a number of tests and a summary csv table showing basic test statistics.

## Basic Usage

```bash
# Run a single scenario with default network conditions
./run_scenarios.sh test_scenarios/sanity_check

# Run with specific network conditions
./run_scenarios.sh --bandwidth="10mbps,100mbps" --latency="10ms,50ms" --congestion=none test_scenarios/sanity_check

# Run multiple scenarios
./run_scenarios.sh scenarios/*

# Test across multiple bandwidths and latencies
./run_scenarios.sh \
  --bandwidth="10mbps,100mbps,1gbps" \
  --latency="10ms,50ms,100ms" \
  --congestion=none \
  test_scenarios/sanity_check

# Run with custom output directory and parallelism
./run_scenarios.sh \
  --out-dir=results/my_test \
  --max-parallel=8 \
  --bandwidth="10mbps,100mbps" \
  --latency="50ms" \
  scenarios/single_upload scenarios/gitxet_upload_burst

# Skip Docker build (faster if image already exists)
./run_scenarios.sh --no-build test_scenarios/sanity_check

# Run comprehensive simulation suite for xet stuff (all scenarios with full network matrix)
./run_full_xet_simulation.sh

# Run simplified simulation suite (all scenarios with reduced network matrix)
./run_simple_xet_simulation.sh
```

## Scenario Scripts

Scenario scripts are bash scripts located in the `scenarios/` or `test_scenarios/` directories. Each scenario:

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
- `test_scenarios/sanity_check`: Basic functionality test (short duration)
- `scenarios/single_upload`: Single client upload test
- `scenarios/gitxet_upload_burst`: Multiple simultaneous uploads
- `scenarios/added_uploads`: Staggered client starts
- `scenarios/download_single`: Single client download test
- `scenarios/download_multiple`: Multiple client download test
- `scenarios/flakey_server`: Server with congestion simulation

### Simulation Server (`simulation_server`)

An HTTP server that accepts POST requests to `/data` and simulates various server behaviors.

**Parameters:**
- `--min-reply-delay-ms=<ms>`: Minimum delay before responding (default: 0)
- `--max-reply-delay-ms=<ms>`: Maximum delay before responding (default: 0)
- `--test-duration-seconds=<seconds>`: Test duration in seconds. Server returns 503 after this time (default: 0, no limit)
- `--congested-bytes-per-second=<bytes>`: Congestion threshold in bytes per second (default: 0, disabled)
- `--min-congested-penalty-ms=<ms>`: Minimum delay penalty when congested (default: 0)
- `--max-congested-penalty-ms=<ms>`: Maximum delay penalty when congested (default: 0)
- `--congested-error-rate=<rate>`: Probability (0.0-1.0) of returning 504 error when congested (default: 0.0)

### Simulation Client (`simulation_client`)

A client that sends data uploads using the adaptive concurrency controller.

**Parameters:**
- `--min-data-kb=<kb>`: Minimum payload size in KB (default: 65536, which is 64MB)
- `--max-data-kb=<kb>`: Maximum payload size in KB (default: 65536, which is 64MB)
- `--repeat-duration-seconds=<seconds>`: How long to run the client (default: 120)
- `--server-addr=<addr>`: Server address (default: 127.0.0.1:8080)

**Behavior:**
- Randomly chooses payload size between `min-data-kb` and `max-data-kb` (in KB) for each upload
- Uses adaptive concurrency controller to manage concurrent uploads
- Reports metrics every 200ms including:
  - Bytes transmitted
  - Concurrency levels
  - Retry counts
  - Success ratios
  - Predicted bandwidth and RTT

**Output:**
- `client_parameters_<id>.json`: Client configuration parameters
- `client_stats_<id>.json`: JSON lines with client metrics every 200ms

## Run Scenarios Script (`run_scenarios.sh`)

Orchestrates running multiple scenarios with different network conditions in parallel.

**Parameters:**
- `--out-dir=<dir>`: Output directory for results (default: `results/`)
- `--no-build`: Skip Docker image build
- `--max-parallel=<n>`: Maximum number of parallel scenarios (default: 16)
- `--bandwidth=<list>`: Comma-separated list of bandwidths (e.g., `"10kbps,100kbps,1mbps,10mbps,100mbps,1gbps,10gbps"`)
- `--latency=<list>`: Comma-separated list of latencies (e.g., `"1ms,10ms,50ms,100ms,250ms"`)
- `--congestion=<list>`: Comma-separated list of congestion modes (e.g., `"none,realistic,heavy"`)

**Network Parameter Matrix:**
The script generates a matrix of all combinations of bandwidth, latency, and congestion parameters. For example:
- `--bandwidth="10mbps,100mbps" --latency="10ms,50ms" --congestion=none` creates 4 combinations
- Each scenario is run with each network combination
- Scenario names include network parameters: `scenario_name-bandwidth-latency-congestion`

**Usage:**
```bash
./run_scenarios.sh [options] <scenario1> [scenario2] ...
```

**Summary Output:**
- `summary.csv`: CSV file with one row per scenario, including:
  - `scenario_name`: Name with network parameters
  - `network_utilization_percent`: Average network utilization
  - `total_retries`: Total retries across all clients
  - `average_per_client_concurrency`: Average concurrency per client
  - `total_concurrency`: Total concurrency across all clients
  - `total_data_transmitted_bytes`: Total bytes successfully transmitted
  - `average_round_trip_time_ms`: Average RTT across all transmissions

**Per-Scenario Output Files:**

For each scenario run with a particular network configuration, the following output files are produced in the specified output directory:

- `client_parameters_<id>.json`: The configuration used for each client instance.
- `client_stats_<id>.json`: Client metrics recorded as JSON lines (one per interval, typically every 200ms).
- `server_stats_<id>.json`: (If enabled) Server metrics and statistics for monitoring server-side throughput and active connections.
- `network_trace_<id>.json` or `network_config_<id>.json`: (If enabled) Captures the emulated network parameters in effect for this scenario run.
- `server_log_<id>.txt` and/or `client_log_<id>.txt`: (If logging is enabled) Raw stdout/stderr logs for debugging (may be large).

Each `<id>` is unique per scenario/client/server instance, enabling correlation between files and parameters.

The output directory is organized as:
```
<out-dir>/
  scenario_name-bandwidth-latency-congestion/
    client_parameters_<id>.json
    client_stats_<id>.json
    server_stats_<id>.json
    network_trace_<id>.json
    server_log_<id>.txt
    client_log_<id>.txt
    ...
```
All files for each scenario run are placed together in a uniquely-named subdirectory for that network condition.

## Regenerating Summaries (`redo_summaries.sh`)

Utility script to regenerate all timeline and summary CSV files for an existing simulation directory. Useful when the reporting code has been updated or when CSV files need to be regenerated.

**Usage:**
```bash
./redo_summaries.sh <simulation_directory>
```

**Output:**
- Regenerates `timeline.csv` in each scenario subdirectory
- Regenerates `summary.csv` in the simulation directory
    
## Network Simulation

Network simulation is performed using Linux `tc` (traffic control) within Docker containers.

**Network Parameters:**
- **Bandwidth**: Rate limiting (e.g., `10kbps`, `100kbps`, `1mbps`, `10mbps`, `100mbps`, `1gbps`, `10gbps`)
- **Latency**: Added delay (e.g., `1ms`, `10ms`, `50ms`, `100ms`, `250ms`)
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
└── <output_dir>/              # e.g., results/ or results/my_test
    └── <timestamp>/            # e.g., 20251118_144714
        ├── summary.csv         # CSV summary of all scenarios
        └── <scenario_name>/     # e.g., sanity_check-100mbps-50ms-none
            ├── run.log          # Execution log
            ├── server_parameters.json
            ├── server_stats.json
            ├── network_stats.json
            ├── client_parameters_<id>.json
            ├── client_stats_<id>.json
            ├── timeline.csv     # Detailed timeline (generated by generate_scenario_report)
            └── summary.json     # Legacy summary (if generated)
```

## Report Format

### Timeline CSV (`timeline.csv`)

Generated by `generate_scenario_report` for each scenario. Contains metrics at 250ms intervals:

**Per-Client Columns:**
- `client_XX_bytes`: Total data transmitted successfully
- `client_XX_concurrency`: Current concurrency level
- `client_XX_retries`: Number of retries
- `client_XX_success_ratio`: Success ratio from concurrency controller
- `client_XX_predicted_bandwidth`: Predicted bandwidth (bytes/sec)
- `client_XX_predicted_max_rtt`: Predicted maximum RTT (seconds)
- `client_XX_predicted_max_rtt_standard_error`: Standard error of RTT prediction

**Summary Columns:**
- `total_bytes`: Sum of bytes across all clients
- `total_retries`: Sum of retries across all clients
- `total_concurrency`: Sum of concurrency across all clients
- `average_concurrency`: Average concurrency across active clients

### Summary CSV (`summary.csv`)

Generated by `generate_summary` for all scenarios in a timestamp directory. Contains one row per scenario:

**Columns:**
- `scenario_name`: Scenario name with network parameters
- `network_utilization_percent`: Average network utilization
- `total_retries`: Total retries across all clients
- `average_per_client_concurrency`: Average concurrency per client
- `total_concurrency`: Total concurrency across all clients
- `total_data_transmitted_bytes`: Total bytes successfully transmitted
- `average_round_trip_time_ms`: Average round trip time
