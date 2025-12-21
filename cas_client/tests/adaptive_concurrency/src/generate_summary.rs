#![cfg(not(target_family = "wasm"))]

use std::fs;
use std::path::Path;

use clap::Parser;
use serde::Deserialize;

mod common;
use common::{ClientMetrics, NetworkStats, ServerSimulationParameters};

fn load_json_lines<T>(file_path: &Path) -> Result<Vec<T>, Box<dyn std::error::Error>>
where
    T: for<'de> Deserialize<'de>,
{
    let content = fs::read_to_string(file_path)?;
    let mut results = Vec::new();

    for line in content.lines() {
        if !line.trim().is_empty() {
            let item: T = serde_json::from_str(line)?;
            results.push(item);
        }
    }

    Ok(results)
}

fn load_single_json<T>(file_path: &Path) -> Result<T, Box<dyn std::error::Error>>
where
    T: for<'de> Deserialize<'de>,
{
    let content = fs::read_to_string(file_path)?;
    let item: T = serde_json::from_str(&content)?;
    Ok(item)
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Results directory (timestamp directory containing multiple scenario subdirectories)
    results_directory: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let results_dir = Path::new(&args.results_directory);
    if !results_dir.exists() {
        eprintln!("Error: Directory {} does not exist", results_dir.display());
        std::process::exit(1);
    }

    generate_summary_mode(results_dir)
}

fn generate_summary_mode(results_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // Find all scenario subdirectories
    let mut scenario_dirs = Vec::new();
    for entry in fs::read_dir(results_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            // Check if it has timeline.csv
            let timeline_path = path.join("timeline.csv");
            if timeline_path.exists() {
                scenario_dirs.push(path);
            }
        }
    }

    if scenario_dirs.is_empty() {
        eprintln!("Error: No scenario directories with timeline.csv found in {}", results_dir.display());
        std::process::exit(1);
    }

    // Process each scenario's timeline.csv
    let mut scenario_data = Vec::new();
    for scenario_dir in &scenario_dirs {
        let scenario_name = scenario_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        let timeline_path = scenario_dir.join("timeline.csv");
        match process_timeline_csv(&timeline_path, scenario_dir, &scenario_name) {
            Ok(data) => scenario_data.push(data),
            Err(e) => {
                eprintln!("Warning: Failed to process timeline.csv for {}: {}", scenario_name, e);
            },
        }
    }

    if scenario_data.is_empty() {
        eprintln!("Error: No valid timeline.csv files found");
        std::process::exit(1);
    }

    // Generate summary CSV
    generate_summary_csv(results_dir, &scenario_data)?;

    Ok(())
}

#[derive(Debug, Clone)]
struct ScenarioSummary {
    scenario_name: String,
    network_utilization_percent: f64,
    total_retries: u64,
    average_per_client_concurrency: f64,
    total_concurrency: f64,
    total_data_transmitted_bytes: f64,
    average_round_trip_time_ms: f64,
}

fn process_timeline_csv(
    timeline_path: &Path,
    scenario_dir: &Path,
    scenario_name: &str,
) -> Result<ScenarioSummary, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(timeline_path)?;
    let lines: Vec<&str> = content.lines().collect();

    if lines.is_empty() {
        return Err("Empty timeline.csv file".into());
    }

    // Parse header to find column indices
    let header = lines[0];
    let header_cols: Vec<&str> = header.split(',').collect();

    let total_bytes_idx = header_cols
        .iter()
        .position(|&s| s == "total_bytes")
        .ok_or("total_bytes column not found")?;
    let total_retries_idx = header_cols
        .iter()
        .position(|&s| s == "total_retries")
        .ok_or("total_retries column not found")?;
    let total_concurrency_idx = header_cols
        .iter()
        .position(|&s| s == "total_concurrency")
        .ok_or("total_concurrency column not found")?;
    let average_concurrency_idx = header_cols
        .iter()
        .position(|&s| s == "average_concurrency")
        .ok_or("average_concurrency column not found")?;

    // Find client concurrency columns for average_per_client_concurrency
    let client_concurrency_indices: Vec<usize> = header_cols
        .iter()
        .enumerate()
        .filter_map(|(idx, col)| {
            if col.starts_with("client_") && col.ends_with("_concurrency") {
                Some(idx)
            } else {
                None
            }
        })
        .collect();

    // Accumulate values across all rows
    // For cumulative values (total_bytes, total_retries), track maximum (final value)
    // For instantaneous values (concurrency), track average
    let mut max_total_bytes: f64 = 0.0;
    let mut max_total_retries: u64 = 0;
    let mut total_concurrency_sum = 0.0;
    let mut average_concurrency_sum = 0.0;
    let mut client_concurrency_sum = 0.0;
    let mut row_count = 0;

    for line in lines.iter().skip(1) {
        if line.trim().is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split(',').collect();

        if cols.len()
            <= total_bytes_idx
                .max(total_retries_idx)
                .max(total_concurrency_idx)
                .max(average_concurrency_idx)
        {
            continue;
        }

        // Parse values
        if let (Ok(bytes), Ok(retries), Ok(concurrency), Ok(avg_concurrency)) = (
            cols[total_bytes_idx].parse::<f64>(),
            cols[total_retries_idx].parse::<u64>(),
            cols[total_concurrency_idx].parse::<f64>(),
            cols[average_concurrency_idx].parse::<f64>(),
        ) {
            // Track maximum for cumulative values (final totals)
            max_total_bytes = max_total_bytes.max(bytes);
            max_total_retries = max_total_retries.max(retries);

            // Sum for averaging instantaneous values
            total_concurrency_sum += concurrency;
            average_concurrency_sum += avg_concurrency;

            // Sum client concurrencies for this row
            let mut row_client_concurrency_sum = 0.0;
            let mut row_client_count = 0;
            for &idx in &client_concurrency_indices {
                if idx < cols.len()
                    && let Ok(cc) = cols[idx].parse::<f64>()
                    && cc > 0.0
                {
                    row_client_concurrency_sum += cc;
                    row_client_count += 1;
                }
            }
            if row_client_count > 0 {
                client_concurrency_sum += row_client_concurrency_sum / row_client_count as f64;
            }

            row_count += 1;
        }
    }

    if row_count == 0 {
        return Err("No valid data rows found in timeline.csv".into());
    }

    // Use maximum values for cumulative totals, averages for instantaneous values
    let final_total_bytes = max_total_bytes;
    let final_total_retries = max_total_retries;
    let avg_total_concurrency = total_concurrency_sum / row_count as f64;
    let avg_per_client_concurrency = if !client_concurrency_indices.is_empty() {
        client_concurrency_sum / row_count as f64
    } else {
        average_concurrency_sum / row_count as f64
    };

    // Calculate network utilization from network_stats.json
    let network_utilization_percent = calculate_network_utilization(scenario_dir, final_total_bytes)?;

    // Calculate average RTT from client stats
    let average_round_trip_time_ms = calculate_average_rtt(scenario_dir)?;

    Ok(ScenarioSummary {
        scenario_name: scenario_name.to_string(),
        network_utilization_percent,
        total_retries: final_total_retries,
        average_per_client_concurrency: avg_per_client_concurrency,
        total_concurrency: avg_total_concurrency,
        total_data_transmitted_bytes: final_total_bytes,
        average_round_trip_time_ms,
    })
}

fn calculate_network_utilization(scenario_dir: &Path, avg_total_bytes: f64) -> Result<f64, Box<dyn std::error::Error>> {
    let network_stats_path = scenario_dir.join("network_stats.json");
    if !network_stats_path.exists() {
        return Ok(0.0);
    }

    let network_stats: Vec<NetworkStats> = load_json_lines(&network_stats_path)?;
    if network_stats.is_empty() {
        return Ok(0.0);
    }

    // Load server parameters for test duration
    let server_params_path = scenario_dir.join("server_parameters.json");
    let test_duration_ms = if server_params_path.exists() {
        let params: ServerSimulationParameters = load_single_json(&server_params_path)?;
        params.test_duration_seconds * 1000
    } else {
        return Ok(0.0);
    };

    let max_possible_data_bytes = calculate_max_possible_data(&network_stats, test_duration_ms);
    if max_possible_data_bytes > 0 {
        Ok((avg_total_bytes / max_possible_data_bytes as f64) * 100.0)
    } else {
        Ok(0.0)
    }
}

fn calculate_average_rtt(scenario_dir: &Path) -> Result<f64, Box<dyn std::error::Error>> {
    let mut all_rtts = Vec::new();

    for entry in fs::read_dir(scenario_dir)? {
        let entry = entry?;
        let path = entry.path();

        if let Some(file_name) = path.file_name().and_then(|n| n.to_str())
            && file_name.starts_with("client_stats_")
            && file_name.ends_with(".json")
            && let Ok(stats) = load_json_lines::<ClientMetrics>(&path)
        {
            for stat in stats {
                if stat.average_round_trip_time_ms > 0.0 {
                    all_rtts.push(stat.average_round_trip_time_ms);
                }
            }
        }
    }

    if all_rtts.is_empty() {
        Ok(0.0)
    } else {
        Ok(all_rtts.iter().sum::<f64>() / all_rtts.len() as f64)
    }
}

fn generate_summary_csv(
    results_dir: &Path,
    scenario_data: &[ScenarioSummary],
) -> Result<(), Box<dyn std::error::Error>> {
    // Sort by scenario name
    let mut sorted_data = scenario_data.to_vec();
    sorted_data.sort_by(|a, b| a.scenario_name.cmp(&b.scenario_name));

    // Create CSV header
    let mut csv = String::new();
    csv.push_str("scenario_name,network_utilization_percent,total_retries,average_per_client_concurrency,total_concurrency,total_data_transmitted_bytes,average_round_trip_time_ms\n");

    // Add rows for each scenario
    for data in &sorted_data {
        csv.push_str(&format!(
            "{},{:.2},{},{:.2},{:.2},{:.0},{:.2}\n",
            data.scenario_name,
            data.network_utilization_percent,
            data.total_retries,
            data.average_per_client_concurrency,
            data.total_concurrency,
            data.total_data_transmitted_bytes,
            data.average_round_trip_time_ms
        ));
    }

    // Write CSV to root directory
    let csv_path = results_dir.join("summary.csv");
    fs::write(&csv_path, csv)?;
    println!("Summary CSV generated: {}", csv_path.display());

    Ok(())
}

fn calculate_max_possible_data(network_stats: &[NetworkStats], duration_ms: u64) -> u64 {
    if network_stats.is_empty() {
        return 0;
    }

    // Sort by timestamp
    let mut sorted_stats = network_stats.to_vec();
    sorted_stats.sort_by_key(|s| s.timestamp.parse::<u64>().unwrap_or(0));

    let mut total_bytes = 0u64;
    let mut current_time = 0u64;

    for (i, stat) in sorted_stats.iter().enumerate() {
        let next_time = if i + 1 < sorted_stats.len() {
            sorted_stats[i + 1].timestamp.parse::<u64>().unwrap_or(0)
        } else {
            duration_ms
        };

        let segment_duration_ms = next_time - current_time;
        let bandwidth_bytes_per_ms = stat.bandwidth_bytes_per_sec as f64 / 1000.0; // Convert bytes/sec to bytes/ms
        let segment_bytes = (bandwidth_bytes_per_ms * segment_duration_ms as f64) as u64;
        total_bytes += segment_bytes;

        current_time = next_time;
    }

    total_bytes
}
