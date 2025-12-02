#![cfg(not(target_family = "wasm"))]

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;

use clap::Parser;
use serde::Deserialize;

mod common;
use common::ClientMetrics;

const INTERVAL_MS: u64 = 250;

#[derive(Debug, Clone)]
struct ClientTimelineData {
    first_timestamp: u64,
    last_timestamp: u64,
    stats_by_timestamp: BTreeMap<u64, ClientMetrics>,
}

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

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Scenario directory containing client_stats files
    scenario_directory: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let scenario_dir = Path::new(&args.scenario_directory);
    if !scenario_dir.exists() {
        eprintln!("Error: Directory {} does not exist", scenario_dir.display());
        std::process::exit(1);
    }

    generate_timeline_csv(scenario_dir)
}

fn generate_timeline_csv(scenario_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // Load client stats files
    let mut client_timelines: HashMap<u64, ClientTimelineData> = HashMap::new();

    for entry in fs::read_dir(scenario_dir)? {
        let entry = entry?;
        let path = entry.path();

        if let Some(file_name) = path.file_name().and_then(|n| n.to_str())
            && file_name.starts_with("client_stats_")
            && file_name.ends_with(".json")
            && let Ok(stats) = load_json_lines::<ClientMetrics>(&path)
        {
            for stat in stats {
                let timestamp = stat.timestamp.parse::<u64>().unwrap_or(0);
                let client_id = stat.client_id;

                let timeline = client_timelines.entry(client_id).or_insert_with(|| ClientTimelineData {
                    first_timestamp: timestamp,
                    last_timestamp: timestamp,
                    stats_by_timestamp: BTreeMap::new(),
                });

                timeline.stats_by_timestamp.insert(timestamp, stat.clone());
                timeline.first_timestamp = timeline.first_timestamp.min(timestamp);
                timeline.last_timestamp = timeline.last_timestamp.max(timestamp);
            }
        }
    }

    if client_timelines.is_empty() {
        eprintln!("Error: No client stats files found in {}", scenario_dir.display());
        std::process::exit(1);
    }

    // Sort clients by first active time
    let mut sorted_clients: Vec<_> = client_timelines.values().collect();
    sorted_clients.sort_by_key(|c| c.first_timestamp);

    // Determine time window: from earliest first_timestamp to latest last_timestamp
    let start_time = sorted_clients.iter().map(|c| c.first_timestamp).min().unwrap_or(0);
    let end_time = sorted_clients.iter().map(|c| c.last_timestamp).max().unwrap_or(0);

    if start_time == 0 || end_time == 0 || end_time <= start_time {
        eprintln!("Error: Invalid time window (start={}, end={})", start_time, end_time);
        std::process::exit(1);
    }

    // Build CSV header
    let mut csv = String::new();
    csv.push_str("timestamp_ms,elapsed_ms");

    for (idx, _client) in sorted_clients.iter().enumerate() {
        let client_num = format!("{:02}", idx + 1);
        csv.push_str(&format!(
            ",client_{}_bytes,client_{}_concurrency,client_{}_retries,client_{}_success_ratio,client_{}_predicted_bandwidth,client_{}_predicted_max_rtt,client_{}_predicted_max_rtt_standard_error",
            client_num, client_num, client_num, client_num, client_num, client_num, client_num
        ));
    }

    csv.push_str(",total_bytes,total_retries,total_concurrency,average_concurrency\n");

    // Generate rows for each 250ms interval
    let mut current_time = start_time;
    while current_time <= end_time {
        let elapsed_ms = current_time - start_time;

        csv.push_str(&format!("{},{}", current_time, elapsed_ms));

        let mut total_bytes = 0u64;
        let mut total_retries = 0u64;
        let mut total_concurrency = 0usize;
        let mut active_client_count = 0usize;
        let mut active_concurrency_sum = 0usize;

        for client in &sorted_clients {
            // Find the last known entry for this client at or before current_time
            // Use range to efficiently find the last entry <= current_time
            let last_entry = client
                .stats_by_timestamp
                .range(..=current_time)
                .next_back()
                .map(|(_, stat)| stat);

            if let Some(stat) = last_entry {
                // Client is active - use actual values
                let bytes = stat.total_bytes;
                let concurrency = if current_time <= client.last_timestamp {
                    stat.current_max_concurrency
                } else {
                    // After client finished, concurrency returns to 0
                    0
                };
                let retries = stat.total_retries;
                let success_ratio = stat
                    .concurrency_controller_stats
                    .as_ref()
                    .map(|s| s.success_ratio)
                    .unwrap_or(0.0);
                let predicted_bandwidth =
                    stat.latency_model_stats.as_ref().map(|s| s.predicted_bandwidth).unwrap_or(0.0);
                let predicted_max_rtt = stat.latency_model_stats.as_ref().map(|s| s.predicted_max_rtt).unwrap_or(0.0);
                let predicted_max_rtt_standard_error = stat
                    .latency_model_stats
                    .as_ref()
                    .map(|s| s.prediction_max_rtt_standard_error)
                    .unwrap_or(0.0);

                csv.push_str(&format!(
                    ",{},{},{},{:.6},{:.6},{:.6},{:.6}",
                    bytes,
                    concurrency,
                    retries,
                    success_ratio,
                    predicted_bandwidth,
                    predicted_max_rtt,
                    predicted_max_rtt_standard_error
                ));

                total_bytes += bytes;
                total_retries += retries;
                total_concurrency += concurrency;

                // Track active clients (non-zero concurrency) for average calculation
                if concurrency > 0 {
                    active_client_count += 1;
                    active_concurrency_sum += concurrency;
                }
            } else {
                // Client not active yet - use 0 values
                csv.push_str(",0,0,0,0.0,0.0,0.0,0.0");
            }
        }

        // Calculate average concurrency across active clients
        let average_concurrency = if active_client_count > 0 {
            active_concurrency_sum as f64 / active_client_count as f64
        } else {
            0.0
        };

        csv.push_str(&format!(",{},{},{},{:.2}\n", total_bytes, total_retries, total_concurrency, average_concurrency));

        current_time += INTERVAL_MS;
    }

    // Write CSV file
    let csv_path = scenario_dir.join("timeline.csv");
    fs::write(&csv_path, csv)?;
    println!("Timeline CSV generated: {}", csv_path.display());

    Ok(())
}
