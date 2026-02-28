//! Timeline and summary CSV generation from client_stats JSON lines.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;

use serde::Deserialize;

use super::upload_simulation_client::ClientMetrics;
use crate::scenario::NetworkStats;

const INTERVAL_MS: u64 = 250;

#[derive(Debug, Clone)]
struct ClientTimelineData {
    first_timestamp: u64,
    last_timestamp: u64,
    stats_by_timestamp: BTreeMap<u64, ClientMetrics>,
}

fn load_json_lines<T>(file_path: &Path) -> Result<Vec<T>, Box<dyn std::error::Error + Send + Sync>>
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

/// Generates timeline.csv in the given scenario directory from client_stats_*.json files.
pub fn generate_timeline_csv(scenario_dir: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut client_timelines: HashMap<u64, ClientTimelineData> = HashMap::new();

    for entry in fs::read_dir(scenario_dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(file_name) = path.file_name().and_then(|n| n.to_str())
            && file_name.starts_with("client_stats_")
            && file_name.ends_with(".json")
        {
            let stats = match load_json_lines::<ClientMetrics>(&path) {
                Ok(s) => s,
                Err(_) => continue,
            };
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
        return Err("No client_stats_*.json found".into());
    }

    let mut sorted_clients: Vec<_> = client_timelines.values().collect();
    sorted_clients.sort_by_key(|c| c.first_timestamp);

    let start_time = sorted_clients.iter().map(|c| c.first_timestamp).min().unwrap_or(0);
    let end_time = sorted_clients.iter().map(|c| c.last_timestamp).max().unwrap_or(0);
    if start_time == 0 || end_time == 0 || end_time <= start_time {
        return Err("Invalid time window".into());
    }

    let mut csv = String::new();
    csv.push_str("timestamp_ms,elapsed_ms");
    for (idx, _) in sorted_clients.iter().enumerate() {
        let client_num = format!("{:02}", idx + 1);
        csv.push_str(&format!(
            ",client_{}_bytes,client_{}_concurrency,client_{}_retries,client_{}_success_ratio,client_{}_predicted_bandwidth,client_{}_predicted_max_rtt,client_{}_predicted_max_rtt_standard_error",
            client_num, client_num, client_num, client_num, client_num, client_num, client_num
        ));
    }
    csv.push_str(",total_bytes,total_retries,total_concurrency,average_concurrency\n");

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
            let last_entry = client.stats_by_timestamp.range(..=current_time).next_back().map(|(_, s)| s);
            if let Some(stat) = last_entry {
                let bytes = stat.total_bytes;
                let concurrency = if current_time <= client.last_timestamp {
                    stat.current_max_concurrency
                } else {
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
                let predicted_max_rtt_se = stat
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
                    predicted_max_rtt_se
                ));
                total_bytes += bytes;
                total_retries += retries;
                total_concurrency += concurrency;
                if concurrency > 0 {
                    active_client_count += 1;
                    active_concurrency_sum += concurrency;
                }
            } else {
                csv.push_str(",0,0,0,0.0,0.0,0.0,0.0");
            }
        }

        let average_concurrency = if active_client_count > 0 {
            active_concurrency_sum as f64 / active_client_count as f64
        } else {
            0.0
        };
        csv.push_str(&format!(",{},{},{},{:.2}\n", total_bytes, total_retries, total_concurrency, average_concurrency));
        current_time += INTERVAL_MS;
    }

    let csv_path = scenario_dir.join("timeline.csv");
    fs::write(&csv_path, csv)?;
    Ok(())
}

#[derive(Debug, Clone)]
struct ScenarioSummaryRow {
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
) -> Result<ScenarioSummaryRow, Box<dyn std::error::Error + Send + Sync>> {
    let content = fs::read_to_string(timeline_path)?;
    let lines: Vec<&str> = content.lines().collect();
    if lines.is_empty() {
        return Err("Empty timeline.csv".into());
    }

    let header = lines[0];
    let header_cols: Vec<&str> = header.split(',').collect();
    let total_bytes_idx = header_cols
        .iter()
        .position(|&s| s == "total_bytes")
        .ok_or("total_bytes column")?;
    let elapsed_ms_idx = header_cols.iter().position(|&s| s == "elapsed_ms").ok_or("elapsed_ms column")?;
    let total_retries_idx = header_cols
        .iter()
        .position(|&s| s == "total_retries")
        .ok_or("total_retries column")?;
    let total_concurrency_idx = header_cols
        .iter()
        .position(|&s| s == "total_concurrency")
        .ok_or("total_concurrency column")?;
    let average_concurrency_idx = header_cols
        .iter()
        .position(|&s| s == "average_concurrency")
        .ok_or("average_concurrency column")?;

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

    let mut max_total_bytes: f64 = 0.0;
    let mut max_elapsed_ms: u64 = 0;
    let mut max_total_retries: u64 = 0;
    let mut total_concurrency_sum = 0.0;
    let mut average_concurrency_sum = 0.0;
    let mut client_concurrency_sum = 0.0;
    let mut row_count = 0;

    let max_idx = total_bytes_idx
        .max(total_retries_idx)
        .max(total_concurrency_idx)
        .max(average_concurrency_idx)
        .max(elapsed_ms_idx);

    for line in lines.iter().skip(1) {
        if line.trim().is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() <= max_idx {
            continue;
        }
        let bytes: f64 = cols[total_bytes_idx].parse().unwrap_or(0.0);
        let elapsed_ms: u64 = cols[elapsed_ms_idx].parse().unwrap_or(0);
        let retries: u64 = cols[total_retries_idx].parse().unwrap_or(0);
        let concurrency: f64 = cols[total_concurrency_idx].parse().unwrap_or(0.0);
        let avg_concurrency: f64 = cols[average_concurrency_idx].parse().unwrap_or(0.0);

        max_total_bytes = max_total_bytes.max(bytes);
        max_elapsed_ms = max_elapsed_ms.max(elapsed_ms);
        max_total_retries = max_total_retries.max(retries);
        total_concurrency_sum += concurrency;
        average_concurrency_sum += avg_concurrency;

        let mut row_client_sum = 0.0;
        let mut row_client_count = 0;
        for &idx in &client_concurrency_indices {
            if idx < cols.len()
                && let Ok(cc) = cols[idx].parse::<f64>()
                && cc > 0.0
            {
                row_client_sum += cc;
                row_client_count += 1;
            }
        }
        if row_client_count > 0 {
            client_concurrency_sum += row_client_sum / row_client_count as f64;
        }
        row_count += 1;
    }

    if row_count == 0 {
        return Err("No data rows in timeline.csv".into());
    }

    let avg_total_concurrency = total_concurrency_sum / row_count as f64;
    let avg_per_client_concurrency = if client_concurrency_indices.is_empty() {
        average_concurrency_sum / row_count as f64
    } else {
        client_concurrency_sum / row_count as f64
    };

    let duration_sec = if max_elapsed_ms > 0 {
        max_elapsed_ms as f64 / 1000.0
    } else {
        0.0
    };
    let network_utilization_percent =
        calculate_network_utilization(scenario_dir, max_total_bytes, duration_sec).unwrap_or(0.0);
    let average_round_trip_time_ms = calculate_average_rtt(scenario_dir).unwrap_or(0.0);

    Ok(ScenarioSummaryRow {
        scenario_name: scenario_name.to_string(),
        network_utilization_percent,
        total_retries: max_total_retries,
        average_per_client_concurrency: avg_per_client_concurrency,
        total_concurrency: avg_total_concurrency,
        total_data_transmitted_bytes: max_total_bytes,
        average_round_trip_time_ms,
    })
}

/// Computes network utilization as (bytes_sent / max_possible_bytes) * 100, capped at 100%.
/// max_possible_bytes = sum over segments of (bandwidth_bytes_per_sec * segment_sec) from network_stats.json,
/// using elapsed_sec for segment boundaries so we don't mix absolute timestamps with duration.
fn calculate_network_utilization(
    scenario_dir: &Path,
    total_bytes: f64,
    duration_sec: f64,
) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
    let network_stats_path = scenario_dir.join("network_stats.json");
    if !network_stats_path.exists() || duration_sec <= 0.0 {
        return Ok(0.0);
    }
    let network_stats: Vec<NetworkStats> = load_json_lines(&network_stats_path)?;
    if network_stats.is_empty() {
        return Ok(0.0);
    }
    let mut sorted_stats = network_stats;
    sorted_stats.sort_by(|a, b| a.elapsed_sec.partial_cmp(&b.elapsed_sec).unwrap_or(std::cmp::Ordering::Equal));
    let mut max_possible: f64 = 0.0;
    let mut current_elapsed_sec: f64 = 0.0;
    for (i, stat) in sorted_stats.iter().enumerate() {
        let next_elapsed_sec = if i + 1 < sorted_stats.len() {
            sorted_stats[i + 1].elapsed_sec
        } else {
            duration_sec
        };
        let segment_sec = (next_elapsed_sec - current_elapsed_sec).max(0.0);
        max_possible += stat.bandwidth_bytes_per_sec as f64 * segment_sec;
        current_elapsed_sec = next_elapsed_sec;
    }
    if max_possible > 0.0 {
        let utilization = (total_bytes / max_possible) * 100.0;
        Ok(utilization.min(100.0))
    } else {
        Ok(0.0)
    }
}

fn calculate_average_rtt(scenario_dir: &Path) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
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

/// Generates summary.csv in the given results directory from all scenario subdirectories that have timeline.csv.
pub fn generate_summary_csv(results_dir: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut scenario_dirs = Vec::new();
    for entry in fs::read_dir(results_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() && path.join("timeline.csv").exists() {
            scenario_dirs.push(path);
        }
    }
    if scenario_dirs.is_empty() {
        return Err("No scenario directories with timeline.csv found".into());
    }

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
                tracing::warn!("Failed to process {}: {}", scenario_name, e);
            },
        }
    }
    if scenario_data.is_empty() {
        return Err("No valid timeline.csv files".into());
    }

    scenario_data.sort_by(|a, b| a.scenario_name.cmp(&b.scenario_name));

    let mut csv = String::new();
    csv.push_str("scenario_name,network_utilization_percent,total_retries,average_per_client_concurrency,total_concurrency,total_data_transmitted_bytes,average_round_trip_time_ms\n");
    for data in &scenario_data {
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
    let csv_path = results_dir.join("summary.csv");
    fs::write(&csv_path, csv)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::generate_summary_csv;

    /// Verifies network utilization is computed from elapsed_sec in network_stats (not raw timestamps).
    #[test]
    fn network_utilization_uses_elapsed_sec() {
        let temp = tempfile::tempdir().unwrap();
        let results_dir = temp.path();
        let scenario_dir = results_dir.join("test-1gbps-20ms-none");
        fs::create_dir_all(&scenario_dir).unwrap();
        let timeline = "timestamp_ms,elapsed_ms,client_01_bytes,client_01_concurrency,client_01_retries,client_01_success_ratio,client_01_predicted_bandwidth,client_01_predicted_max_rtt,client_01_predicted_max_rtt_standard_error,total_bytes,total_retries,total_concurrency,average_concurrency\n\
1000,0,0,1,0,1.0,0.0,0.0,0.0,0,0,1,1.00\n\
20000,19000,625000000,1,0,1.0,0.0,0.0,0.0,625000000,0,1,1.00\n";
        fs::write(scenario_dir.join("timeline.csv"), timeline).unwrap();
        let network_stats = r#"{"timestamp":"1000","latency_ms":20.0,"bandwidth_bytes_per_sec":125000000,"congestion_mode":null,"interface":null,"elapsed_sec":0.0,"total_upload_possible":0,"server_latency_profile":"none"}
{"timestamp":"2000","latency_ms":20.0,"bandwidth_bytes_per_sec":125000000,"congestion_mode":null,"interface":null,"elapsed_sec":0.25,"total_upload_possible":31250000,"server_latency_profile":"none"}
{"timestamp":"3000","latency_ms":20.0,"bandwidth_bytes_per_sec":125000000,"congestion_mode":null,"interface":null,"elapsed_sec":5.0,"total_upload_possible":625000000,"server_latency_profile":"none"}
"#;
        fs::write(scenario_dir.join("network_stats.json"), network_stats).unwrap();
        generate_summary_csv(results_dir).unwrap();
        let csv = fs::read_to_string(results_dir.join("summary.csv")).unwrap();
        let lines: Vec<&str> = csv.lines().collect();
        assert!(lines.len() >= 2, "expected header + at least one row");
        let row = lines[1];
        let parts: Vec<&str> = row.split(',').collect();
        let utilization: f64 = parts.get(1).unwrap().parse().unwrap();
        assert!(utilization > 0.0 && utilization <= 100.0, "utilization should be in (0, 100], got {}", utilization);
        let total_bytes: f64 = parts.get(5).unwrap().parse().unwrap();
        assert_eq!(total_bytes, 625_000_000.0);
    }
}
