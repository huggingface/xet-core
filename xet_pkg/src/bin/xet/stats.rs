use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Args;
use walkdir::WalkDir;
use xet_data::deduplication::DeduplicationMetrics;
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::{FileUploadSession, Sha256Policy};

use super::Cli;

#[derive(Args)]
pub struct ScanArgs {
    /// Files or directories to analyze.
    pub files: Vec<PathBuf>,

    /// Process directories recursively.
    #[arg(short, long)]
    pub recursive: bool,

    /// Write JSON results to this file instead of stdout.
    #[arg(long)]
    pub output: Option<PathBuf>,
}

pub async fn run(cli: &Cli, args: &ScanArgs) -> Result<()> {
    let config = super::session::build_translator_config(&cli.resolved_endpoint())?;
    let metrics = run_scan(config, args).await?;
    if let Some(output_path) = &args.output {
        let json = serde_json::to_string_pretty(&serde_json::json!({
            "total_bytes": metrics.total_bytes,
            "new_bytes": metrics.new_bytes,
            "deduped_bytes": metrics.deduped_bytes,
            "deduped_bytes_by_global_dedup": metrics.deduped_bytes_by_global_dedup,
            "defrag_prevented_dedup_bytes": metrics.defrag_prevented_dedup_bytes,
            "total_chunks": metrics.total_chunks,
            "new_chunks": metrics.new_chunks,
            "deduped_chunks": metrics.deduped_chunks,
            "deduped_chunks_by_global_dedup": metrics.deduped_chunks_by_global_dedup,
            "defrag_prevented_dedup_chunks": metrics.defrag_prevented_dedup_chunks,
            "xorb_bytes_uploaded": metrics.xorb_bytes_uploaded,
            "shard_bytes_uploaded": metrics.shard_bytes_uploaded,
            "total_bytes_uploaded": metrics.total_bytes_uploaded,
        }))?;
        std::fs::write(output_path, json)?;
    } else if !cli.quiet {
        println!(
            "total_bytes={}  new_bytes={}  deduped_bytes={}  uploaded_bytes={}",
            metrics.total_bytes, metrics.new_bytes, metrics.deduped_bytes, metrics.total_bytes_uploaded
        );
        if metrics.total_bytes > 0 {
            let dedup_pct = 100.0 * metrics.deduped_bytes as f64 / metrics.total_bytes as f64;
            let compression_pct = if metrics.new_bytes > 0 {
                100.0 * (1.0 - metrics.total_bytes_uploaded as f64 / metrics.new_bytes as f64)
            } else {
                0.0
            };
            println!("dedup_ratio={dedup_pct:.1}%  compression_ratio={compression_pct:.1}%");
        }
    }
    Ok(())
}

pub async fn run_scan(config: Arc<TranslatorConfig>, args: &ScanArgs) -> Result<DeduplicationMetrics> {
    let session: Arc<FileUploadSession> = FileUploadSession::dry_run(config).await?;
    let files = collect_files(&args.files, args.recursive);
    let file_entries: Vec<(PathBuf, Sha256Policy)> = files.into_iter().map(|p| (p, Sha256Policy::Skip)).collect();
    session.upload_files(file_entries).await?;
    let metrics = session.finalize().await?;
    Ok(metrics)
}

fn collect_files(inputs: &[PathBuf], recursive: bool) -> Vec<PathBuf> {
    if !recursive {
        return inputs.to_vec();
    }
    inputs
        .iter()
        .flat_map(|p| {
            WalkDir::new(p)
                .follow_links(false)
                .into_iter()
                .flatten()
                .filter(|e| e.file_type().is_file())
                .map(|e| e.path().to_path_buf())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use more_asserts::assert_ge;
    use tempfile::tempdir;

    use super::*;

    fn make_translator(cas_dir: &tempfile::TempDir) -> Arc<TranslatorConfig> {
        let endpoint = format!("local://{}", cas_dir.path().display());
        crate::session::build_translator_config(&endpoint).unwrap()
    }

    #[tokio::test]
    async fn test_scan_single_file() {
        let work_dir = tempdir().unwrap();
        let src = work_dir.path().join("data.bin");
        let content = vec![42u8; 3072];
        std::fs::write(&src, &content).unwrap();

        let cas_dir = tempdir().unwrap();
        let config = make_translator(&cas_dir);

        let args = ScanArgs {
            files: vec![src],
            recursive: false,
            output: None,
        };
        let metrics = run_scan(config, &args).await.unwrap();
        assert_eq!(metrics.total_bytes, 3072);
        assert_ge!(metrics.new_bytes + metrics.deduped_bytes, metrics.total_bytes);
    }

    #[tokio::test]
    async fn test_scan_multiple_files() {
        let work_dir = tempdir().unwrap();
        let sizes: Vec<usize> = vec![1024, 2048, 4096];
        let files: Vec<PathBuf> = sizes
            .iter()
            .enumerate()
            .map(|(i, &sz)| {
                let path = work_dir.path().join(format!("file_{i}.bin"));
                std::fs::write(&path, vec![(i as u8).wrapping_add(1); sz]).unwrap();
                path
            })
            .collect();

        let cas_dir = tempdir().unwrap();
        let config = make_translator(&cas_dir);

        let args = ScanArgs {
            files,
            recursive: false,
            output: None,
        };
        let metrics = run_scan(config, &args).await.unwrap();
        let expected_total: u64 = sizes.iter().map(|&s| s as u64).sum();
        assert_eq!(metrics.total_bytes, expected_total);
    }

    #[tokio::test]
    async fn test_scan_recursive_directory() {
        let work_dir = tempdir().unwrap();
        let sub = work_dir.path().join("subdir");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(work_dir.path().join("a.bin"), vec![1u8; 512]).unwrap();
        std::fs::write(sub.join("b.bin"), vec![2u8; 256]).unwrap();

        let cas_dir = tempdir().unwrap();
        let config = make_translator(&cas_dir);

        let args = ScanArgs {
            files: vec![work_dir.path().to_path_buf()],
            recursive: true,
            output: None,
        };
        let metrics = run_scan(config, &args).await.unwrap();
        assert_eq!(metrics.total_bytes, 768);
    }

    #[tokio::test]
    async fn test_scan_json_output() {
        let work_dir = tempdir().unwrap();
        let src = work_dir.path().join("data.bin");
        std::fs::write(&src, vec![99u8; 2048]).unwrap();

        let cas_dir = tempdir().unwrap();
        let config = make_translator(&cas_dir);

        let out_dir = tempdir().unwrap();
        let json_path = out_dir.path().join("stats.json");

        let args = ScanArgs {
            files: vec![src],
            recursive: false,
            output: Some(json_path.clone()),
        };
        let metrics = run_scan(config, &args).await.unwrap();

        let json = serde_json::to_string_pretty(&serde_json::json!({
            "total_bytes": metrics.total_bytes,
            "new_bytes": metrics.new_bytes,
            "deduped_bytes": metrics.deduped_bytes,
            "total_bytes_uploaded": metrics.total_bytes_uploaded,
        }))
        .unwrap();
        std::fs::write(&json_path, &json).unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&json_path).unwrap()).unwrap();
        assert_eq!(parsed["total_bytes"], 2048);
    }
}
