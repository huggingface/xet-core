//! Session-based upload/download example.
//!
//! Shows the three-level hierarchy: XetSession → XetUploadCommit/XetFileDownloadGroup → files.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use xet::xet_session::{
    HeaderMap, HeaderValue, Sha256Policy, XetFileMetadata, XetSessionBuilder, XetTaskState, header,
};

#[derive(Parser)]
#[clap(name = "session-demo", about = "XetSession API demo")]
struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Upload files and save metadata to upload_metadata.json
    Upload {
        #[clap(required = true)]
        files: Vec<PathBuf>,
        #[clap(long)]
        endpoint: Option<String>,
    },
    /// Download files from metadata saved by the upload subcommand
    Download {
        metadata_file: PathBuf,
        #[clap(short, long, default_value = "./downloads")]
        output_dir: PathBuf,
        #[clap(long)]
        endpoint: Option<String>,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Command::Upload { files, endpoint } => upload_files(files, endpoint),
        Command::Download {
            metadata_file,
            output_dir,
            endpoint,
        } => download_files(metadata_file, output_dir, endpoint),
    }
}

fn upload_files(files: Vec<PathBuf>, endpoint: Option<String>) -> Result<()> {
    let mut hf_hub_header = HeaderMap::new();
    hf_hub_header.insert(header::AUTHORIZATION, HeaderValue::from_str("Bearer [HF_WRITE_TOKEN]")?);
    let endpoint = endpoint.unwrap_or("https://huggingface.co".into());
    let token_refresh_url = format!("{endpoint}/api/{}s/{}/xet-{}-token/{}", "model", "user/repo", "write", "main");

    let session = XetSessionBuilder::new().build()?;

    let commit = session
        .new_upload_commit()?
        .with_token_refresh_url(token_refresh_url, hf_hub_header)
        .build_blocking()?;

    let n_files = files.len();
    for f in &files {
        commit.upload_from_path_blocking(f.clone(), Sha256Policy::Compute)?;
    }

    // Spawn a task to print progress; the main thread blocks in commit() below.
    let commit_for_progress = commit.clone();
    std::thread::spawn(move || {
        loop {
            let report = commit_for_progress.progress();
            println!("{}/{} bytes", report.total_bytes_completed, report.total_bytes);
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    let report = commit.commit_blocking()?;

    for m in report.uploads.values() {
        let size = m.xet_info.file_size.map_or("unknown".to_string(), |s| s.to_string());
        println!("  {} -> {} ({} bytes)", m.tracking_name.as_deref().unwrap_or("?"), m.xet_info.hash, size);
    }
    println!("Uploaded {} files", n_files);

    // Persist metadata so it can be passed to the `download` subcommand.
    let uploads_vec: Vec<_> = report.uploads.into_values().collect();
    std::fs::write("upload_metadata.json", serde_json::to_string_pretty(&uploads_vec)?)?;

    Ok(())
}

fn download_files(metadata_file: PathBuf, output_dir: PathBuf, endpoint: Option<String>) -> Result<()> {
    let metadata: Vec<XetFileMetadata> = serde_json::from_str(&std::fs::read_to_string(metadata_file)?)?;
    std::fs::create_dir_all(&output_dir)?;

    let mut hf_hub_header = HeaderMap::new();
    hf_hub_header.insert(header::AUTHORIZATION, HeaderValue::from_str("Bearer [HF_READ_TOKEN]")?);
    let endpoint = endpoint.unwrap_or("https://huggingface.co".into());
    let token_refresh_url = format!("{endpoint}/api/{}s/{}/xet-{}-token/{}", "model", "user/repo", "read", "main");

    let session = XetSessionBuilder::new().build()?;

    let group = session
        .new_file_download_group()?
        .with_token_refresh_url(token_refresh_url, hf_hub_header)
        .build_blocking()?;

    // Enqueue all downloads; each starts immediately in the background.
    let n_files = metadata.len();
    let mut handles = Vec::with_capacity(n_files);
    for m in &metadata {
        let dest = output_dir.join(m.tracking_name.as_deref().unwrap_or("file"));
        handles.push(group.download_file_to_path_blocking(m.xet_info.clone(), dest)?);
    }

    // Spawn a task to print progress; the main thread blocks in finish() below.
    let group_for_progress = group.clone();
    std::thread::spawn(move || {
        loop {
            {
                let report = group_for_progress.progress();
                let done = handles
                    .iter()
                    .filter(|h| matches!(h.status(), Ok(XetTaskState::Completed)))
                    .count();
                println!("{}/{} files | {}/{} bytes", done, n_files, report.total_bytes_completed, report.total_bytes);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    // Block until all downloads finish.
    let report = group.finish_blocking()?;

    for r in report.downloads.values() {
        println!(
            "  {} ({:?} bytes)",
            r.path.display(),
            r.file_info.file_size
        );
    }

    Ok(())
}
