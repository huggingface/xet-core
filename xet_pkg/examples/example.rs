//! Async session-based upload/download example.
//!
//! Mirror of `example.rs` using the async API (`UploadCommit` / `FileDownloadGroup`).
//! Requires an async runtime — here provided by `#[tokio::main]`.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use xet::xet_session::{
    DownloadTaskHandle, FileMetadata, Sha256Policy, TaskStatus, UploadTaskHandle, XetFileInfo, XetSessionBuilder,
};

#[derive(Parser)]
#[clap(name = "session-demo-async", about = "XetSession async API demo")]
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Command::Upload { files, endpoint } => upload_files(files, endpoint).await,
        Command::Download {
            metadata_file,
            output_dir,
            endpoint,
        } => download_files(metadata_file, output_dir, endpoint).await,
    }
}

async fn upload_files(files: Vec<PathBuf>, endpoint: Option<String>) -> Result<()> {
    let mut builder = XetSessionBuilder::new();
    if let Some(ep) = endpoint {
        builder = builder.with_endpoint(ep);
    }
    let session = builder.build_async().await?;
    let commit = session.new_upload_commit().await?;

    // Enqueue all uploads; each starts immediately in the background.
    let n_files = files.len();
    let mut handles = Vec::with_capacity(n_files);
    for f in &files {
        handles.push(commit.upload_from_path(f.clone(), Sha256Policy::Compute).await?);
    }

    // Spawn a task to print progress while the main task awaits commit().
    let commit_for_progress = commit.clone();
    tokio::spawn(async move {
        loop {
            if let Ok(report) = commit_for_progress.get_progress() {
                let done = handles
                    .iter()
                    .filter(|h: &&UploadTaskHandle| matches!(h.status(), Ok(TaskStatus::Completed)))
                    .count();
                println!("{}/{} files | {}/{} bytes", done, n_files, report.total_bytes_completed, report.total_bytes);
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });

    // Await until all uploads finish and metadata is finalized.
    let results = commit.commit().await?;

    for m in results.values().filter_map(|m| m.as_ref().as_ref().ok()) {
        println!("  {} -> {} ({} bytes)", m.tracking_name.as_deref().unwrap_or("?"), m.hash, m.file_size);
    }

    // Persist metadata so it can be passed to the `download` subcommand.
    let metadata: Vec<_> = results
        .into_values()
        .filter_map(|m| m.as_ref().as_ref().ok().cloned())
        .collect();
    std::fs::write("upload_metadata.json", serde_json::to_string_pretty(&metadata)?)?;

    Ok(())
}

async fn download_files(metadata_file: PathBuf, output_dir: PathBuf, endpoint: Option<String>) -> Result<()> {
    let metadata: Vec<FileMetadata> = serde_json::from_str(&std::fs::read_to_string(metadata_file)?)?;
    std::fs::create_dir_all(&output_dir)?;

    let mut builder = XetSessionBuilder::new();
    if let Some(ep) = endpoint {
        builder = builder.with_endpoint(ep);
    }
    let session = builder.build_async().await?;
    let group = session.new_file_download_group().await?;

    // Enqueue all downloads; each starts immediately in the background.
    let n_files = metadata.len();
    let mut handles: Vec<DownloadTaskHandle> = Vec::with_capacity(n_files);
    for m in &metadata {
        let dest = output_dir.join(m.tracking_name.as_deref().unwrap_or("file"));
        handles.push(
            group
                .download_file_to_path(
                    XetFileInfo {
                        hash: m.hash.clone(),
                        file_size: m.file_size,
                        sha256: m.sha256.clone(),
                    },
                    dest,
                )
                .await?,
        );
    }

    // Spawn a task to print progress while the main task awaits finish().
    let group_for_progress = group.clone();
    tokio::spawn(async move {
        loop {
            if let Ok(report) = group_for_progress.get_progress() {
                let done = handles
                    .iter()
                    .filter(|h| matches!(h.status(), Ok(TaskStatus::Completed)))
                    .count();
                println!("{}/{} files | {}/{} bytes", done, n_files, report.total_bytes_completed, report.total_bytes);
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });

    // Await until all downloads finish.
    let results = group.finish().await?;

    for (_task_id, result) in &results {
        if let Ok(r) = result.as_ref() {
            println!("  {} ({} bytes)", r.dest_path.display(), r.file_info.file_size);
        }
    }

    Ok(())
}
