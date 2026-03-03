//! Session-based upload/download example.
//!
//! Shows the three-level hierarchy: XetSession → UploadCommit/DownloadGroup → files.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use xet_session::{FileMetadata, TaskHandle, TaskStatus, XetFileInfo, XetSession};

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
    let session = XetSession::new(endpoint, None, None, None)?;
    let commit = session.new_upload_commit()?;

    // Enqueue all uploads; each starts immediately in the background.
    let n_files = files.len();
    let handles: Vec<TaskHandle> = files
        .iter()
        .map(|f| commit.upload_from_path(f.clone()))
        .collect::<Result<_, _>>()?;

    // Spawn a task to print progress; the main thread blocks in commit() below.
    let commit_for_progress = commit.clone();
    let progress_task = tokio::spawn(async move {
        loop {
            if let Ok(snapshot) = commit_for_progress.get_progress() {
                let p = snapshot.total();
                let done = handles
                    .iter()
                    .filter(|h| matches!(h.status(), Ok(TaskStatus::Completed)))
                    .count();
                println!("{}/{} files | {}/{} bytes", done, n_files, p.total_bytes_completed, p.total_bytes);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // Block until all uploads finish and metadata is finalized.
    let metadata = commit.commit()?;
    progress_task.abort();

    for m in &metadata {
        println!("  {} -> {} ({} bytes)", m.tracking_name.as_deref().unwrap_or("?"), m.hash, m.file_size);
    }

    // Persist metadata so it can be passed to the `download` subcommand.
    std::fs::write("upload_metadata.json", serde_json::to_string_pretty(&metadata)?)?;

    Ok(())
}

async fn download_files(metadata_file: PathBuf, output_dir: PathBuf, endpoint: Option<String>) -> Result<()> {
    let metadata: Vec<FileMetadata> = serde_json::from_str(&std::fs::read_to_string(metadata_file)?)?;
    std::fs::create_dir_all(&output_dir)?;

    let session = XetSession::new(endpoint, None, None, None)?;
    let group = session.new_download_group()?;

    // Enqueue all downloads; each starts immediately in the background.
    let n_files = metadata.len();
    let handles: Vec<TaskHandle> = metadata
        .iter()
        .map(|m| {
            let dest = output_dir.join(m.tracking_name.as_deref().unwrap_or("file"));
            group.download_file_to_path(
                XetFileInfo {
                    hash: m.hash.clone(),
                    file_size: m.file_size,
                },
                dest,
            )
        })
        .collect::<Result<_, _>>()?;

    // Spawn a task to print progress; the main thread blocks in finish() below.
    let group_for_progress = group.clone();
    let progress_task = tokio::spawn(async move {
        loop {
            if let Ok(snapshot) = group_for_progress.get_progress() {
                let p = snapshot.total();
                let done = handles
                    .iter()
                    .filter(|h| matches!(h.status(), Ok(TaskStatus::Completed)))
                    .count();
                println!("{}/{} files | {}/{} bytes", done, n_files, p.total_bytes_completed, p.total_bytes);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // Block until all downloads finish.
    let results = group.finish()?;
    progress_task.abort();

    for r in &results {
        println!("  {} ({} bytes)", r.dest_path.display(), r.file_info.file_size);
    }

    Ok(())
}
