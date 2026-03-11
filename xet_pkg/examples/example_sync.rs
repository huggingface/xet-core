//! Session-based upload/download example.
//!
//! Shows the three-level hierarchy: XetSession → UploadCommit/DownloadGroup → files.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use xet::xet_session::{FileMetadata, TaskStatus, XetFileInfo, XetSessionBuilder};
use xet_data::processing::Sha256Policy;

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
    let mut builder = XetSessionBuilder::new();
    if let Some(ep) = endpoint {
        builder = builder.with_endpoint(ep);
    }
    let session = builder.build()?;
    let commit = session.new_upload_commit_blocking()?;

    // Enqueue all uploads; each starts immediately in the background.
    let n_files = files.len();
    let handles: Vec<_> = files
        .iter()
        .map(|f| commit.upload_from_path(f.clone(), Sha256Policy::Compute))
        .collect::<Result<_, _>>()?;

    // Spawn a task to print progress; the main thread blocks in commit() below.
    let commit_for_progress = commit.clone();
    std::thread::spawn(move || {
        loop {
            if let Ok(snapshot) = commit_for_progress.get_progress() {
                let p = snapshot.total();
                let done = handles
                    .iter()
                    .filter(|h| matches!(h.status(), Ok(TaskStatus::Completed)))
                    .count();
                println!("{}/{} files | {}/{} bytes", done, n_files, p.total_bytes_completed, p.total_bytes);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    // Block until all uploads finish and metadata is finalized.
    let results = commit.commit()?;

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

fn download_files(metadata_file: PathBuf, output_dir: PathBuf, endpoint: Option<String>) -> Result<()> {
    let metadata: Vec<FileMetadata> = serde_json::from_str(&std::fs::read_to_string(metadata_file)?)?;
    std::fs::create_dir_all(&output_dir)?;

    let mut builder = XetSessionBuilder::new();
    if let Some(ep) = endpoint {
        builder = builder.with_endpoint(ep);
    }
    let session = builder.build()?;
    let group = session.new_download_group_blocking()?;

    // Enqueue all downloads; each starts immediately in the background.
    let n_files = metadata.len();
    let handles: Vec<_> = metadata
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
    std::thread::spawn(move || {
        loop {
            if let Ok(snapshot) = group_for_progress.get_progress() {
                let p = snapshot.total();
                let done = handles
                    .iter()
                    .filter(|h| matches!(h.status(), Ok(TaskStatus::Completed)))
                    .count();
                println!("{}/{} files | {}/{} bytes", done, n_files, p.total_bytes_completed, p.total_bytes);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    // Block until all downloads finish.
    let results = group.finish()?;

    for (_task_id, result) in &results {
        if let Ok(r) = result.as_ref() {
            println!("  {} ({} bytes)", r.dest_path.display(), r.file_info.file_size);
        }
    }

    Ok(())
}
