//! Session-based upload/download example
//!
//! This example demonstrates the session-based API for XetHub.
//! It shows how to:
//! - Create a session with configuration
//! - Upload files in a commit (batch)
//! - Poll progress without callbacks
//! - Download files in a group (batch)
//! - End the session

use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use xet_session::{TaskStatus, XetSession};

#[derive(Parser)]
#[clap(name = "session-demo")]
#[clap(about = "Demonstrates the XetSession API for batch uploads/downloads")]
struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Upload one or more files using a session
    Upload {
        /// Files to upload
        #[clap(required = true)]
        files: Vec<PathBuf>,

        /// CAS endpoint (optional)
        #[clap(long)]
        endpoint: Option<String>,
    },

    /// Download files using file metadata
    Download {
        /// JSON file containing metadata from a previous upload
        metadata_file: PathBuf,

        /// Output directory for downloaded files
        #[clap(short, long, default_value = "./downloads")]
        output_dir: PathBuf,

        /// CAS endpoint (optional)
        #[clap(long)]
        endpoint: Option<String>,
    },

    /// Full round-trip: upload, then download the same files
    RoundTrip {
        /// Files to upload and download
        #[clap(required = true)]
        files: Vec<PathBuf>,

        /// Output directory for downloaded files
        #[clap(short, long, default_value = "./downloads")]
        output_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for debugging
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Command::Upload { files, endpoint } => {
            upload_files(files, endpoint).await?;
        },
        Command::Download {
            metadata_file,
            output_dir,
            endpoint,
        } => {
            download_files(metadata_file, output_dir, endpoint).await?;
        },
        Command::RoundTrip { files, output_dir } => {
            round_trip(files, output_dir).await?;
        },
    }

    Ok(())
}

/// Upload files using XetSession and UploadCommit
async fn upload_files(files: Vec<PathBuf>, endpoint: Option<String>) -> Result<()> {
    println!("📤 Starting upload session...");

    // Create XetSession with configuration
    let session = XetSession::new(
        endpoint,
        None, // token_info
        None, // token_refresher
        None, // custom_headers
    )?;

    println!("✅ Session created");

    // Create an upload commit (groups related uploads)
    let upload_commit = session.new_upload_commit()?;
    println!("📦 Upload commit created");

    // Start uploading all files (tasks execute immediately)
    println!("\n🚀 Starting uploads for {} files...", files.len());
    for file in &files {
        let task_id = upload_commit.upload_from_path(file.clone())?;
        println!("  ⏫ Started upload: {} (task: {})", file.display(), task_id);
    }

    // Poll progress until all uploads complete
    println!("\n📊 Monitoring progress...");
    loop {
        let progress = upload_commit.get_progress();

        let total_files = progress.len();
        let completed = progress.iter().filter(|p| p.status == TaskStatus::Completed).count();
        let failed = progress.iter().filter(|p| p.status == TaskStatus::Failed).count();

        let total_bytes: u64 = progress.iter().map(|p| p.bytes_total).sum();
        let completed_bytes: u64 = progress.iter().map(|p| p.bytes_completed).sum();

        let percentage = if total_bytes > 0 {
            (completed_bytes as f64 / total_bytes as f64 * 100.0) as u32
        } else {
            0
        };

        print!(
            "\r  Progress: {}/{} files | {}/{} bytes ({}%)",
            completed, total_files, completed_bytes, total_bytes, percentage
        );
        std::io::stdout().flush()?;

        // Check if all done
        if completed + failed == total_files {
            println!(); // newline
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Commit - finalize uploads and get metadata
    println!("\n💾 Committing uploads...");
    let metadata = upload_commit.commit()?;

    println!("\n✅ Upload complete! Metadata:");
    for m in &metadata {
        println!(
            "  📄 {} -> {} ({} bytes)",
            m.file_name.as_ref().unwrap_or(&"<unknown>".to_string()),
            m.hash,
            m.file_size
        );
    }

    // Save metadata to file for later download
    let metadata_json = serde_json::to_string_pretty(&metadata)?;
    std::fs::write("upload_metadata.json", metadata_json)?;
    println!("\n💾 Metadata saved to upload_metadata.json");

    println!("🏁 Upload session complete");

    Ok(())
}

/// Download files using metadata from a previous upload
async fn download_files(metadata_file: PathBuf, output_dir: PathBuf, endpoint: Option<String>) -> Result<()> {
    println!("📥 Starting download session...");

    // Load metadata
    let metadata_json = std::fs::read_to_string(&metadata_file)?;
    let metadata: Vec<serde_json::Value> = serde_json::from_str(&metadata_json)?;

    // Create XetSession
    let session = XetSession::new(endpoint, None, None, None)?;

    println!("✅ Session created");

    // Create download group
    let download_group = session.new_download_group()?;
    println!("📦 Download group created");

    // Create output directory
    std::fs::create_dir_all(&output_dir)?;

    // Start downloading all files
    println!("\n🚀 Starting downloads for {} files...", metadata.len());
    for item in &metadata {
        let hash = item["hash"].as_str().unwrap().to_string();
        let file_size = item["file_size"].as_u64().unwrap();
        let file_name = item["file_name"].as_str().unwrap();

        let dest_path = output_dir.join(file_name);

        let task_id = download_group.download_file(hash, file_size, dest_path)?;
        println!("  ⏬ Started download: {} (task: {})", file_name, task_id);
    }

    // Poll progress
    println!("\n📊 Monitoring progress...");
    loop {
        let progress = download_group.get_progress();

        let total_files = progress.len();
        let completed = progress.iter().filter(|p| p.status == TaskStatus::Completed).count();
        let failed = progress.iter().filter(|p| p.status == TaskStatus::Failed).count();

        let total_bytes: u64 = progress.iter().map(|p| p.bytes_total).sum();
        let completed_bytes: u64 = progress.iter().map(|p| p.bytes_completed).sum();

        let percentage = if total_bytes > 0 {
            (completed_bytes as f64 / total_bytes as f64 * 100.0) as u32
        } else {
            0
        };

        print!(
            "\r  Progress: {}/{} files | {}/{} bytes ({}%)",
            completed, total_files, completed_bytes, total_bytes, percentage
        );
        std::io::stdout().flush()?;

        if completed + failed == total_files {
            println!();
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Finish - wait for all downloads
    println!("\n💾 Finishing downloads...");
    let results = download_group.finish()?;

    println!("\n✅ Download complete!");
    for r in &results {
        println!("  📄 {} -> {}", r.file_hash, r.dest_path.display());
    }

    println!("🏁 Download session complete");

    Ok(())
}

/// Demonstrate full round-trip: upload then download
async fn round_trip(files: Vec<PathBuf>, output_dir: PathBuf) -> Result<()> {
    println!("🔄 Starting round-trip demo (upload then download)...\n");

    // === UPLOAD PHASE ===
    println!("=== UPLOAD PHASE ===");
    let session = XetSession::new(None, None, None, None)?;

    let upload_commit = session.new_upload_commit()?;
    println!("📦 Created upload commit");

    // Upload files
    for file in &files {
        upload_commit.upload_from_path(file.clone())?;
    }
    println!("⏫ Started {} uploads", files.len());

    // Wait for uploads
    loop {
        let progress = upload_commit.get_progress();
        let all_done = progress
            .iter()
            .all(|p| p.status == TaskStatus::Completed || p.status == TaskStatus::Failed);
        if all_done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let metadata = upload_commit.commit()?;
    println!("✅ Uploaded {} files\n", metadata.len());

    // === DOWNLOAD PHASE ===
    println!("=== DOWNLOAD PHASE ===");
    let download_group = session.new_download_group()?;
    println!("📦 Created download group");

    // Create output directory
    std::fs::create_dir_all(&output_dir)?;

    // Download files
    for m in &metadata {
        let file_name = m.file_name.as_ref().unwrap();
        let dest_path = output_dir.join(file_name);

        download_group.download_file(m.hash.clone(), m.file_size, dest_path)?;
    }
    println!("⏬ Started {} downloads", metadata.len());

    // Wait for downloads
    loop {
        let progress = download_group.get_progress();
        let all_done = progress
            .iter()
            .all(|p| p.status == TaskStatus::Completed || p.status == TaskStatus::Failed);
        if all_done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let results = download_group.finish()?;
    println!("✅ Downloaded {} files to {}\n", results.len(), output_dir.display());

    println!("🏁 Round-trip complete!");

    Ok(())
}
