//! Async session-based upload/download example.
//!
//! Mirror of `example_sync.rs` using the async API (`XetUploadCommit` / `XetFileDownloadGroup`).
//! Requires an async runtime — here provided by `#[tokio::main]`.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use http::{HeaderMap, HeaderValue, header};
use xet::xet_session::{Sha256Policy, XetFileDownload, XetFileMetadata, XetSessionBuilder, XetTaskState};
use xet_client::hub_client::{self, HFRepoType, HubClient, RepoInfo};

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
    let mut hf_hub_header = HeaderMap::new();
    hf_hub_header.insert(header::AUTHORIZATION, HeaderValue::from_str("Bearer [HF_WRITE_TOKEN]")?);
    let hub_client = HubClient::new(
        &endpoint.unwrap_or("https://huggingface.co".into()),
        RepoInfo {
            repo_type: HFRepoType::Model,
            full_name: "user/repo".into(),
        },
        Some("main".into()),
        "",
        None,
        Some(hf_hub_header),
    )?;
    let token_info = hub_client.get_cas_jwt(hub_client::Operation::Upload).await?;

    let session = XetSessionBuilder::new().with_endpoint(token_info.cas_url).build()?;

    let commit = session
        .new_upload_commit()?
        .with_token_info(token_info.access_token, token_info.exp)
        //.with_token_refresh_url(token_refresh_url, hf_hub_header) // see HubClient::get_cas_jwt for how to build a token_refresh_url
        .build()
        .await?;

    let n_files = files.len();
    for f in &files {
        commit.upload_from_path(f.clone(), Sha256Policy::Compute).await?;
    }

    // Spawn a task to print progress while the main task awaits commit().
    let commit_for_progress = commit.clone();
    tokio::spawn(async move {
        loop {
            let report = commit_for_progress.progress();
            println!("{}/{} bytes", report.total_bytes_completed, report.total_bytes);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });

    let report = commit.commit().await?;

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

async fn download_files(metadata_file: PathBuf, output_dir: PathBuf, endpoint: Option<String>) -> Result<()> {
    let metadata: Vec<XetFileMetadata> = serde_json::from_str(&std::fs::read_to_string(metadata_file)?)?;
    std::fs::create_dir_all(&output_dir)?;

    let mut hf_hub_header = HeaderMap::new();
    hf_hub_header.insert(header::AUTHORIZATION, HeaderValue::from_str("Bearer [HF_READ_TOKEN]")?);
    let hub_client = HubClient::new(
        &endpoint.unwrap_or("https://huggingface.co".into()),
        RepoInfo {
            repo_type: HFRepoType::Model,
            full_name: "user/repo".into(),
        },
        Some("main".into()),
        "",
        None,
        Some(hf_hub_header),
    )?;
    let token_info = hub_client.get_cas_jwt(hub_client::Operation::Download).await?;

    let session = XetSessionBuilder::new().with_endpoint(token_info.cas_url).build()?;

    let group = session
        .new_file_download_group()?
        .with_token_info(token_info.access_token, token_info.exp)
        //.with_token_refresh_url(token_refresh_url, hf_hub_header) // see HubClient::get_cas_jwt for how to build a token_refresh_url
        .build()
        .await?;

    let n_files = metadata.len();
    let mut handles: Vec<XetFileDownload> = Vec::with_capacity(n_files);
    for m in &metadata {
        let dest = output_dir.join(m.tracking_name.as_deref().unwrap_or("file"));
        handles.push(group.download_file_to_path(m.xet_info.clone(), dest).await?);
    }

    // Spawn a task to print progress while the main task awaits finish().
    let group_for_progress = group.clone();
    tokio::spawn(async move {
        loop {
            {
                let report = group_for_progress.progress();
                let done = handles
                    .iter()
                    .filter(|h| matches!(h.status(), Ok(XetTaskState::Completed)))
                    .count();
                println!("{}/{} files | {}/{} bytes", done, n_files, report.total_bytes_completed, report.total_bytes);
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });

    let report = group.finish().await?;

    for r in report.downloads.values() {
        println!(
            "  {} ({:?} bytes)",
            r.path.as_ref().map_or("?".into(), |p| p.display().to_string()),
            r.file_info.file_size
        );
    }

    Ok(())
}
