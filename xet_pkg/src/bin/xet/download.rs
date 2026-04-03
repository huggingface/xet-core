use std::fs::{File, OpenOptions};
use std::io::{Seek, Write};
use std::ops::Range;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use xet::xet_session::XetSession;
use xet_data::processing::XetFileInfo;
use xet_runtime::config::XetConfig;

use super::Cli;

fn parse_range(s: &str) -> Result<Range<u64>> {
    let (start, end) = super::parse_byte_range(s)?;
    Ok(start..end)
}

#[derive(Args)]
pub struct DownloadArgs {
    /// Hex-encoded MerkleHash of the file.
    pub hash: String,

    /// Output file path. If omitted, data is written to stdout.
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,

    /// Source byte range to download, e.g. "32..64" or "100..".
    /// Omit for the full file.
    #[arg(long)]
    pub source_range: Option<String>,

    /// Byte range in the output file to write into, e.g. "64..".
    /// Only valid with -o. Seeks to the start offset before writing.
    #[arg(long, requires = "output")]
    pub write_range: Option<String>,

    /// Expected file size in bytes (optional; improves progress tracking).
    #[arg(long)]
    pub size: Option<u64>,
}

pub async fn run(cli: &Cli, config: XetConfig, args: &DownloadArgs) -> Result<()> {
    let session = super::session::build_xet_session(config)?;
    let endpoint = cli.resolved_endpoint();
    run_download(&session, &endpoint, args, cli.quiet, cli.resolved_token()).await
}

pub async fn run_download(
    session: &XetSession,
    endpoint: &str,
    args: &DownloadArgs,
    quiet: bool,
    token: Option<String>,
) -> Result<()> {
    if args.write_range.is_some() && args.output.is_none() {
        anyhow::bail!("--write-range requires --output");
    }

    let file_info = match args.size {
        Some(size) => XetFileInfo::new(args.hash.clone(), size),
        None => XetFileInfo::new_hash_only(args.hash.clone()),
    };

    let source_range: Option<Range<u64>> = args.source_range.as_deref().map(parse_range).transpose()?;

    let mut group_builder = session
        .new_download_stream_group()
        .map_err(|e| anyhow::anyhow!(e))?
        .with_endpoint(endpoint);
    if let Some(tok) = token {
        group_builder = group_builder.with_token_info(tok, u64::MAX);
    }
    let group = group_builder.build().await.map_err(|e| anyhow::anyhow!(e))?;
    let mut stream = group
        .download_stream(file_info, source_range)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let mut total_bytes: u64 = 0;

    match &args.output {
        Some(output_path) => {
            if let Some(parent) = output_path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent)?;
            }

            let write_range: Option<Range<u64>> = args.write_range.as_deref().map(parse_range).transpose()?;

            let mut file: File = if write_range.is_some() {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(output_path)
                    .with_context(|| format!("failed to open {}", output_path.display()))?
            } else {
                File::create(output_path).with_context(|| format!("failed to create {}", output_path.display()))?
            };

            if let Some(ref wr) = write_range {
                file.seek(std::io::SeekFrom::Start(wr.start))?;
            }

            let write_limit = write_range
                .as_ref()
                .map(|r| if r.end == u64::MAX { u64::MAX } else { r.end - r.start });

            while let Some(chunk) = stream.next().await? {
                let data = if let Some(limit) = write_limit {
                    let remaining = limit.saturating_sub(total_bytes);
                    if remaining == 0 {
                        break;
                    }
                    if (chunk.len() as u64) > remaining {
                        &chunk[..remaining as usize]
                    } else {
                        &chunk
                    }
                } else {
                    &chunk
                };
                file.write_all(data)?;
                total_bytes += data.len() as u64;
            }
            file.flush()?;

            if !quiet {
                eprintln!("Downloaded {} → {} ({total_bytes} bytes)", args.hash, output_path.display());
            }
        },
        None => {
            while let Some(chunk) = stream.next().await? {
                std::io::stdout().write_all(&chunk)?;
            }
            std::io::stdout().flush()?;
        },
    }

    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::session::build_xet_session;
    use crate::upload::{UploadArgs, run_upload};

    pub(crate) async fn upload_test_file(
        cas_dir: &tempfile::TempDir,
        name: &str,
        content: &[u8],
    ) -> (String, String, u64) {
        let endpoint = format!("local://{}", cas_dir.path().display());
        let src_dir = tempdir().unwrap();
        let src = src_dir.path().join(name);
        std::fs::write(&src, content).unwrap();

        let session = build_xet_session(XetConfig::new()).unwrap();
        let upload_args = UploadArgs {
            files: vec![src.to_str().unwrap().to_owned()],
            no_sha256: true,
            dump_stats: false,
            output: None,
        };
        let results = run_upload(&session, &endpoint, &upload_args, None).await.unwrap();
        let meta = &results[0];
        (endpoint, meta.xet_info.hash.clone(), meta.xet_info.file_size.unwrap_or(0))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_download_to_file() {
        let cas_dir = tempdir().unwrap();
        let content = b"download test content 12345";
        let (endpoint, hash, size) = upload_test_file(&cas_dir, "data.bin", content).await;

        let dest_dir = tempdir().unwrap();
        let dest = dest_dir.path().join("out.bin");

        let session = build_xet_session(XetConfig::new()).unwrap();
        let args = DownloadArgs {
            hash,
            output: Some(dest.clone()),
            source_range: None,
            write_range: None,
            size: Some(size),
        };
        run_download(&session, &endpoint, &args, false, None).await.unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), content);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_download_without_size() {
        let cas_dir = tempdir().unwrap();
        let content = b"download without size test";
        let (endpoint, hash, _) = upload_test_file(&cas_dir, "data.bin", content).await;

        let dest_dir = tempdir().unwrap();
        let dest = dest_dir.path().join("out.bin");

        let session = build_xet_session(XetConfig::new()).unwrap();
        let args = DownloadArgs {
            hash,
            output: Some(dest.clone()),
            source_range: None,
            write_range: None,
            size: None,
        };
        run_download(&session, &endpoint, &args, false, None).await.unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), content);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_download_nonexistent_hash() {
        let cas_dir = tempdir().unwrap();
        let endpoint = format!("local://{}", cas_dir.path().display());

        let dest_dir = tempdir().unwrap();
        let dest = dest_dir.path().join("empty.bin");

        let session = build_xet_session(XetConfig::new()).unwrap();
        let args = DownloadArgs {
            hash: "0".repeat(64),
            output: Some(dest.clone()),
            source_range: None,
            write_range: None,
            size: None,
        };
        let _ = run_download(&session, &endpoint, &args, false, None).await;
        // LocalClient returns empty reconstruction for unknown hashes;
        // the output file should exist but be empty.
        let content = std::fs::read(&dest).unwrap_or_default();
        assert!(content.is_empty());
    }
}
