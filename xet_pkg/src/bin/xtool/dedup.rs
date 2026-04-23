use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use anyhow::Result;
use clap::Args;
use tracing::{Instrument, info_span};
use walkdir::WalkDir;
use xet_data::processing::data_client::{clean_file, default_config};
use xet_data::processing::migration_tool::migrate::migrate_files_impl;
use xet_data::processing::{FileUploadSession, Sha256Policy, XetFileInfo};
use xet_runtime::core::XetContext;
use xet_runtime::core::par_utils::run_constrained;

use super::Cli;
use super::endpoint::EndpointConfig;

#[derive(Args)]
pub struct DedupArgs {
    /// Path to the file(s) to dedup.
    pub files: Vec<String>,

    /// If the paths specified are directories, compute recursively for files
    /// under these directories.
    #[arg(short, long)]
    pub recursive: bool,

    /// Compute for files sequentially in the order as specified, or as enumerated
    /// from directory walking if in recursive mode.
    #[arg(short, long)]
    pub sequential: bool,

    /// If a file path is specified, write out the JSON formatted file reconstruction info
    /// to the file; otherwise write out to stdout.
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// The compression scheme to use on XORB upload. Choices are
    /// 0: no compression;
    /// 1: LZ4 compression;
    /// 2: 4 byte groups with LZ4 compression.
    /// If not specified, this will be determined by the repo type.
    #[arg(short, long)]
    pub compression: Option<u8>,

    /// Migrate the files by actually uploading them to the CAS server.
    #[arg(short, long)]
    pub migrate: bool,
}

pub async fn run(cli: &Cli, ctx: &XetContext, ep: &EndpointConfig, args: &DedupArgs) -> Result<()> {
    let file_paths = walk_files(args.files.clone(), args.recursive);
    eprintln!("Dedupping {} files...", file_paths.len());

    let dry_run = !args.migrate;

    let (all_file_info, clean_ret, total_bytes_trans) = if let Some(ref hub_info) = ep.hub_info {
        // Hub mode: build a fresh HubClient and pass it to migrate_files_impl.
        let hub_client = hub_info.build_hub_client(ctx)?;
        migrate_files_impl(ctx, file_paths, None, args.sequential, hub_client, None, dry_run).await?
    } else {
        // Direct mode: replicate migrate_files_impl logic without HubClient.
        run_dedup_direct(ctx, ep, file_paths, args.sequential, dry_run).await?
    };

    if dry_run {
        let mut writer: Box<dyn Write> = if let Some(ref path) = args.output {
            Box::new(BufWriter::new(File::options().create(true).write(true).truncate(true).open(path)?))
        } else {
            Box::new(std::io::stdout())
        };
        serde_json::to_writer(&mut writer, &all_file_info)?;
        writer.flush()?;
    }

    if !cli.quiet {
        eprintln!("\n\nClean results:");
        for (xf, new_bytes) in &clean_ret {
            println!(
                "{}: {} bytes -> {} bytes",
                xf.hash(),
                xf.file_size().map_or("?".to_string(), |s| s.to_string()),
                new_bytes
            );
        }
        eprintln!("Transmitted {total_bytes_trans} bytes in total.");
    }

    Ok(())
}

type DedupResult = (Vec<xet_core_structures::metadata_shard::file_structs::MDBFileInfo>, Vec<(XetFileInfo, u64)>, u64);

async fn run_dedup_direct(
    ctx: &XetContext,
    ep: &EndpointConfig,
    file_paths: Vec<String>,
    sequential: bool,
    dry_run: bool,
) -> Result<DedupResult> {
    let token_info = ep.token.as_ref().map(|t| (t.clone(), u64::MAX));

    let config = default_config(ctx, ep.cas_endpoint.clone(), token_info, None, None)?;

    let num_workers = if sequential {
        1
    } else {
        ctx.runtime.num_worker_threads()
    };
    let processor = if dry_run {
        FileUploadSession::dry_run(config.into()).await?
    } else {
        FileUploadSession::new(config.into()).await?
    };

    let sha256_policies = vec![Sha256Policy::Compute; file_paths.len()];

    let clean_futs = file_paths.into_iter().zip(sha256_policies).map(|(file_path, policy)| {
        let proc = processor.clone();
        async move {
            let (pf, metrics) = clean_file(proc, file_path, policy).await?;
            Ok::<(XetFileInfo, u64), xet_data::error::DataError>((pf, metrics.new_bytes))
        }
        .instrument(info_span!("clean_file"))
    });
    let clean_ret = run_constrained(clean_futs, num_workers).await?;

    if dry_run {
        let (metrics, all_file_info) = processor.finalize_with_file_info().await?;
        Ok((all_file_info, clean_ret, metrics.total_bytes_uploaded))
    } else {
        let metrics = processor.finalize().await?;
        Ok((vec![], clean_ret, metrics.total_bytes_uploaded as u64))
    }
}

fn walk_files(files: Vec<String>, recursive: bool) -> Vec<String> {
    if recursive {
        files
            .iter()
            .flat_map(|dir| {
                WalkDir::new(dir)
                    .follow_links(false)
                    .max_depth(usize::MAX)
                    .into_iter()
                    .filter_entry(|e| !is_git_special_files(e.file_name().to_str().unwrap_or_default()))
                    .flatten()
                    .filter(|e| {
                        e.file_type().is_file() && !is_git_special_files(e.file_name().to_str().unwrap_or_default())
                    })
                    .filter_map(|e| e.path().to_str().map(|s| s.to_owned()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    } else {
        files
    }
}

fn is_git_special_files(path: &str) -> bool {
    matches!(path, ".git" | ".gitignore" | ".gitattributes")
}
