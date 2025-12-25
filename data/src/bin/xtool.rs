use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use cas_client::{Client, RemoteClient};
use cas_object::CompressionScheme;
use cas_types::{FileRange, QueryReconstructionResponse};
use clap::{Args, Parser, Subcommand};
use data::data_client::default_config;
use data::migration_tool::hub_client_token_refresher::HubClientTokenRefresher;
use data::migration_tool::migrate::migrate_files_impl;
use hub_client::{BearerCredentialHelper, HubClient, Operation, RepoInfo};
use merklehash::MerkleHash;
use utils::auth::TokenRefresher;
use walkdir::WalkDir;
use xet_runtime::XetRuntime;

const DEFAULT_HF_ENDPOINT: &str = "https://huggingface.co";
const USER_AGENT: &str = concat!("xtool", "/", env!("CARGO_PKG_VERSION"));

#[derive(Parser)]
struct XCommand {
    #[clap(flatten)]
    overrides: CliOverrides,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Args)]
struct CliOverrides {
    /// HF Hub endpoint.
    #[clap(long)]
    endpoint: Option<String>, // if not specified we use env:HF_ENDPOINT
    /// HF Hub access token.
    #[clap(long)]
    token: Option<String>, // if not specified we use env:HF_TOKEN
    /// Type of the associated repo: "model", "dataset", or "space"
    #[clap(long)]
    repo_type: String,
    /// A namespace and a repo name separated by a '/'.
    #[clap(long)]
    repo_id: String,
}

impl XCommand {
    async fn run(self) -> Result<()> {
        let endpoint = self
            .overrides
            .endpoint
            .unwrap_or_else(|| std::env::var("HF_ENDPOINT").unwrap_or(DEFAULT_HF_ENDPOINT.to_owned()));
        let token = self
            .overrides
            .token
            .unwrap_or_else(|| std::env::var("HF_TOKEN").unwrap_or_default());

        let cred_helper = BearerCredentialHelper::new(token, "");
        let hub_client = HubClient::new(
            &endpoint,
            RepoInfo::try_from(&self.overrides.repo_type, &self.overrides.repo_id)?,
            Some("main".to_owned()),
            USER_AGENT,
            "",
            cred_helper,
        )?;

        self.command.run(hub_client).await
    }
}

#[derive(Subcommand)]
enum Command {
    /// Dry-run of file upload to get file info after dedup.
    Dedup(DedupArg),
    /// Queries reconstruction information about a file.
    Query(QueryArg),
}

#[derive(Args)]
struct DedupArg {
    /// Path to the file to dedup.
    files: Vec<String>,
    /// If the paths specified are directories, compute recursively for files
    /// under these directories.
    #[clap(short, long)]
    recursive: bool,
    /// Compute for files sequentially in the order as specified, or as enumerated
    /// from directory walking if in recursive mode. This can be helpful to study
    /// a set of files where there is a temporal relation.
    #[clap(short, long)]
    sequential: bool,
    /// If a file path is specified, write out the JSON formatted file reconstruction info
    /// to the file; otherwise write out to the stdout.
    #[clap(short, long)]
    output: Option<PathBuf>,
    /// The compression scheme to use on XORB upload. Choices are
    /// 0: no compression;
    /// 1: LZ4 compression;
    /// 2: 4 byte groups with LZ4 compression.
    /// If not specified, this will be determined by the repo type.
    #[clap(short, long)]
    compression: Option<u8>,
    /// Migrate the files by actually uploading them to the CAS server.
    #[clap(short, long)]
    migrate: bool,
}

#[derive(Args)]
struct QueryArg {
    /// Xet-hash of a file.
    hash: String,
    /// Query regarding a certain range in bytes: [start, end), specified
    /// in the format of "start-end".
    bytes_range: Option<FileRange>,
}

impl Command {
    async fn run(self, hub_client: HubClient) -> Result<()> {
        match self {
            Command::Dedup(arg) => {
                let file_paths = walk_files(arg.files, arg.recursive);
                eprintln!("Dedupping {} files...", file_paths.len());

                let (all_file_info, clean_ret, total_bytes_trans) = migrate_files_impl(
                    file_paths,
                    None,
                    arg.sequential,
                    hub_client,
                    None,
                    arg.compression.and_then(|c| CompressionScheme::try_from(c).ok()),
                    !arg.migrate,
                )
                .await?;

                // Print file info for analysis
                if !arg.migrate {
                    let mut writer: Box<dyn Write> = if let Some(path) = arg.output {
                        Box::new(BufWriter::new(File::options().create(true).write(true).truncate(true).open(path)?))
                    } else {
                        Box::new(std::io::stdout())
                    };
                    serde_json::to_writer(&mut writer, &all_file_info)?;
                    writer.flush()?;
                }

                eprintln!("\n\nClean results:");
                for (xf, new_bytes) in clean_ret {
                    println!("{}: {} bytes -> {} bytes", xf.hash(), xf.file_size(), new_bytes);
                }

                eprintln!("Transmitted {total_bytes_trans} bytes in total.");

                Ok(())
            },
            Command::Query(arg) => {
                let file_hash = MerkleHash::from_hex(&arg.hash)?;
                let ret = query_reconstruction(file_hash, arg.bytes_range, hub_client).await?;

                eprintln!("{ret:?}");

                Ok(())
            },
        }
    }
}

fn walk_files(files: Vec<String>, recursive: bool) -> Vec<String> {
    // Scan all files if under recursive mode
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

async fn query_reconstruction(
    file_hash: MerkleHash,
    bytes_range: Option<FileRange>,
    hub_client: HubClient,
) -> Result<Option<QueryReconstructionResponse>> {
    let operation = Operation::Download;
    let jwt_info = hub_client.get_cas_jwt(operation).await?;
    let token_refresher = Arc::new(HubClientTokenRefresher {
        operation,
        client: Arc::new(hub_client),
    }) as Arc<dyn TokenRefresher>;

    let config = default_config(
        jwt_info.cas_url.clone(),
        None,
        Some((jwt_info.access_token, jwt_info.exp)),
        Some(token_refresher),
        USER_AGENT.to_string(),
    )?;
    let cas_storage_config = &config.data_config;
    let remote_client =
        RemoteClient::new(&jwt_info.cas_url, &cas_storage_config.auth, "", true, &cas_storage_config.user_agent);

    remote_client
        .get_reconstruction(&file_hash, bytes_range)
        .await
        .map_err(anyhow::Error::from)
}

fn main() -> Result<()> {
    let cli = XCommand::parse();
    let threadpool = XetRuntime::new()?;
    threadpool.external_run_async_task(async move { cli.run().await })??;

    Ok(())
}
