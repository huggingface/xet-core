use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use cas_client::build_http_client;
use cas_object::CompressionScheme;
use clap::{Args, Parser, Subcommand};
use data::data_client::{clean_file, default_config, xorb_compression_for_repo_type};
use data::errors::DataProcessingError;
use data::{PointerFile, PointerFileTranslator};
use mdb_shard::file_structs::MDBFileInfo;
use parutils::{tokio_par_for_each, ParallelError};
use reqwest_middleware::ClientWithMiddleware;
use utils::auth::{TokenInfo, TokenRefresher};
use utils::errors::AuthError;
use walkdir::WalkDir;
use xet_threadpool::ThreadPool;

const DEFAULT_HF_ENDPOINT: &str = "https://huggingface.co";

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
    async fn run(self, threadpool: Arc<ThreadPool>) -> Result<()> {
        let endpoint = self
            .overrides
            .endpoint
            .unwrap_or_else(|| std::env::var("HF_ENDPOINT").unwrap_or(DEFAULT_HF_ENDPOINT.to_owned()));
        let token = self
            .overrides
            .token
            .unwrap_or_else(|| std::env::var("HF_TOKEN").unwrap_or_default());
        let hub_client = HubClient {
            endpoint,
            token,
            repo_type: self.overrides.repo_type,
            repo_id: self.overrides.repo_id,
            client: build_http_client(&None)?,
        };

        self.command.run(hub_client, threadpool).await
    }
}

#[derive(Debug)]
struct HubClient {
    endpoint: String,
    token: String,
    repo_type: String,
    repo_id: String,
    client: ClientWithMiddleware,
}

impl HubClient {
    // Get CAS access token from Hub access token. "token_type" is either "read" or "write".
    async fn get_jwt_token(&self, token_type: &str) -> Result<(String, String, u64)> {
        let endpoint = self.endpoint.as_str();
        let repo_type = self.repo_type.as_str();
        let repo_id = self.repo_id.as_str();

        let url = format!("{endpoint}/api/{repo_type}s/{repo_id}/xet-{token_type}-token/main");

        let response = self
            .client
            .get(url)
            .bearer_auth(&self.token)
            .header("user-agent", "xtool")
            .send()
            .await?;

        let headers = response.headers();
        let cas_endpoint = headers["X-Xet-Cas-Url"].to_str()?.to_owned();
        let jwt_token: String = headers["X-Xet-Access-Token"].to_str()?.to_owned();
        let jwt_token_expiry: u64 = headers["X-Xet-Token-Expiration"].to_str()?.parse()?;

        Ok((cas_endpoint, jwt_token, jwt_token_expiry))
    }

    async fn refresh_jwt_token(&self, token_type: &str) -> Result<(String, u64)> {
        let (_, jwt_token, jwt_token_expiry) = self.get_jwt_token(token_type).await?;

        Ok((jwt_token, jwt_token_expiry))
    }
}

#[derive(Debug)]
struct HubClientTokenRefresher {
    threadpool: Arc<ThreadPool>,
    token_type: String,
    client: Arc<HubClient>,
}

impl TokenRefresher for HubClientTokenRefresher {
    fn refresh(&self) -> std::result::Result<TokenInfo, AuthError> {
        let client = self.client.clone();
        let token_type = self.token_type.clone();
        let ret = self
            .threadpool
            .external_run_async_task(async move { client.refresh_jwt_token(&token_type).await })
            .map_err(|e| AuthError::TokenRefreshFailure(e.to_string()))?
            .map_err(|e| AuthError::TokenRefreshFailure(e.to_string()))?;
        Ok(ret)
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
    /// Xet-hash of a file
    hash: String,
}

impl Command {
    async fn run(self, hub_client: HubClient, threadpool: Arc<ThreadPool>) -> Result<()> {
        match self {
            Command::Dedup(arg) => {
                let (all_file_info, clean_ret, total_bytes_trans) = dedup_files(
                    arg.files,
                    arg.recursive,
                    arg.sequential,
                    hub_client,
                    threadpool,
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
                for (pf, new_bytes) in clean_ret {
                    println!("{}: {} bytes -> {} bytes", pf.hash_string(), pf.filesize(), new_bytes);
                }

                eprintln!("Transmitted {total_bytes_trans} bytes in total.");

                Ok(())
            },
            Command::Query(arg) => query_file(arg.hash, hub_client, threadpool),
        }
    }
}

fn main() -> Result<()> {
    let cli = XCommand::parse();
    let threadpool = Arc::new(ThreadPool::new_with_hardware_parallelism_limit()?);
    let threadpool_internal = threadpool.clone();
    threadpool.external_run_async_task(async move { cli.run(threadpool_internal).await })??;

    Ok(())
}

async fn dedup_files(
    files: Vec<String>,
    recursive: bool,
    sequential: bool,
    hub_client: HubClient,
    threadpool: Arc<ThreadPool>,
    compression: Option<CompressionScheme>,
    dry_run: bool,
) -> Result<(Vec<MDBFileInfo>, Vec<(PointerFile, u64)>, u64)> {
    let compression = compression.unwrap_or_else(|| xorb_compression_for_repo_type(&hub_client.repo_type));
    eprintln!("Using {compression} compression");

    let token_type = "write";
    let (endpoint, jwt_token, jwt_token_expiry) = hub_client.get_jwt_token(token_type).await?;
    let token_refresher = Arc::new(HubClientTokenRefresher {
        threadpool: threadpool.clone(),
        token_type: token_type.to_owned(),
        client: Arc::new(hub_client),
    }) as Arc<dyn TokenRefresher>;

    let (config, _tempdir) =
        default_config(endpoint, Some(compression), Some((jwt_token, jwt_token_expiry)), Some(token_refresher))?;

    let num_workers = if sequential { 1 } else { threadpool.num_worker_threads() };
    let processor = if dry_run {
        Arc::new(PointerFileTranslator::dry_run(config, threadpool, None, false).await?)
    } else {
        Arc::new(PointerFileTranslator::new(config, threadpool, None, false).await?)
    };

    // Scan all files if under recursive mode
    let file_paths = if recursive {
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
    };

    eprintln!("Dedupping {} files...", file_paths.len());

    let clean_ret = tokio_par_for_each(file_paths, num_workers, |f, _| async {
        let proc = processor.clone();
        clean_file(&proc, f).await
    })
    .await
    .map_err(|e| match e {
        ParallelError::JoinError => DataProcessingError::InternalError("Join error".to_string()),
        ParallelError::TaskError(e) => e,
    })?;

    let total_bytes_trans = processor.finalize_cleaning().await?;

    if dry_run {
        let all_file_info = processor.summarize_file_info_of_session().await?;
        Ok((all_file_info, clean_ret, total_bytes_trans))
    } else {
        Ok((vec![], clean_ret, total_bytes_trans))
    }
}

fn is_git_special_files(path: &str) -> bool {
    matches!(path, ".git" | ".gitignore" | ".gitattributes")
}

fn query_file(_hash: String, _hub_client: HubClient, _threadpool: Arc<ThreadPool>) -> Result<()> {
    todo!()
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use cas_client::build_http_client;

    use crate::HubClient;

    #[tokio::test]
    #[ignore = "need valid token"]
    async fn test_get_jwt_token() -> Result<()> {
        let hub_client = HubClient {
            endpoint: "https://xethub-poc.us.dev.moon.huggingface.tech".to_owned(),
            token: "[MASKED]".to_owned(),
            repo_type: "dataset".to_owned(),
            repo_id: "test/t2".to_owned(),
            client: build_http_client(&None)?,
        };

        let (cas_endpoint, jwt_token, jwt_token_expiry) = hub_client.get_jwt_token("read").await?;

        println!("{cas_endpoint}, {jwt_token}, {jwt_token_expiry}");

        println!("{:?}", hub_client.refresh_jwt_token("write").await?);

        Ok(())
    }
}
