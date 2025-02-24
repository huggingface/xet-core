use std::cmp::min;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use cas_client::build_http_client;
use cas_object::CompressionScheme;
use clap::{Args, Parser, Subcommand};
use data::data_client::{default_config, READ_BLOCK_SIZE};
use data::migration_tool::hub_client::HubClient;
use data::migration_tool::migrate::migrate_files_impl;
use data::{PointerFile, PointerFileTranslator};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use merkledb::constants::IDEAL_CAS_BLOCK_SIZE;
use rand::Rng;
use serde::{Deserialize, Serialize};
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

#[derive(Subcommand)]
enum Command {
    /// Dry-run of file upload to get file info after dedup.
    Dedup(DedupArg),
    /// Queries reconstruction information about a file.
    Query(QueryArg),
    /// Upload data to CAS
    Upload(UploadArgs),
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

#[derive(Args)]
struct UploadArgs {
    #[clap(short, long)]
    size: u64,
    #[clap(short, long)]
    endpoint: String,
    /// JWT token secret
    #[clap(short, long)]
    secret: String,
    #[clap(short, long)]
    repo_id: Option<String>,
}

impl Command {
    async fn run(self, hub_client: HubClient, threadpool: Arc<ThreadPool>) -> Result<()> {
        match self {
            Command::Dedup(arg) => {
                let file_paths = walk_files(arg.files, arg.recursive);
                eprintln!("Dedupping {} files...", file_paths.len());

                let (all_file_info, clean_ret, total_bytes_trans) = migrate_files_impl(
                    file_paths,
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
            Command::Query(_arg) => unimplemented!(),
            Command::Upload(args) => upload_to_cas(args).await,
        }
    }
}

fn walk_files(files: Vec<String>, recursive: bool) -> Vec<String> {
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

    file_paths
}

fn is_git_special_files(path: &str) -> bool {
    matches!(path, ".git" | ".gitignore" | ".gitattributes")
}

async fn upload_to_cas(args: UploadArgs) -> Result<()> {
    let token_refresher = Arc::new(UploadTokenRefresher::new(args.secret, args.repo_id));
    let (config, _tempdir) = default_config(args.endpoint, Some(CompressionScheme::LZ4), None, Some(token_refresher))?;
    let threadpool = Arc::new(ThreadPool::new()?);
    let processor = Arc::new(PointerFileTranslator::new(config, threadpool, None, false).await?);
    let mut reader = BufReader::new(RandomReader::new(args.size));
    let path = PathBuf::from("foo");
    let mut read_buf = vec![0u8; READ_BLOCK_SIZE];
    let handle = processor
        .start_clean(IDEAL_CAS_BLOCK_SIZE / READ_BLOCK_SIZE, Some(&path))
        .await?;
    loop {
        let bytes = reader.read(&mut read_buf)?;
        if bytes == 0 {
            break;
        }

        handle.add_bytes(read_buf[0..bytes].to_vec()).await?;
    }
    let (pf_str, new_bytes) = handle.result().await?;
    let pf = PointerFile::init_from_string(&pf_str, path.to_str().unwrap());
    println!("cleaned: {} ({} total, {} new)", pf.hash_string(), pf.filesize(), new_bytes);

    processor.finalize_cleaning().await?;
    println!("uploaded");

    Ok(())
}

struct RandomReader {
    size: u64,
    bytes_read: u64,
}

impl RandomReader {
    fn new(size: u64) -> Self {
        Self { size, bytes_read: 0 }
    }
}

impl Read for RandomReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.bytes_read >= self.size {
            return Ok(0);
        }
        let remaining = self.size - self.bytes_read;
        let bytes_to_fill = min(buf.len() as u64, remaining) as usize;
        let mut rng = rand::thread_rng();
        rng.fill(&mut buf[..bytes_to_fill]);
        self.bytes_read += bytes_to_fill as u64;
        Ok(bytes_to_fill)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reader() {
        let mut r = RandomReader::new(40);
        let mut buf = [0u8; 30];
        assert_eq!(30, r.read(&mut buf).unwrap());
        assert_eq!(10, r.read(&mut buf).unwrap());
        assert_eq!(0, r.read(&mut buf).unwrap());
    }

    #[test]
    fn test_reader_exact() {
        let mut r = RandomReader::new(40);
        let mut buf = [0u8; 40];
        assert_eq!(40, r.read(&mut buf).unwrap());
        assert_eq!(0, r.read(&mut buf).unwrap());
    }
}

const USER_ID: &str = "xtool-upload";
const DEFAULT_REPO_ID: &str = "11111111";

struct UploadTokenRefresher {
    secret: EncodingKey,
    repo_id: String,
    user_id: String,
}

impl UploadTokenRefresher {
    fn new(secret_str: String, repo_id: Option<String>) -> Self {
        let secret = EncodingKey::from_secret(secret_str.as_bytes());
        UploadTokenRefresher {
            secret,
            repo_id: repo_id.unwrap_or_else(|| DEFAULT_REPO_ID.to_owned()),
            user_id: USER_ID.to_owned(),
        }
    }
}

// EncodingKey isn't Debug, so manually implement
impl Debug for UploadTokenRefresher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StaticTokenRefresher: {}, {}", self.repo_id, self.user_id)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenClaim {
    pub repo_id: String,
    pub user_id: String,
    pub access: String,
    pub exp: usize,
}

impl TokenRefresher for UploadTokenRefresher {
    fn refresh(&self) -> std::result::Result<TokenInfo, AuthError> {
        let exp_time = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap() + Duration::from_secs(3600)).as_secs();
        let claim = TokenClaim {
            repo_id: self.repo_id.clone(),
            user_id: self.user_id.clone(),
            access: "write".to_string(),
            exp: exp_time as usize,
        };
        let header = Header::new(Algorithm::HS256);
        let token = jsonwebtoken::encode(&header, &claim, &self.secret)
            .map_err(|e| AuthError::TokenRefreshFailure(e.to_string()))?;
        Ok((token, exp_time))
    }
}

fn main() -> Result<()> {
    let cli = XCommand::parse();
    let threadpool = Arc::new(ThreadPool::new_with_hardware_parallelism_limit()?);
    let threadpool_internal = threadpool.clone();
    threadpool.external_run_async_task(async move { cli.run(threadpool_internal).await })??;

    Ok(())
}
