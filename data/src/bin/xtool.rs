use std::cmp::min;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::{sink, BufRead, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::Client;
use cas_client::build_http_client;
use cas_object::CompressionScheme;
use clap::{Args, Parser, Subcommand};
use data::data_client::{default_config, READ_BLOCK_SIZE};
use data::migration_tool::hub_client::HubClient;
use data::migration_tool::migrate::migrate_files_impl;
use data::{PointerFile, PointerFileTranslator};
use futures_util::StreamExt;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use merkledb::constants::IDEAL_CAS_BLOCK_SIZE;
use parutils::{tokio_par_for_each, ParallelError};
use rand::Rng;
use reqwest::{Response, Url};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
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
    /// Download from bridge service
    Bridge(BridgeArgs),
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
    endpoint: String,
    /// JWT token secret
    #[clap(short = 't', long)]
    secret: String,
    #[clap(short, long)]
    size: u64,
    #[clap(short, long)]
    num: usize,
}

#[derive(Args)]
struct BridgeArgs {
    #[clap(short, long)]
    endpoint: String,
    /// Secret key
    #[clap(short = 't', long)]
    secret: String,
    #[clap(short, long)]
    manifest: PathBuf,
    #[clap(short, long)]
    num: usize,
    #[clap(short, long)]
    part_size: u64,
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
            Command::Upload(args) => upload_to_cas(args, threadpool, hub_client.repo_id).await,
            Command::Bridge(args) => download_from_bridge(args, threadpool, hub_client.repo_id).await,
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

async fn upload_to_cas(args: UploadArgs, threadpool: Arc<ThreadPool>, repo_id: String) -> Result<()> {
    let token_refresher = Arc::new(UploadTokenRefresher::new(args.secret, repo_id));
    let (config, _tempdir) = default_config(args.endpoint, Some(CompressionScheme::LZ4), None, Some(token_refresher))?;
    let processor = Arc::new(PointerFileTranslator::new(config, threadpool, None, false).await?);
    let vec = (0..args.num).map(|i| format!("file-{i}")).collect();
    tokio_par_for_each(vec, 12, |id, _| async {
        let proc = processor.clone();
        clean_file(&proc, id, args.size).await
    })
    .await
    .map_err(|e| anyhow!("{e:?}"))?;

    processor.finalize_cleaning().await?;
    info!("uploaded xorbs and shards");

    Ok(())
}

async fn clean_file(processor: &Arc<PointerFileTranslator>, id: String, size: u64) -> Result<()> {
    let mut reader = BufReader::new(RandomReader::new(size));
    let path = PathBuf::from(id);
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
    info!("cleaned: {} ({} total, {} new)", pf.hash_string(), pf.filesize(), new_bytes);
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
    fn new(secret_str: String, repo_id: String) -> Self {
        let secret = EncodingKey::from_secret(secret_str.as_bytes());
        UploadTokenRefresher {
            secret,
            repo_id,
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

async fn download_from_bridge(args: BridgeArgs, _threadpool: Arc<ThreadPool>, repo_id: String) -> Result<()> {
    let access_key = "cas";
    let s3_config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::v2024_03_28())
        .force_path_style(true)
        .region(Region::new("us-east-1"))
        .endpoint_url(args.endpoint)
        .credentials_provider(Credentials::new(access_key, args.secret, None, None, "xtool"))
        .build();
    let client = Client::from_conf(s3_config);
    let client = Arc::new(client);

    let files = parse_manifest(args.manifest)?;
    tokio_par_for_each(files, 12, |file, _| {
        let client = client.clone();
        let repo = repo_id.clone();
        async move {
            let start = Instant::now();
            let result = read_file(file.clone(), client, repo, &start).await;
            let elapsed = start.elapsed().as_millis() as u64;
            match result {
                Ok(bytes_read) => {
                    info!(elapsed, file, bytes_read, "Read from remote");
                },
                Err(err) => {
                    error!(elapsed, file, "Failed read from remote: {err:?}");
                },
            }
            Ok(())
        }
    })
    .await
    .map_err(|e: ParallelError<anyhow::Error>| anyhow!("{e:?}"))?;
    info!("read files");

    Ok(())
}

async fn read_file(file: String, client: Arc<Client>, repo: String, start: &Instant) -> Result<u64> {
    let bucket = "download";
    let key = format!("{repo}/{file}");
    let get_req = client.get_object().bucket(bucket).key(key);
    let presigning_config = PresigningConfig::expires_in(Duration::from_secs(600))?;
    let presigned_request = get_req.presigned(presigning_config).await?;
    let uri = presigned_request.uri();

    let resp = reqwest::get(Url::parse(&uri)?).await?;
    let elapsed = start.elapsed().as_millis() as u64;
    let status = resp.status().as_u16();
    info!(elapsed, status, file, "received response");
    if !resp.status().is_success() {
        return Err(anyhow!("bad response: {:?}", resp.text().await?));
    }
    let mut dev_null = sink();
    let mut stream = resp.bytes_stream();
    let mut total_read = 0;
    while let Some(Ok(b)) = stream.next().await {
        let len = b.len() as u64;
        total_read += len;
        let read_elapsed = start.elapsed().as_millis() as u64 - elapsed;
        info!(elapsed, read_elapsed, file, total_read, len, "read next chunk of data");
        dev_null.write_all(&b[..])?;
    }
    Ok(total_read)
}

fn parse_manifest(manifest: PathBuf) -> Result<Vec<String>> {
    let file = File::open(manifest)?;
    let reader = BufReader::new(file);
    Ok(reader.lines().collect::<std::io::Result<Vec<_>>>()?)
}

fn main() -> Result<()> {
    initialize_logging();
    let cli = XCommand::parse();
    let threadpool = Arc::new(ThreadPool::new_with_hardware_parallelism_limit()?);
    let threadpool_internal = threadpool.clone();
    threadpool.external_run_async_task(async move { cli.run(threadpool_internal).await })??;

    Ok(())
}

/// Default log level for the library to use. Override using `RUST_LOG` env variable.
const DEFAULT_LOG_LEVEL: &str = "info";

// example endpoint: http://host.docker.internal:4317
pub fn initialize_logging() {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_file(true)
        .with_target(false)
        .json();

    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(DEFAULT_LOG_LEVEL))
        .unwrap_or_default();

    let name = "xtool".to_string();
    // let otel_layer = config.trace_endpoint.as_ref().map(|endpoint| {
    //     let otlp_exporter = opentelemetry_otlp::new_exporter().tonic().with_endpoint(endpoint);
    //     let tracer = opentelemetry_otlp::new_pipeline()
    //         .tracing()
    //         .with_exporter(otlp_exporter)
    //         .with_trace_config(Config::default().with_resource(Resource::new(vec![
    //             KeyValue::new(opentelemetry_semantic_conventions::resource::SERVICE_NAME, name.clone()),
    //             KeyValue::new(opentelemetry_semantic_conventions::resource::SERVICE_NAMESPACE, "xet"),
    //             KeyValue::new(
    //                 opentelemetry_semantic_conventions::resource::DEPLOYMENT_ENVIRONMENT_NAME,
    //                 config.env.to_string(),
    //             ),
    //         ])))
    //         .install_batch(opentelemetry_sdk::runtime::Tokio)
    //         .unwrap()
    //         .tracer(name);
    //
    //     OpenTelemetryLayer::new(tracer)
    // });

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(filter_layer)
        // .with(otel_layer)
        .init();
}
