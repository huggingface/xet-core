mod dedup;
mod download;
mod endpoint;
mod hub_query;
mod query;
mod session;
mod stats;
mod upload;

use anyhow::Result;
use clap::{Parser, Subcommand};
use dedup::DedupArgs;
use download::DownloadArgs;
use hub_query::HubQueryArgs;
use query::DumpReconstructionArgs;
use stats::ScanArgs;
use upload::UploadArgs;
use xet_client::hub_client::Operation;
use xet_runtime::core::{XetContext, XetRuntime};

use crate::endpoint::EndpointConfig;

const DEFAULT_HF_ENDPOINT: &str = "https://huggingface.co";

/// Xet developer tool for uploading, downloading, inspecting, and migrating files.
#[derive(Parser)]
#[command(name = "xtool", version)]
pub struct Cli {
    /// CAS endpoint URL or local path (direct mode), or Hub endpoint (Hub mode).
    ///
    /// In direct mode (no --repo-type/--repo-id): this is the CAS endpoint.
    /// Accepts https:// URLs, absolute paths (auto-prefixed with local://),
    /// or explicit local:// URLs.
    ///
    /// In Hub mode (--repo-type + --repo-id): this is the Hub endpoint
    /// (default: https://huggingface.co). The CAS endpoint is resolved
    /// via the Hub's JWT mechanism.
    ///
    /// Falls back to HF_ENDPOINT env var, then https://huggingface.co.
    #[arg(long, global = true)]
    pub endpoint: Option<String>,

    /// Auth token (env: HF_TOKEN). Used for CAS auth in direct mode,
    /// or Hub auth in Hub mode.
    #[arg(long, global = true)]
    pub token: Option<String>,

    /// Repo type: "model", "dataset", or "space".
    /// When provided (with --repo-id), enables Hub mode: the CAS endpoint
    /// and auth are resolved from the Hub.
    #[arg(long, global = true)]
    pub repo_type: Option<String>,

    /// Repo as namespace/name (e.g. "org/model-name").
    /// Required when --repo-type is set.
    #[arg(long, global = true)]
    pub repo_id: Option<String>,

    /// Suppress informational output; only errors are printed to stderr.
    #[arg(long, short, global = true)]
    pub quiet: bool,

    /// Override a xet_config value. May be repeated.
    #[arg(short = 'c', long = "config", global = true, value_name = "KEY=VALUE")]
    pub config_overrides: Vec<String>,

    #[command(subcommand)]
    pub command: TopLevel,
}

#[derive(Subcommand)]
pub enum TopLevel {
    /// File-level operations: upload, download, scan, dump-reconstruction.
    File {
        #[command(subcommand)]
        command: FileCommands,
    },
    /// Dry-run or real file upload with dedup metrics.
    Dedup(DedupArgs),
    /// Query reconstruction information about a file.
    Query(HubQueryArgs),
}

#[derive(Subcommand)]
pub enum FileCommands {
    /// Upload one or more files (or stdin) to the CAS endpoint.
    Upload(UploadArgs),
    /// Download a file by its xet hash.
    Download(DownloadArgs),
    /// Dry-run dedup and compression analysis (no upload).
    Scan(ScanArgs),
    /// Show reconstruction metadata for a file hash (JSON).
    DumpReconstruction(DumpReconstructionArgs),
}

impl Cli {
    fn resolved_endpoint(&self) -> String {
        resolve_endpoint(self.endpoint.as_deref(), std::env::var("HF_ENDPOINT").ok().as_deref())
    }

    fn resolved_token(&self) -> Option<String> {
        resolve_token(self.token.as_deref(), std::env::var("HF_TOKEN").ok().as_deref())
    }

    fn is_hub_mode(&self) -> bool {
        self.repo_type.is_some() || self.repo_id.is_some()
    }
}

fn resolve_endpoint(cli_endpoint: Option<&str>, env_endpoint: Option<&str>) -> String {
    let raw = cli_endpoint.or(env_endpoint).unwrap_or(DEFAULT_HF_ENDPOINT);
    normalize_endpoint(raw)
}

fn resolve_token(cli_token: Option<&str>, env_token: Option<&str>) -> Option<String> {
    cli_token
        .map(str::to_owned)
        .or_else(|| env_token.map(str::to_owned))
        .filter(|token| !token.is_empty())
}

/// Parse a range string like "32..64" or "32.." into start and end values.
/// Open-ended "N.." produces `N..u64::MAX` (interpreted as "to end of file").
pub fn parse_byte_range(s: &str) -> anyhow::Result<(u64, u64)> {
    let (start_s, end_s) = s
        .split_once("..")
        .ok_or_else(|| anyhow::anyhow!("range must be START..END or START.., got: {s}"))?;
    let start: u64 = start_s
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid range start '{start_s}': {e}"))?;
    let end: u64 = if end_s.is_empty() {
        u64::MAX
    } else {
        end_s.parse().map_err(|e| anyhow::anyhow!("invalid range end '{end_s}': {e}"))?
    };
    if start > end {
        anyhow::bail!("range start ({start}) must be <= end ({end})");
    }
    Ok((start, end))
}

/// Normalizes an endpoint string: absolute filesystem paths get a `local://` prefix.
pub fn normalize_endpoint(raw: &str) -> String {
    if raw.contains("://") {
        raw.to_owned()
    } else if std::path::Path::new(raw).is_absolute() || (raw.starts_with('/') && !raw.starts_with("//")) {
        format!("local://{raw}")
    } else {
        raw.to_owned()
    }
}

/// Determine the CAS operation type for the command so we request the right JWT scope.
fn operation_for_command(cmd: &TopLevel) -> Operation {
    match cmd {
        TopLevel::File { command } => match command {
            FileCommands::Upload(_) => Operation::Upload,
            FileCommands::Download(_) => Operation::Download,
            FileCommands::Scan(_) => Operation::Upload,
            FileCommands::DumpReconstruction(_) => Operation::Download,
        },
        TopLevel::Dedup(_) => Operation::Upload,
        TopLevel::Query(_) => Operation::Download,
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut config = xet_runtime::config::XetConfig::new();
    for kv in &cli.config_overrides {
        let (key, val) = kv
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--config must be KEY=VALUE, got: {kv}"))?;
        config = config.with_config(key, val)?;
    }

    if let TopLevel::Dedup(ref args) = cli.command {
        if let Some(c) = args.compression {
            use xet_core_structures::xorb_object::CompressionScheme;
            let scheme = CompressionScheme::try_from(c).map_err(|_| {
                anyhow::anyhow!(
                    "Invalid compression value {c}; expected one of: 0 (none), 1 (lz4), 2 (bg4-lz4), 99 (auto)"
                )
            })?;
            config
                .xorb
                .compression_policy
                .try_set(<&str>::from(scheme))
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
    }

    let runtime = XetRuntime::new(&config).map_err(|e| anyhow::anyhow!(e))?;
    let ctx = XetContext::new(config, runtime.clone());

    let cli = std::sync::Arc::new(cli);

    runtime.external_run_async_task({
        let cli = cli.clone();
        async move {
            let operation = operation_for_command(&cli.command);
            let endpoint_config = EndpointConfig::resolve(&cli, &ctx, operation).await?;

            match &cli.command {
                TopLevel::File { command } => match command {
                    FileCommands::Upload(args) => upload::run(&cli, &ctx, &endpoint_config, args).await,
                    FileCommands::Download(args) => download::run(&cli, &ctx, &endpoint_config, args).await,
                    FileCommands::Scan(args) => stats::run(&cli, &ctx, &endpoint_config, args).await,
                    FileCommands::DumpReconstruction(args) => query::run(&ctx, &endpoint_config, args).await,
                },
                TopLevel::Dedup(args) => dedup::run(&cli, &ctx, &endpoint_config, args).await,
                TopLevel::Query(args) => hub_query::run(&ctx, &endpoint_config, args).await,
            }
        }
    })?
}

#[cfg(test)]
mod tests {
    use super::{normalize_endpoint, parse_byte_range, resolve_endpoint, resolve_token};

    #[test]
    fn test_normalize_endpoint() {
        let cases = [
            ("/tmp/cas", "local:///tmp/cas"),
            ("/", "local:///"),
            ("local:///tmp/cas", "local:///tmp/cas"),
            ("https://cas.example.com", "https://cas.example.com"),
            ("http://localhost:8080", "http://localhost:8080"),
            ("relative/path", "relative/path"),
        ];
        for (input, expected) in cases {
            assert_eq!(normalize_endpoint(input), expected);
        }

        #[cfg(windows)]
        assert_eq!(normalize_endpoint("C:\\tmp\\cas"), "local://C:\\tmp\\cas");
    }

    #[test]
    fn test_resolve_endpoint_precedence() {
        assert_eq!(
            resolve_endpoint(Some("https://flag.example.com"), Some("https://env.example.com"),),
            "https://flag.example.com"
        );
        assert_eq!(resolve_endpoint(None, Some("https://env.example.com")), "https://env.example.com");
        assert_eq!(resolve_endpoint(None, None), "https://huggingface.co");
    }

    #[test]
    fn test_parse_byte_range() {
        assert_eq!(parse_byte_range("0..1024").unwrap(), (0, 1024));
        assert_eq!(parse_byte_range("100..").unwrap(), (100, u64::MAX));
        assert!(parse_byte_range("1024..0").is_err());
        assert!(parse_byte_range("abc..100").is_err());
        assert!(parse_byte_range("no_dots").is_err());
    }

    #[test]
    fn test_resolve_token_precedence() {
        assert_eq!(resolve_token(Some("flag-token"), Some("env-token")), Some("flag-token".to_owned()));
        assert_eq!(resolve_token(None, Some("env-token")), Some("env-token".to_owned()));
        assert_eq!(resolve_token(Some(""), Some("env-token")), None);
    }
}
