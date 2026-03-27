mod download;
mod query;
mod session;
mod stats;
mod upload;

use anyhow::Result;
use clap::{Parser, Subcommand};
use download::DownloadArgs;
use query::DumpReconstructionArgs;
use stats::ScanArgs;
use upload::UploadArgs;

const DEFAULT_HF_ENDPOINT: &str = "https://huggingface.co";

/// Xet CAS developer tool for uploading, downloading, and inspecting files.
#[derive(Parser)]
#[command(name = "xet", version)]
pub struct Cli {
    /// CAS endpoint URL or local path (env: HF_ENDPOINT).
    ///
    /// Accepts https:// URLs for remote servers, absolute filesystem
    /// paths (auto-prefixed with local://), or explicit local:// URLs.
    /// Defaults to HF_ENDPOINT env var, then https://huggingface.co.
    #[arg(long, global = true)]
    pub endpoint: Option<String>,

    /// Auth token for remote endpoints (env: HF_TOKEN).
    ///
    /// Falls back to HF_TOKEN env var. Not needed for local endpoints.
    #[arg(long, global = true)]
    pub token: Option<String>,

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
    /// Resolve the endpoint to a canonical form:
    /// - absolute paths are prefixed with "local://"
    /// - local:// URLs are returned as-is
    /// - https:// URLs are returned as-is
    /// - None falls back to HF_ENDPOINT env var or the HF default
    pub fn resolved_endpoint(&self) -> String {
        let raw = self
            .endpoint
            .clone()
            .unwrap_or_else(|| std::env::var("HF_ENDPOINT").unwrap_or_else(|_| DEFAULT_HF_ENDPOINT.to_owned()));
        normalize_endpoint(&raw)
    }

    /// Resolve the token: --token flag, then HF_TOKEN env var, then None.
    pub fn resolved_token(&self) -> Option<String> {
        self.token
            .clone()
            .or_else(|| std::env::var("HF_TOKEN").ok())
            .filter(|t| !t.is_empty())
    }
}

/// Normalizes an endpoint string: absolute filesystem paths get a `local://` prefix.
pub fn normalize_endpoint(raw: &str) -> String {
    if raw.contains("://") {
        raw.to_owned()
    } else if raw.starts_with('/') {
        format!("local://{raw}")
    } else {
        raw.to_owned()
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

    let runtime = xet_runtime::core::XetRuntime::new_with_config(config.clone())?;

    let cli = std::sync::Arc::new(cli);

    runtime.external_run_async_task({
        let cli = cli.clone();
        async move {
            match &cli.command {
                TopLevel::File { command } => match command {
                    FileCommands::Upload(args) => upload::run(&cli, config.clone(), args).await,
                    FileCommands::Download(args) => download::run(&cli, config.clone(), args).await,
                    FileCommands::Scan(args) => stats::run(&cli, args).await,
                    FileCommands::DumpReconstruction(args) => query::run(&cli, args).await,
                },
            }
        }
    })?
}

#[cfg(test)]
mod tests {
    use super::normalize_endpoint;

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
            assert_eq!(normalize_endpoint(input), expected, "input: {input}");
        }
    }
}
