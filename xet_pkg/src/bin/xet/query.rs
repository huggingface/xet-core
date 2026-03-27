use std::sync::Arc;

use anyhow::Result;
use clap::Args;
use xet_client::cas_client::Client;
use xet_client::cas_types::{FileRange, QueryReconstructionResponseV2};
use xet_core_structures::merklehash::MerkleHash;

use super::Cli;

#[derive(Args)]
pub struct DumpReconstructionArgs {
    /// Hex-encoded MerkleHash of the file.
    pub hash: String,

    /// Source byte range to query, e.g. "0..1048576" (start inclusive, end exclusive).
    #[arg(long)]
    pub source_range: Option<String>,
}

pub async fn run(cli: &Cli, args: &DumpReconstructionArgs) -> Result<()> {
    let client = super::session::build_cas_client(&cli.resolved_endpoint(), cli.resolved_token()).await?;
    let response = run_query(client, args).await?;
    let json = serde_json::to_string_pretty(&response)?;
    println!("{json}");
    Ok(())
}

fn parse_range(s: &str) -> Result<FileRange> {
    s.parse::<FileRange>()
        .map_err(|e| anyhow::anyhow!("range must be 'start..end' or CAS format, got: {s}: {e}"))
}

pub async fn run_query(
    client: Arc<dyn Client>,
    args: &DumpReconstructionArgs,
) -> Result<Option<QueryReconstructionResponseV2>> {
    let hash = MerkleHash::from_hex(&args.hash).map_err(|e| anyhow::anyhow!("invalid hash '{}': {e}", args.hash))?;
    let range: Option<FileRange> = args.source_range.as_deref().map(parse_range).transpose()?;
    client.get_reconstruction(&hash, range).await.map_err(anyhow::Error::from)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use xet_runtime::config::XetConfig;

    use super::*;
    use crate::session::{build_cas_client, build_xet_session};
    use crate::upload::{UploadArgs, run_upload};

    async fn upload_and_get_hash(cas_dir: &tempfile::TempDir, content: &[u8]) -> (String, String, u64) {
        let endpoint = format!("local://{}", cas_dir.path().display());
        let src_dir = tempdir().unwrap();
        let src = src_dir.path().join("query_test.bin");
        std::fs::write(&src, content).unwrap();

        let session = build_xet_session(&endpoint, None, XetConfig::new()).unwrap();
        let results = run_upload(
            &session,
            &UploadArgs {
                files: vec![src.to_str().unwrap().to_owned()],
                no_sha256: true,
                dump_stats: false,
                output: None,
            },
        )
        .await
        .unwrap();
        let meta = &results[0];
        (endpoint, meta.xet_info.hash.clone(), meta.xet_info.file_size.unwrap_or(0))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_after_upload() {
        let cas_dir = tempdir().unwrap();
        let (endpoint, hash, file_size) = upload_and_get_hash(&cas_dir, &vec![1u8; 4096]).await;

        let client = build_cas_client(&endpoint, None).await.unwrap();
        let args = DumpReconstructionArgs {
            hash,
            source_range: None,
        };
        let response = run_query(client, &args).await.unwrap();
        let r = response.expect("expected Some reconstruction response");
        assert!(!r.terms.is_empty());
        let total_bytes: u64 = r.terms.iter().map(|t| t.unpacked_length as u64).sum();
        assert_eq!(total_bytes, file_size);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_nonexistent_hash() {
        let cas_dir = tempdir().unwrap();
        let endpoint = format!("local://{}", cas_dir.path().display());

        let client = build_cas_client(&endpoint, None).await.unwrap();
        let args = DumpReconstructionArgs {
            hash: "0".repeat(64),
            source_range: None,
        };
        let response = run_query(client, &args).await.unwrap();
        match response {
            None => {},
            Some(r) => assert!(r.terms.is_empty()),
        }
    }
}
