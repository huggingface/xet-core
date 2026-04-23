use std::sync::Arc;

use anyhow::Result;
use clap::Args;
use xet_client::cas_client::Client;
use xet_client::cas_types::{FileRange, QueryReconstructionResponseV2};
use xet_core_structures::merklehash::MerkleHash;
use xet_runtime::core::XetContext;

use super::endpoint::EndpointConfig;

#[derive(Args)]
pub struct DumpReconstructionArgs {
    /// Hex-encoded MerkleHash of the file.
    pub hash: String,

    /// Source byte range to query, e.g. "0..1048576" (start inclusive, end exclusive).
    #[arg(long)]
    pub source_range: Option<String>,
}

pub async fn run(ctx: &XetContext, ep: &EndpointConfig, args: &DumpReconstructionArgs) -> Result<()> {
    let client = super::session::build_cas_client(ctx, &ep.cas_endpoint, ep.token.clone()).await?;
    let response = run_query(client, args).await?;
    let json = serde_json::to_string_pretty(&response)?;
    println!("{json}");
    Ok(())
}

fn parse_range(s: &str) -> Result<FileRange> {
    let (start, end) = super::parse_byte_range(s)?;
    Ok(FileRange::new(start, end))
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
    use xet_runtime::core::XetContext;

    use super::*;
    use crate::download::tests::upload_test_file;
    use crate::session::build_cas_client;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_after_upload() {
        let cas_dir = tempdir().unwrap();
        let (endpoint, hash, file_size) = upload_test_file(&cas_dir, "query_test.bin", &vec![1u8; 4096]).await;

        let ctx = XetContext::default().unwrap();
        let client = build_cas_client(&ctx, &endpoint, None).await.unwrap();
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

        let ctx = XetContext::default().unwrap();
        let client = build_cas_client(&ctx, &endpoint, None).await.unwrap();
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
