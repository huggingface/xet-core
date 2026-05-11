use std::sync::Arc;

use anyhow::Result;
use clap::Args;
use xet_client::cas_client::RemoteClient;
use xet_client::cas_client::auth::TokenRefresher;
use xet_client::cas_types::{FileRange, QueryReconstructionResponse};
use xet_client::hub_client::{HubClient, Operation};
use xet_core_structures::merklehash::MerkleHash;
use xet_data::processing::data_client::default_config;
use xet_data::processing::migration_tool::hub_client_token_refresher::HubClientTokenRefresher;
use xet_runtime::core::XetContext;

use super::endpoint::EndpointConfig;
use super::query::DumpReconstructionArgs;

#[derive(Args)]
pub struct HubQueryArgs {
    /// Hex-encoded MerkleHash of the file.
    pub hash: String,

    /// Query regarding a certain range in bytes: [start, end), specified
    /// in the format of "start-end".
    pub bytes_range: Option<FileRange>,
}

pub async fn run(ctx: &XetContext, ep: &EndpointConfig, args: &HubQueryArgs) -> Result<()> {
    if let Some(ref hub_info) = ep.hub_info {
        // Hub mode: use HubClientTokenRefresher + RemoteClient + v1 API.
        let hub_client = hub_info.build_hub_client(ctx)?;
        let file_hash = MerkleHash::from_hex(&args.hash)?;
        let ret = query_reconstruction_hub(ctx, file_hash, args.bytes_range, hub_client).await?;
        eprintln!("{ret:?}");
    } else {
        // Direct mode: delegate to the dump-reconstruction (v2) code path.
        let recon_args = DumpReconstructionArgs {
            hash: args.hash.clone(),
            source_range: args.bytes_range.as_ref().map(|r| format!("{}..{}", r.start, r.end)),
        };
        super::query::run(ctx, ep, &recon_args).await?;
    }
    Ok(())
}

async fn query_reconstruction_hub(
    ctx: &XetContext,
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
        ctx,
        jwt_info.cas_url.clone(),
        Some((jwt_info.access_token, jwt_info.exp)),
        Some(token_refresher),
        None,
    )?;
    let remote_client = RemoteClient::new(
        ctx.clone(),
        &jwt_info.cas_url,
        &config.session.auth,
        "",
        true,
        config.session.custom_headers.clone(),
    );

    remote_client
        .get_reconstruction_v1(&file_hash, bytes_range)
        .await
        .map_err(Into::into)
}
