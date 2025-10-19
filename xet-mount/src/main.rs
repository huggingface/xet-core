mod fs;

use std::sync::Arc;

use clap::Parser;
use data::FileDownloader;
use data::data_client::default_config;
use data::migration_tool::hub_client_token_refresher::HubClientTokenRefresher;
use hub_client::{BearerCredentialHelper, HFRepoType, HubClient, HubXetTokenTrait, Operation, RepoInfo};
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use uuid::Uuid;

use crate::fs::XetFS;

#[derive(Parser, Debug)]
#[command(
    name = "xet-mount",
    version,
    about = "Mount a Hugging Face repository to a local directory"
)]
struct MountArgs {
    #[clap(short, long)]
    repo_id: String,
    #[clap(long, short = 't', default_value = "HFRepoType::Model")]
    repo_type: HFRepoType,
    #[clap(long, visible_alias = "ref")]
    reference: Option<String>,
    #[clap(short, long)]
    token: Option<String>,
    // #[clap(short, long)]
    // path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = MountArgs::parse();
    println!("{:?}", args);

    let session_id = Uuid::new_v4();
    let user_agent = format!("xet-mount/{}", env!("CARGO_PKG_VERSION"));

    let Some(token) = args.token.or_else(|| std::env::var("HF_TOKEN").ok()) else {
        return Err("HF_TOKEN is not set".into());
    };

    let cred_helper = BearerCredentialHelper::new(token, "");

    let hub_client = Arc::new(HubClient::new(
        "https://huggingface.co",
        RepoInfo {
            repo_type: args.repo_type,
            full_name: args.repo_id,
        },
        args.reference,
        user_agent.as_str(),
        session_id.to_string().as_str(),
        cred_helper,
    )?);
    let jwt_info = hub_client.get_xet_token(Operation::Download).await?;
    let token_refresher = HubClientTokenRefresher {
        operation: Operation::Download,
        client: hub_client.clone(),
    };
    let config = default_config(
        jwt_info.cas_url,
        None,
        Some((jwt_info.access_token, jwt_info.exp)),
        Some(Arc::new(token_refresher)),
    )?;
    let xet_downloader = FileDownloader::new(Arc::new(config)).await?;

    let xfs = XetFS::new(hub_client, xet_downloader);

    let x = NFSTcpListener::bind("127.0.0.1:11111", xfs)
        .await
        .expect("Failed to bind to port 11111");

    x.handle_forever().await?;
    Ok(())
}
