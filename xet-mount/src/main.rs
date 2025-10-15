mod fs;

use std::path::PathBuf;

use clap::Parser;
use hub_client::{BearerCredentialHelper, HFRepoType, HubClient, RepoInfo};
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
    #[clap(long, default_value = "HFRepoType::Model")]
    repo_type: HFRepoType,
    #[clap(long, visible_alias = "ref")]
    reference: Option<String>,
    #[clap(short, long)]
    token: Option<String>,
    #[clap(short, long)]
    path: PathBuf,
    #[clap(short, long, visible_alias = "username")]
    namespace: Option<String>,
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

    let hub_client = HubClient::new(
        "https://huggingface.co",
        RepoInfo {
            repo_type: args.repo_type,
            full_name: args.repo_id,
        },
        args.reference,
        user_agent.as_str(),
        session_id.to_string().as_str(),
        cred_helper,
    )?;
    let _xfs = XetFS::new(hub_client);

    Ok(())
}
