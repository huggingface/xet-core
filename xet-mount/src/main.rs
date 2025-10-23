mod fs;

use std::{path::PathBuf, process::Command, sync::Arc};

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
    #[clap(long, short = 't', default_value = "model")]
    repo_type: HFRepoType,
    #[clap(long, visible_alias = "ref", default_value = "main")]
    reference: Option<String>,
    #[clap(long)]
    token: Option<String>,
    #[clap(long)]
    path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = MountArgs::parse();
    eprintln!("{:?}", args);

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

    let listener = NFSTcpListener::bind("127.0.0.1:11111", xfs)
        .await
        .expect("Failed to bind to port 11111");

    let ip = listener.get_listen_ip().to_string();
    let hostport = listener.get_listen_port();

    let task_handle = tokio::spawn(async move { listener.handle_forever().await });

    let mount_path = utils::normalized_path_from_user_string(args.path.as_os_str().to_str().expect("invalid path"));
    perform_mount(ip, hostport, mount_path).await?;

    task_handle.await??;

    Ok(())
}

async fn perform_mount(ip: String, hostport: u16, mount_path: PathBuf) -> Result<(), anyhow::Error> {
    eprintln!("Performing mount...");
    std::fs::create_dir_all(&mount_path)?;
    let mut cmd = Command::new("/sbin/mount");
    cmd.args(["-t", "nfs"]);
    cmd.args([
        "-o",
        &format!("rdonly,nolocks,vers=3,tcp,rsize=131072,actimeo=120,port={hostport},mountport={hostport}"),
    ]);

    cmd.arg(format!("{}:/", &ip)).arg(mount_path);

    cmd.status()?;

    Ok(())
}
