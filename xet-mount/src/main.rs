mod fs;

use anyhow::anyhow;
use clap::Parser;
use data::FileDownloader;
use data::data_client::default_config;
use data::migration_tool::hub_client_token_refresher::HubClientTokenRefresher;
use hub_client::{BearerCredentialHelper, HFRepoType, HubClient, HubXetTokenTrait, Operation, RepoInfo};
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use std::ffi::OsStr;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use tokio::signal::unix::SignalKind;
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
    #[clap(short, long)]
    quiet: bool,
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

    let xfs = XetFS::new(hub_client, xet_downloader, args.quiet);

    let listener = NFSTcpListener::bind("127.0.0.1:11111", xfs)
        .await
        .expect("Failed to bind to port 11111");

    let ip = listener.get_listen_ip().to_string();
    let hostport = listener.get_listen_port();

    let task_handle = tokio::spawn(async move { listener.handle_forever().await });

    let mount_path = utils::normalized_path_from_user_string(args.path.as_os_str().to_str().expect("invalid path"));
    let cleanup_dir = !perform_mount(ip, hostport, mount_path.clone()).await?;

    // Set up signal handlers
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt())?;

    let res = tokio::select! {
        result = task_handle => result,
        _ = sigterm.recv() => Ok(Ok(())),
        _ = sigint.recv() => Ok(Ok(())),
    };
    unmount(mount_path.clone(), cleanup_dir).await?;
    res??;
    Ok(())
}

#[cfg(target_os = "macos")]
const MOUNT_BIN: &str = "/sbin/mount";
#[cfg(target_os = "macos")]
const UMOUNT_BIN: &str = "/sbin/umount";

#[cfg(target_os = "linux")]
const MOUNT_BIN: &str = "/usr/bin/mount";
#[cfg(target_os = "linux")]
const UMOUNT_BIN: &str = "/usr/bin/umount";

// on success returns a boolean indicating whether the mount directory existed before being called
async fn perform_mount(ip: String, hostport: u16, mount_path: PathBuf) -> Result<bool, anyhow::Error> {
    eprintln!("Performing mount... on {mount_path:?}");

    let previously_existed = std::fs::exists(&mount_path)?;
    if !previously_existed {
        std::fs::create_dir_all(&mount_path)?;
    }
    let mut cmd = Command::new(MOUNT_BIN);
    cmd.args(["-t", "nfs"]);
    #[cfg(target_os = "macos")]
    cmd.args([
        "-o",
        &format!("rdonly,nolocks,vers=3,tcp,rsize=131072,actimeo=120,port={hostport},mountport={hostport}"),
    ]);
    #[cfg(target_os = "linux")]
    cmd.args([
        "-o",
        &format!("user,noacl,nolock,vers=3,tcp,rsize=131072,actimeo=120,port={hostport},mountport={hostport}"),
    ]);

    cmd.arg(format!("{}:/", &ip)).arg(mount_path.clone());

    if cmd.status().is_err() {
        let mut cmd = Command::new("sudo");
        cmd.arg(MOUNT_BIN);
        cmd.args(["-t", "nfs"]);
        #[cfg(target_os = "macos")]
        cmd.args([
            "-o",
            &format!("rdonly,nolocks,vers=3,tcp,rsize=131072,actimeo=120,port={hostport},mountport={hostport}"),
        ]);
        #[cfg(target_os = "linux")]
        cmd.args([
            "-o",
            &format!("user,noacl,nolock,vers=3,tcp,rsize=131072,actimeo=120,port={hostport},mountport={hostport}"),
        ]);

        cmd.arg(format!("{}:/", &ip)).arg(mount_path);
        cmd.status()?;
    }

    eprintln!("Mounted.");

    Ok(previously_existed)
}

async fn unmount(mount_path: PathBuf, delete_path: bool) -> Result<(), anyhow::Error> {
    eprintln!("Unmounting...");

    let mut cmd = Command::new(UMOUNT_BIN);
    cmd.arg("-f");
    cmd.arg(mount_path.clone());
    if cmd.status().is_err() {
        let mut cmd = Command::new("sudo");
        cmd.arg(UMOUNT_BIN);
        cmd.arg("-f");
        cmd.arg(mount_path.clone());
        cmd.status()?;
    }

    eprintln!("Unmounted.");
    if delete_path {
        std::fs::remove_dir_all(&mount_path)?;
    }

    Ok(())
}

fn run_command_with_root_backup<T: AsRef<OsStr>>(
    program: impl AsRef<OsStr>,
    params: Vec<T>,
) -> Result<(), anyhow::Error> {

}
