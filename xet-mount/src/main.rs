mod fs;

use clap::Parser;
use hub_client::HFRepoType;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "xet-mount", about = "Mount a Hugging Face repository to a local directory")]
struct MountArgs {
    #[clap(short, long)]
    repo_id: String,
    #[clap(long, default_value = "HFRepoType::Model")]
    repo_type: HFRepoType,
    #[clap(long, visible_alias = &["branch", "ref", "commit", "tag"])]
    reference: Option<String>,
    #[clap(short, long)]
    token: Option<String>,
    #[clap(short, long)]
    path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = MountArgs::parse();

    Ok(())
}
