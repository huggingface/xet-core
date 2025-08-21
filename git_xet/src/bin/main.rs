use anyhow::Result;
use clap::Parser;
use git_xet::app::XetApp;

#[tokio::main]
async fn main() -> Result<()> {
    let app = XetApp::parse();

    app.run().await?;

    Ok(())
}
