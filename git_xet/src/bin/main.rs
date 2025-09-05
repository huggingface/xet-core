use anyhow::Result;
use clap::Parser;
use git_xet::app::XetAgentApp;

#[tokio::main]
async fn main() -> Result<()> {
    let app = XetAgentApp::parse();

    app.run().await?;

    Ok(())
}
