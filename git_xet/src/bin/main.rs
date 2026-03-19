use clap::Parser;
use git_xet::app::XetAgentApp;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = XetAgentApp::parse();

    app.run().await?;

    Ok(())
}
