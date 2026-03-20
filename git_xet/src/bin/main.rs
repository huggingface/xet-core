use clap::Parser;
use git_xet::app::XetAgentApp;
use xet_runtime::GenericError;

#[tokio::main]
async fn main() -> Result<(), GenericError> {
    let app = XetAgentApp::parse();

    app.run().await?;

    Ok(())
}
