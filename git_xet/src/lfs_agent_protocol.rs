use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::errors::{GitXetError, Result};

mod agent_state;
pub mod errors;
mod progress_updater;
mod protocol_spec;

use agent_state::LFSAgentState;
pub use errors::GitLFSProtocolError;
pub use progress_updater::ProgressUpdater;
pub use protocol_spec::*;

// Any Git LFS custom transfer agent should implement this trait to be plugged
// into the driver function `lfs_protocol_loop`.
pub trait TransferAgent {
    // Prepare itself for upload operations.
    async fn init_upload(&mut self, req: &InitRequestInner) -> Result<()>;

    // Prepare itself for download operations.
    async fn init_download(&mut self, req: &InitRequestInner) -> Result<()>;

    // Upload one file.
    async fn upload_one<W: Write + Send + Sync + 'static>(
        &mut self,
        req: &TransferRequest,
        progress_updater: ProgressUpdater<W>,
    ) -> Result<()>;

    // Download one file.
    async fn download_one<W: Write + Send + Sync + 'static>(
        &mut self,
        req: &TransferRequest,
        progress_updater: ProgressUpdater<W>,
    ) -> Result<PathBuf>;

    // Cleanup all resource and terminate.
    async fn terminate(&mut self) -> Result<()>;
}

/// The git-lfs client communicates with the custom transfer process via the stdin and stdout streams. No file content
/// is communicated on these streams, only request / response metadata. The metadata exchanged is always in Line
/// Delimited JSON format. External files will be referenced when actual content is exchanged.
///
/// The protocol consists of 3 stages:
/// Stage 1: Initiation
///     Immediately after invoking a custom transfer process, git-lfs sends initiation data to the process over stdin.
///     This tells the process useful information about the configuration.
/// Stage 2: 0..N Transfers
///     After the initiation exchange, git-lfs will send any number of transfer requests to the stdin of the transfer
///     process, in a serial sequence. Once a transfer request is sent to the process, it awaits a completion response
///     before sending the next request.
/// Stage 3: Finish & Cleanup
///     When all transfers have been processed, git-lfs will send a "terminate" event. On receiving this message the
///     transfer process should clean up and terminate. No response is expected.
///
/// See https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md for details.
pub async fn lfs_protocol_loop<R, W, A>(input_channel: R, output_channel: W, agent: &mut A) -> Result<()>
where
    R: BufRead,
    W: Write + Send + Sync + 'static,
    A: TransferAgent,
{
    let mut stdin = input_channel;
    let stdout = Arc::new(Mutex::new(output_channel));
    let mut state = LFSAgentState::PendingInit;

    // Each request and response is serialized as a JSON structure, to be sent and received on a
    // single line as per Line Delimited JSON.
    loop {
        let event = recv_request(&mut stdin)?;

        // Then we validate the state transition before processing the request.
        let response = match event {
            LFSProtocolRequestEvent::Init(req) => {
                let init_ret = match &req {
                    InitRequest::Upload(req) => {
                        state.transit_to(LFSAgentState::InitedForUpload)?;
                        agent.init_upload(req).await
                    },
                    InitRequest::Download(req) => {
                        state.transit_to(LFSAgentState::InitedForDownload)?;
                        agent.init_download(req).await
                    },
                };

                match init_ret {
                    Ok(_) => to_line_delimited_json_string(req.success())?,
                    Err(e) => to_line_delimited_json_string(req.error(e))?,
                }
            },
            LFSProtocolRequestEvent::Upload(req) => {
                state.transit_to(LFSAgentState::Uploading)?;

                match agent.upload_one(&req, ProgressUpdater::new(stdout.clone(), &req.oid)).await {
                    Ok(_) => to_line_delimited_json_string(req.success(None))?,
                    Err(e) => to_line_delimited_json_string(req.error(e))?,
                }
            },
            LFSProtocolRequestEvent::Download(req) => {
                state.transit_to(LFSAgentState::Downloading)?;

                match agent.download_one(&req, ProgressUpdater::new(stdout.clone(), &req.oid)).await {
                    Ok(path) => to_line_delimited_json_string(req.success(Some(path)))?,
                    Err(e) => to_line_delimited_json_string(req.error(e))?,
                }
            },
            LFSProtocolRequestEvent::Terminate => {
                agent.terminate().await?;

                // successful termination, no response is expected
                break;
            },
        };

        stdout.lock().map_err(GitXetError::internal)?.write_all(response.as_bytes())?;
    }

    Ok(())
}

// Parse a request from git-lfs
fn recv_request<R>(input_channel: &mut R) -> Result<LFSProtocolRequestEvent>
where
    R: BufRead,
{
    let mut event_json = String::new();
    input_channel.read_line(&mut event_json)?;

    // Upon receiving a request event, parse makes sure its syntax and logic is correct.
    let event: LFSProtocolRequestEvent = event_json.parse()?;

    Ok(event)
}
