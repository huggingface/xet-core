#![allow(unused)]

use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::errors::*;
use crate::lfs_agent_protocol::agent_state::*;

mod agent_state;
pub mod errors;
mod progress_updater;
mod protocol_spec;

pub use errors::GitLFSProtocolError;
pub use progress_updater::ProgressUpdater;
pub use protocol_spec::*;

pub trait TransferAgent {
    async fn init_upload(&mut self, req: &InitRequestInner) -> Result<()>;
    async fn init_download(&mut self, req: &InitRequestInner) -> Result<()>;
    async fn upload_one<W: Write + Send + Sync + 'static>(
        &mut self,
        req: &TransferRequest,
        progress_updater: ProgressUpdater<W>,
    ) -> Result<()>;
    async fn download_one<W: Write + Send + Sync + 'static>(
        &mut self,
        req: &TransferRequest,
        progress_updater: ProgressUpdater<W>,
    ) -> Result<PathBuf>;
    async fn terminate(&mut self) -> Result<()>;
}

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

/// The LFS custom transfer agent protocol is serial. git-lfs waits for the completion of one request
/// by reading the response before sending the next request.
pub async fn lfs_protocol_loop<R, W, A>(input_channel: R, output_channel: W, agent: &mut A) -> Result<()>
where
    R: BufRead,
    W: Write + Send + Sync + 'static,
    A: TransferAgent,
{
    let mut stdin = input_channel;
    let stdout = Arc::new(Mutex::new(output_channel));
    let mut state = LFSAgentState::PendingInit;

    let pid = std::process::id();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(format!("log.{}.txt", pid))
        .unwrap();

    // Each request and response is serialized as a JSON structure, to be sent and received on a
    // single line as per Line Delimited JSON.
    loop {
        let event = recv_request(&mut stdin)?;

        file.write_all(
            format!("[{}:{}] [{:?}] recv: {event:?}\n", pid, chrono::Local::now().to_rfc3339(), state).as_bytes(),
        )
        .unwrap();

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
                if let Err(e) = agent.terminate().await {
                    Err(e)?
                }

                file.write_all(
                    format!("[{}:{}]\t + sleeping...\n", std::process::id(), chrono::Local::now().to_rfc3339())
                        .as_bytes(),
                )
                .unwrap();
                // std::thread::sleep(Duration::from_secs(60));
                tokio::time::sleep(Duration::from_secs(60)).await;
                file.write_all(
                    format!("[{}:{}]\t + wake up\n", std::process::id(), chrono::Local::now().to_rfc3339()).as_bytes(),
                )
                .unwrap();

                // successful termination, no response is expected
                break;
            },
        };

        file.write_all(
            format!("[{}:{}] [{:?}] resp: {response}\n", std::process::id(), chrono::Local::now().to_rfc3339(), state)
                .as_bytes(),
        )
        .unwrap();
        file.flush().unwrap();

        stdout.lock().map_err(internal)?.write_all(response.as_bytes())?;
    }

    Ok(())
}
