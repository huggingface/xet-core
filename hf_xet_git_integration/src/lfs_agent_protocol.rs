use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::time::Duration;

use crate::errors::{Result, bad_protocol};
use crate::lfs_agent_protocol::definitions::*;

pub mod definitions;

#[allow(dead_code)]
pub struct ProgressUpdater<'a, W: Write> {
    update_channel: &'a W,

    bytes_so_far: u64,
    bytes_since_last: u64,
    bytes_total: u64,
}

#[allow(dead_code)]
impl<'a, W: Write> ProgressUpdater<'a, W> {
    fn new(update_channel: &'a W, bytes_total: u64) -> Self {
        Self {
            update_channel,
            bytes_so_far: 0,
            bytes_since_last: 0,
            bytes_total,
        }
    }

    fn bytes_so_far(&mut self, number: u64) {
        self.bytes_so_far = number;
    }
}

// This defines the state of a transfer agent to make sure that request events are initiated
// in the correct order. Unlike a traditional state machines, we don't define a "terminated"
// state as the agent will quit on a termination event and thus not move forward from there.
#[derive(Debug)]
enum LFSAgentState {
    PendingInit,
    InitedForUpload,
    InitedForDownload,
    Uploading,
    Downloading,
}

impl LFSAgentState {
    fn transit_to(&mut self, to: Self) -> Result<()> {
        let new_state = match self {
            Self::PendingInit => match to {
                Self::InitedForUpload | Self::InitedForDownload => Ok(to),
                _ => Err(bad_protocol("init event not yet received")),
            },
            Self::InitedForUpload => match to {
                Self::Uploading => Ok(to),
                Self::Downloading => Err(bad_protocol("agent initialized for upload")),
                _ => Err(bad_protocol("init event already received")),
            },
            Self::InitedForDownload => match to {
                Self::Downloading => Ok(to),
                Self::Uploading => Err(bad_protocol("agent initialized for download")),
                _ => Err(bad_protocol("init event already received")),
            },
            Self::Uploading => match to {
                Self::Uploading => Ok(to),
                Self::Downloading => Err(bad_protocol("agent initialized for upload")),
                _ => Err(bad_protocol("data transfer already in progress")),
            },
            Self::Downloading => match to {
                Self::Downloading => Ok(to),
                Self::Uploading => Err(bad_protocol("agent initialized for download")),
                _ => Err(bad_protocol("data transfer already in progress")),
            },
        };

        *self = new_state?;

        Ok(())
    }
}

pub trait TransferAgent {
    async fn init_upload(&mut self, req: &InitRequestInner) -> Result<()>;
    async fn init_download(&mut self, req: &InitRequestInner) -> Result<()>;
    async fn upload_one<W: Write>(&mut self, req: &TransferRequest, progress_updater: ProgressUpdater<W>)
    -> Result<()>;
    async fn download_one<W: Write>(
        &mut self,
        req: &TransferRequest,
        progress_updater: ProgressUpdater<W>,
    ) -> Result<PathBuf>;
    async fn terminate(&mut self) -> Result<()>;
}

/// The LFS custom transfer agent protocol is serial. git-lfs waits for the completion of one request
/// by reading the response before sending the next request.
pub async fn lfs_protocol_loop<R, W, A>(input_channel: R, output_channel: W, agent: &mut A) -> Result<()>
where
    R: BufRead,
    W: Write,
    A: TransferAgent,
{
    let mut stdin = input_channel;
    let mut stdout = output_channel;
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
        let mut event_json = String::new();
        stdin.read_line(&mut event_json)?;

        // Upon receiving a request event, parse makes sure its syntax and logic is correct.
        let event: LFSProtocolRequestEvent = event_json.parse()?;

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
                    Ok(_) => to_json_string(req.success())?,
                    Err(e) => to_json_string(req.error(e))?,
                }
            },
            LFSProtocolRequestEvent::Upload(req) => {
                state.transit_to(LFSAgentState::Uploading)?;

                match agent.upload_one(&req, ProgressUpdater::new(&stdout, req.size)).await {
                    Ok(_) => to_json_string(req.success(None))?,
                    Err(e) => to_json_string(req.error(e))?,
                }
            },
            LFSProtocolRequestEvent::Download(req) => {
                state.transit_to(LFSAgentState::Downloading)?;

                match agent.download_one(&req, ProgressUpdater::new(&stdout, req.size)).await {
                    Ok(path) => to_json_string(req.success(Some(path)))?,
                    Err(e) => to_json_string(req.error(e))?,
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

        stdout.write_all(response.as_bytes())?;
        // add the line delimiter
        stdout.write_all(b"\n")?;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use anyhow::Result;

    use super::*;

    #[test]
    fn test_state_transit_good() -> Result<()> {
        // two upload events after init
        let mut state = LFSAgentState::PendingInit;
        state.transit_to(LFSAgentState::InitedForUpload)?;
        state.transit_to(LFSAgentState::Uploading)?;
        state.transit_to(LFSAgentState::Uploading)?;

        // two download events after init
        let mut state = LFSAgentState::PendingInit;
        state.transit_to(LFSAgentState::InitedForDownload)?;
        state.transit_to(LFSAgentState::Downloading)?;
        state.transit_to(LFSAgentState::Downloading)?;

        Ok(())
    }

    #[test]
    fn test_state_transit_bad() -> Result<()> {
        // upload event before init
        let mut state = LFSAgentState::PendingInit;
        let ret = state.transit_to(LFSAgentState::Uploading);

        assert!(matches!(ret, Err(crate::errors::GitXetError::GitLFSProtocolError(_))));

        // double init event
        let mut state = LFSAgentState::PendingInit;
        state.transit_to(LFSAgentState::InitedForUpload)?;
        let ret = state.transit_to(LFSAgentState::InitedForUpload);

        assert!(matches!(ret, Err(crate::errors::GitXetError::GitLFSProtocolError(_))));

        // upload init event followed by download init event
        let mut state = LFSAgentState::InitedForUpload;
        let ret = state.transit_to(LFSAgentState::InitedForDownload);

        assert!(matches!(ret, Err(crate::errors::GitXetError::GitLFSProtocolError(_))));

        // upload init event followed by download transfer event
        let mut state = LFSAgentState::InitedForUpload;
        let ret = state.transit_to(LFSAgentState::Downloading);

        assert!(matches!(ret, Err(crate::errors::GitXetError::GitLFSProtocolError(_))));

        // download init event followed by upload transfer event
        let mut state = LFSAgentState::InitedForDownload;
        let ret = state.transit_to(LFSAgentState::Uploading);

        assert!(matches!(ret, Err(crate::errors::GitXetError::GitLFSProtocolError(_))));

        // upload transfer event followed by download transfer event
        let mut state = LFSAgentState::Uploading;
        let ret = state.transit_to(LFSAgentState::Downloading);

        assert!(matches!(ret, Err(crate::errors::GitXetError::GitLFSProtocolError(_))));

        // download transfer event followed by upload transfer event
        let mut state = LFSAgentState::Downloading;
        let ret = state.transit_to(LFSAgentState::Uploading);

        assert!(matches!(ret, Err(crate::errors::GitXetError::GitLFSProtocolError(_))));

        Ok(())
    }
}
