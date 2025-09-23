use super::errors::{GitLFSProtocolError, Result};

// This defines the state of a transfer agent to make sure that request events are initiated
// in the correct order. Unlike a traditional state machines, we don't define a "terminated"
// state as the agent will quit on a termination event and thus not move forward from there.
// For more information on the state transfer, see `crate::lfs_agent_protocol.rs`.
#[derive(Debug)]
pub enum LFSAgentState {
    PendingInit,
    InitedForUpload,
    InitedForDownload,
    Uploading,
    Downloading,
}

impl LFSAgentState {
    pub fn transit_to(&mut self, to: Self) -> Result<()> {
        match self {
            Self::PendingInit => match to {
                Self::InitedForUpload | Self::InitedForDownload => (),
                _ => return Err(GitLFSProtocolError::bad_state("init event not yet received")),
            },
            Self::InitedForUpload => match to {
                Self::Uploading => (),
                Self::Downloading => return Err(GitLFSProtocolError::bad_state("agent initiated for upload")),
                _ => return Err(GitLFSProtocolError::bad_state("init event already received")),
            },
            Self::InitedForDownload => match to {
                Self::Downloading => (),
                Self::Uploading => return Err(GitLFSProtocolError::bad_state("agent initiated for download")),
                _ => return Err(GitLFSProtocolError::bad_state("init event already received")),
            },
            Self::Uploading => match to {
                Self::Uploading => (),
                Self::Downloading => return Err(GitLFSProtocolError::bad_state("agent initiated for upload")),
                _ => return Err(GitLFSProtocolError::bad_state("data transfer already in progress")),
            },
            Self::Downloading => match to {
                Self::Downloading => (),
                Self::Uploading => return Err(GitLFSProtocolError::bad_state("agent initiated for download")),
                _ => return Err(GitLFSProtocolError::bad_state("data transfer already in progress")),
            },
        };

        *self = to;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::LFSAgentState;
    use crate::lfs_agent_protocol::errors::GitLFSProtocolError;

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

        assert!(matches!(ret, Err(GitLFSProtocolError::State(_))));

        // double init event
        let mut state = LFSAgentState::PendingInit;
        state.transit_to(LFSAgentState::InitedForUpload)?;
        let ret = state.transit_to(LFSAgentState::InitedForUpload);

        assert!(matches!(ret, Err(GitLFSProtocolError::State(_))));

        // upload init event followed by download init event
        let mut state = LFSAgentState::InitedForUpload;
        let ret = state.transit_to(LFSAgentState::InitedForDownload);

        assert!(matches!(ret, Err(GitLFSProtocolError::State(_))));

        // upload init event followed by download transfer event
        let mut state = LFSAgentState::InitedForUpload;
        let ret = state.transit_to(LFSAgentState::Downloading);

        assert!(matches!(ret, Err(GitLFSProtocolError::State(_))));

        // download init event followed by upload transfer event
        let mut state = LFSAgentState::InitedForDownload;
        let ret = state.transit_to(LFSAgentState::Uploading);

        assert!(matches!(ret, Err(GitLFSProtocolError::State(_))));

        // upload transfer event followed by download transfer event
        let mut state = LFSAgentState::Uploading;
        let ret = state.transit_to(LFSAgentState::Downloading);

        assert!(matches!(ret, Err(GitLFSProtocolError::State(_))));

        // download transfer event followed by upload transfer event
        let mut state = LFSAgentState::Downloading;
        let ret = state.transit_to(LFSAgentState::Uploading);

        assert!(matches!(ret, Err(GitLFSProtocolError::State(_))));

        Ok(())
    }
}
