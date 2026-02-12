//! XetSession - manages runtime and configuration

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use xet_runtime::XetRuntime;
use xet_config::XetConfig;
use ulid::Ulid;
use utils::auth::TokenRefresher;

use crate::errors::SessionError;
use crate::upload_commit::UploadCommit;
use crate::download_group::DownloadGroup;

/// Session state
struct SessionState {
    accepting_new_commits: bool,
    session_id: String,
}

/// Main session for managing file uploads and downloads
pub struct XetSession {
    runtime: Arc<XetRuntime>,
    config: Arc<XetConfig>,

    // Endpoint and auth (shared by all commits/groups)
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,

    // Track active commits and groups
    active_commits: Arc<Mutex<HashSet<Ulid>>>,
    active_groups: Arc<Mutex<HashSet<Ulid>>>,

    // Session state
    state: Arc<Mutex<SessionState>>,
}

impl XetSession {
    /// Create a new session with default configuration
    pub fn new(config: XetConfig) -> Result<Arc<Self>, SessionError> {
        Self::new_with_auth(config, None, None, None)
    }

    /// Create a new session with authentication
    pub fn new_with_auth(
        config: XetConfig,
        endpoint: Option<String>,
        token_info: Option<(String, u64)>,
        token_refresher: Option<Arc<dyn TokenRefresher>>,
    ) -> Result<Arc<Self>, SessionError> {
        // Create a NEW runtime for this session (not from global cache)
        let runtime = XetRuntime::new_with_config(config.clone())?;

        let session_id = Ulid::new().to_string();

        Ok(Arc::new(Self {
            runtime,
            config: Arc::new(config),
            endpoint,
            token_info,
            token_refresher,
            active_commits: Arc::new(Mutex::new(HashSet::new())),
            active_groups: Arc::new(Mutex::new(HashSet::new())),
            state: Arc::new(Mutex::new(SessionState {
                accepting_new_commits: true,
                session_id,
            })),
        }))
    }

    /// Create a new upload commit
    pub fn new_upload_commit(self: &Arc<Self>) -> Result<UploadCommit, SessionError> {
        let state = self.state.lock()?;
        if !state.accepting_new_commits {
            return Err(SessionError::NotAcceptingTasks);
        }
        drop(state);

        let commit = UploadCommit::new(self.clone())?;

        // Register the commit
        self.active_commits.lock()?.insert(commit.commit_id());

        Ok(commit)
    }

    /// Create a new download group
    pub fn new_download_group(self: &Arc<Self>) -> Result<DownloadGroup, SessionError> {
        let state = self.state.lock()?;
        if !state.accepting_new_commits {
            return Err(SessionError::NotAcceptingTasks);
        }
        drop(state);

        let group = DownloadGroup::new(self.clone())?;

        // Register the group
        self.active_groups.lock()?.insert(group.group_id());

        Ok(group)
    }

    /// End the session - wait for all active commits/groups to finish
    pub async fn end(self: Arc<Self>) -> Result<(), SessionError> {
        // Mark as not accepting new commits
        {
            let mut state = self.state.lock()?;
            state.accepting_new_commits = false;
        }

        // Wait for all active commits and groups to be unregistered
        // (they unregister themselves when commit() or finish() is called)

        // For now, we just verify they're all done
        let commits = self.active_commits.lock()?.len();
        let groups = self.active_groups.lock()?.len();

        if commits > 0 || groups > 0 {
            tracing::warn!(
                "Session ending with {} uncommitted uploads and {} unfinished downloads",
                commits,
                groups
            );
        }

        Ok(())
    }

    // Accessors for child objects
    pub fn runtime(&self) -> &Arc<XetRuntime> {
        &self.runtime
    }

    pub(crate) fn config(&self) -> &Arc<XetConfig> {
        &self.config
    }

    pub(crate) fn endpoint(&self) -> &Option<String> {
        &self.endpoint
    }

    pub(crate) fn token_info(&self) -> &Option<(String, u64)> {
        &self.token_info
    }

    pub(crate) fn token_refresher(&self) -> &Option<Arc<dyn TokenRefresher>> {
        &self.token_refresher
    }

    pub(crate) fn unregister_commit(&self, commit_id: Ulid) {
        if let Ok(mut commits) = self.active_commits.lock() {
            commits.remove(&commit_id);
        } else {
            tracing::error!("Failed to acquire lock to unregister commit {}", commit_id);
        }
    }

    pub(crate) fn unregister_group(&self, group_id: Ulid) {
        if let Ok(mut groups) = self.active_groups.lock() {
            groups.remove(&group_id);
        } else {
            tracing::error!("Failed to acquire lock to unregister group {}", group_id);
        }
    }
}
