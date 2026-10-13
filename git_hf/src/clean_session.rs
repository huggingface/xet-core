//! Shared clean lifetime: one upload session, staging directory, and dedup source for many files.
//!
//! Git receives pointers before the session is finalized at process shutdown. Git aborts a command
//! on any required-filter error, so request failures never publish earlier pointers. Shutdown is
//! different: Git has already written the index and ignores the filter's exit status. A failed
//! finalization therefore writes a marker listing the affected paths, for pre-push to report.
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use git_extension_common::{GitRepo, get_noninteractive_credential};
use tempfile::TempDir;
use xet_client::cas_client::auth::{AuthConfig, AuthError, TokenInfo, TokenRefresher};
use xet_client::cas_client::{Client, RemoteClient};
use xet_client::hub_client::{HubClient, NoopCredentialHelper, Operation};
use xet_data::processing::configurations::{SessionContext, TranslatorConfig};
use xet_data::processing::{FileUploadSession, UploadShardCachePolicy};
use xet_runtime::core::XetContext;
use xet_runtime::file_utils::SafeFileCreator;

use crate::staging::{StagedDedupSource, StagingClient};
use crate::{GitHfError, Result};

pub(crate) struct CleanSession {
    pub(crate) ctx: XetContext,
    state: Mutex<Option<Arc<CleanSessionState>>>,
}

/// Directory under staging holding one marker per failed session. A marker is a sequence of
/// NUL-terminated records: the originating worktree root (staging is shared by linked worktrees,
/// while Git's pathnames are relative to the worktree that ran the filter), then each pathname.
pub(crate) const FINALIZE_FAILED_DIR: &str = "finalize-failed";

pub(crate) struct CleanSessionState {
    pub(crate) session: Arc<FileUploadSession>,
    pub(crate) dedup: Arc<StagedDedupSource>,
    // Pathnames whose returned pointers depend on this session's finalization.
    paths: Mutex<Vec<Vec<u8>>>,
    // Keep temporary metadata alive until all file cleaners and finalization finish.
    scratch: TempDir,
    staging: PathBuf,
    worktree: PathBuf,
}

impl CleanSessionState {
    pub(crate) fn record_path(&self, pathname: &[u8]) -> Result<()> {
        self.paths
            .lock()
            .map_err(|_| GitHfError::Internal("Clean session path lock poisoned".into()))?
            .push(pathname.to_vec());
        Ok(())
    }

    /// Writes `staging/finalize-failed/<session>` atomically and returns its path.
    fn write_failure_marker(&self, paths: &[Vec<u8>]) -> Result<PathBuf> {
        let name = self.scratch.path().file_name().unwrap();
        let marker = self.staging.join(FINALIZE_FAILED_DIR).join(name);
        std::fs::create_dir_all(marker.parent().unwrap())?;
        let mut file = SafeFileCreator::new(&marker)?;
        file.write_all(self.worktree.as_os_str().as_encoded_bytes())?;
        file.write_all(b"\0")?;
        for path in paths {
            file.write_all(path)?;
            file.write_all(b"\0")?;
        }
        file.close()?;
        Ok(marker)
    }
}

struct HubTokens(Arc<HubClient>);
#[async_trait]
impl TokenRefresher for HubTokens {
    async fn refresh(&self) -> std::result::Result<TokenInfo, AuthError> {
        let info = self
            .0
            .get_cas_jwt(Operation::Download)
            .await
            .map_err(AuthError::token_refresh_failure)?;
        Ok((info.access_token, info.exp))
    }
}

impl CleanSession {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            ctx: XetContext::default()?,
            state: Mutex::new(None),
        })
    }

    pub(crate) fn state(&self) -> Result<Arc<CleanSessionState>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| GitHfError::Internal("Clean session lock poisoned".into()))?;
        if let Some(state) = state.as_ref() {
            return Ok(state.clone());
        }
        let created = self.create_state(&GitRepo::open_from_cur_dir()?)?;
        *state = Some(created.clone());
        Ok(created)
    }

    async fn remote(ctx: &XetContext, repo: &GitRepo, id: &str) -> Result<(Arc<dyn Client>, TranslatorConfig)> {
        let url = repo.remote_url()?;
        let hub = Arc::new(HubClient::new(
            ctx.clone(),
            &url.to_derived_http_host_url()?,
            url.repo_info()?,
            repo.branch_name()?,
            id,
            Some(get_noninteractive_credential(repo, &url)?.unwrap_or_else(|| NoopCredentialHelper::new())),
            None,
        )?);
        let info = hub.get_cas_jwt(Operation::Download).await?;
        let auth = Some(AuthConfig {
            token: info.access_token,
            token_expiration: info.exp,
            token_refresher: Arc::new(HubTokens(hub)),
        });
        let client = RemoteClient::new(ctx.clone(), &info.cas_url, &auth, id, false, None);
        let config = TranslatorConfig::new(
            ctx,
            SessionContext {
                endpoint: info.cas_url,
                auth,
                custom_headers: None,
                repo_paths: vec![],
                session_id: Some(id.to_owned()),
                cache_root: None,
            },
        )?;
        Ok((client, config))
    }

    fn create_state(&self, repo: &GitRepo) -> Result<Arc<CleanSessionState>> {
        let worktree = repo
            .worktree_dir()?
            .ok_or_else(|| GitHfError::NotSupported("Cleaning requires a worktree".into()))?;
        let base = repo.common_dir()?.join("xet").join("staging");
        std::fs::create_dir_all(&base)?;
        let scratch = tempfile::Builder::new().prefix("session-").tempdir_in(&base)?;
        let id = scratch.path().file_name().unwrap().to_string_lossy();
        let remote = self.ctx.runtime.handle().block_on(async {
            tokio::time::timeout(Duration::from_secs(2), Self::remote(&self.ctx, repo, &id))
                .await
                .ok()
                .and_then(|r| r.ok())
        });
        let (remote, mut config) = match remote {
            Some((client, config)) => (Some(client), config),
            // Without a resolved CAS endpoint, no global cache namespace can safely be selected.
            None => (None, TranslatorConfig::memory_config(&self.ctx, scratch.path())?),
        };
        config.shard_session_directory = scratch.path().join("session");
        config.force_disable_progress_aggregation = true;
        let client = StagingClient::new(self.ctx.clone(), &base, remote)?;
        let dedup = self
            .ctx
            .runtime
            .handle()
            .block_on(StagedDedupSource::new(&self.ctx, &base.join("shards")))?;
        let session = self.ctx.runtime.handle().block_on(FileUploadSession::new_with_client(
            Arc::new(config),
            client,
            Some(dedup.clone()),
            UploadShardCachePolicy::Skip,
        ))?;
        Ok(Arc::new(CleanSessionState {
            session,
            dedup,
            paths: Mutex::new(Vec::new()),
            scratch,
            staging: base,
            worktree,
        }))
    }

    /// Finalizes staged work. On failure, records the affected paths before returning the error,
    /// because Git will not report it: the index already references the returned pointers.
    pub(crate) fn finish(self) -> Result<()> {
        let Some(state) = self
            .state
            .into_inner()
            .map_err(|_| GitHfError::Internal("Clean session lock poisoned".into()))?
        else {
            return Ok(());
        };
        let Err(error) = self.ctx.runtime.handle().block_on(state.session.clone().finalize()) else {
            return Ok(());
        };
        let paths = state
            .paths
            .lock()
            .map_err(|_| GitHfError::Internal("Clean session path lock poisoned".into()))?;
        report_failure(state.write_failure_marker(&paths), &state.worktree, &paths);
        Err(error.into())
    }

    #[cfg(test)]
    fn clean(&self, input: &mut impl std::io::Read) -> Result<Vec<u8>> {
        crate::cleaner::SelectingFileCleaner::new(self, b"file").clean(input)
    }
}

fn report_failure(marker: Result<PathBuf>, worktree: &std::path::Path, paths: &[Vec<u8>]) {
    eprintln!(
        "git-hf: staging failed; these files in {} are staged as pointers without local data:",
        worktree.display()
    );
    for path in paths {
        eprintln!("  {}", String::from_utf8_lossy(path));
    }
    eprintln!("git-hf: after fixing the cause, run `git add --renormalize -- <paths>` in that worktree.");
    match marker {
        Ok(marker) => eprintln!("git-hf: recorded in {}", marker.display()),
        Err(error) => eprintln!("git-hf: failed to record the affected paths: {error}"),
    }
}

/// Reads one marker as its worktree root and pathnames.
#[cfg(test)]
pub(crate) fn read_failure_marker(marker: &std::path::Path) -> Result<(PathBuf, Vec<Vec<u8>>)> {
    let data = std::fs::read(marker)?;
    let mut records = data.strip_suffix(b"\0").unwrap_or(&data).split(|&b| b == 0);
    let worktree = String::from_utf8_lossy(records.next().unwrap_or_default()).into_owned();
    Ok((worktree.into(), records.map(<[u8]>::to_vec).collect()))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Cursor;

    use git_extension_common::test_utils::TestRepo;
    use serial_test::serial;
    use xet_core_structures::merklehash::MerkleHash;
    use xet_core_structures::metadata_shard::streaming_shard::MDBMinimalShard;
    use xet_runtime::utils::CwdGuard;

    use super::*;
    use crate::XetPointer;
    use crate::selection::{BINARY_LIMIT, TEXT_LIMIT};

    #[test]
    #[serial(env_var_write_read)]
    fn concurrent_file_cleaners_share_aggregation() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(repo.path())?;
        let session = CleanSession::new()?;
        let pointers = std::thread::scope(|scope| {
            let first = scope.spawn(|| session.clean(&mut vec![0; BINARY_LIMIT + 1].as_slice()));
            let second = scope.spawn(|| session.clean(&mut vec![1; BINARY_LIMIT + 1].as_slice()));
            Ok::<_, GitHfError>([first.join().unwrap()?, second.join().unwrap()?])
        })?;
        assert_ne!(pointers[0], pointers[1]);
        let root = session.state()?.staging.clone();
        session.finish()?;
        assert_eq!(fs::read_dir(root.join("xorbs"))?.count(), 1);
        let files = fs::read_dir(root.join("shards"))?
            .map(|entry| {
                let shard = MDBMinimalShard::from_reader(&mut Cursor::new(fs::read(entry?.path())?), true, true)?;
                Ok::<_, anyhow::Error>(shard.num_files())
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        assert_eq!(files.iter().sum::<usize>(), 2);
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn completed_sessions_are_shared_for_dedup() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(repo.path())?;
        let mut a = vec![b'a'; BINARY_LIMIT + 1];
        a[0] = 0;
        let mut b = vec![b'b'; BINARY_LIMIT + 1];
        b[0] = 0;
        let first = CleanSession::new()?;
        let a_pointer = first.clean(&mut a.as_slice())?;
        let a_hash = XetPointer::parse(&a_pointer)?.unwrap().hash;
        let second = CleanSession::new()?;
        second.clean(&mut b.as_slice())?;
        assert_ne!(first.state()?.scratch.path(), second.state()?.scratch.path());
        assert_eq!(first.state()?.staging, second.state()?.staging);
        let root = first.state()?.staging.clone();
        first.finish()?;
        let first_xorb = fs::read_dir(root.join("xorbs"))?.next().unwrap()?.file_name();
        assert_eq!(second.clean(&mut a.as_slice())?, a_pointer);
        second.finish()?;
        assert_eq!(fs::read_dir(root.join("xorbs"))?.count(), 2);
        // Both the original and later file records must reference A's existing xorb.
        let mut matches = 0;
        for entry in fs::read_dir(root.join("shards"))? {
            let shard = MDBMinimalShard::from_reader(&mut Cursor::new(fs::read(entry?.path())?), true, true)?;
            for i in 0..shard.num_files() {
                let file = shard.file(i).unwrap();
                if file.file_hash() == a_hash {
                    matches += 1;
                    assert_eq!(file.num_entries(), 1);
                    assert_eq!(std::ffi::OsString::from(format!("{}.xorb", file.entry(0).xorb_hash.hex())), first_xorb);
                }
            }
        }
        assert_eq!(matches, 2);
        let third = CleanSession::new()?;
        // Initialize the new session, then forbid any xorb writes: a repeated file needs none.
        third.state()?;
        fs::rename(root.join("xorbs"), root.join("saved-xorbs"))?;
        fs::write(root.join("xorbs"), b"not a directory")?;
        assert_eq!(third.clean(&mut a.as_slice())?, a_pointer);
        third.finish()?;
        fs::remove_file(root.join("xorbs"))?;
        fs::rename(root.join("saved-xorbs"), root.join("xorbs"))?;
        assert_eq!(fs::read_dir(root)?.count(), 2);
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn finalize_failure_records_unbacked_paths() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(repo.path())?;
        let cleaner = CleanSession::new()?;
        let data = vec![b'a'; TEXT_LIMIT + 1];
        let result = cleaner.clean(&mut data.as_slice())?;
        assert_eq!(XetPointer::parse(&result)?.unwrap().size, data.len() as u64);
        let staging = GitRepo::open(repo.path())?.common_dir()?.join("xet/staging");
        let root = staging;
        // The remaining partial xorb is written at finalization. Make that destination unwritable.
        fs::rename(root.join("xorbs"), root.join("saved-xorbs"))?;
        fs::write(root.join("xorbs"), b"not a directory")?;
        assert!(cleaner.finish().is_err());
        assert_eq!(fs::read_dir(root.join("shards"))?.count(), 0);
        // The pointer above is unbacked; the marker names its path for later recovery.
        let markers = fs::read_dir(root.join(FINALIZE_FAILED_DIR))?.collect::<std::io::Result<Vec<_>>>()?;
        assert_eq!(markers.len(), 1);
        let (worktree, paths) = read_failure_marker(&markers[0].path())?;
        assert_eq!(worktree.canonicalize()?, repo.path().canonicalize()?);
        assert_eq!(paths, [b"file".to_vec()]);
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn selection_and_persistent_shared_xorbs() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(repo.path())?;
        let cleaner = CleanSession::new()?;
        for data in [Vec::new(), vec![b'a'; TEXT_LIMIT], vec![0; BINARY_LIMIT]] {
            assert_eq!(cleaner.clean(&mut data.as_slice())?, data);
        }
        assert!(cleaner.state.lock().unwrap().is_none());
        let pointer = XetPointer::new(MerkleHash::default(), 42)
            .to_string()
            .replace("size 42\n", "size 42\nextension hello\n");
        assert_eq!(cleaner.clean(&mut pointer.as_bytes())?, pointer.as_bytes());
        assert!(cleaner.clean(&mut &b"version https://hf.co/spec/xet/v1\n"[..]).is_err());
        let mut hashes = Vec::new();
        for byte in [b'a', b'b'] {
            let mut data = vec![byte; BINARY_LIMIT + 1];
            data[0] = 0;
            let out = cleaner.clean(&mut data.as_slice())?;
            hashes.push(XetPointer::parse(&out)?.unwrap().hash);
        }
        let stage = GitRepo::open(repo.path())?.common_dir()?.join("xet/staging");
        let root = stage.clone();
        assert_eq!(fs::read_dir(root.join("shards"))?.count(), 0);
        cleaner.finish()?;
        assert_eq!(fs::read_dir(root.join("xorbs"))?.count(), 1);
        let mut found = Vec::new();
        for entry in fs::read_dir(root.join("shards"))? {
            let shard = MDBMinimalShard::from_reader(&mut Cursor::new(fs::read(entry?.path())?), true, true)?;
            for i in 0..shard.num_files() {
                found.push(shard.file(i).unwrap().file_hash());
            }
        }
        hashes.sort();
        found.sort();
        assert_eq!(found, hashes);
        let second = CleanSession::new()?;
        let mut data = vec![0; BINARY_LIMIT + 1];
        data[1] = 17;
        second.clean(&mut data.as_slice())?;
        second.finish()?;
        assert_eq!(fs::read_dir(stage)?.count(), 2);
        assert_eq!(fs::read_dir(root.join("xorbs"))?.count(), 2);
        Ok(())
    }
}
