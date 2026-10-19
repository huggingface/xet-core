//! Shared clean lifetime: one upload session, staging directory, and dedup source for many files.
//!
//! Git receives pointers before the session is finalized at process shutdown. Git aborts a command
//! on any required-filter error, so request failures never publish earlier pointers. Shutdown is
//! different: Git has already written the index and ignores the filter's exit status. A failed
//! finalization therefore writes a marker listing the affected file hashes, for pre-push to check.
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use git_extension_common::{GitRepo, GitUrl, Interaction};
use tempfile::TempDir;
use tokio::sync::OnceCell;
use xet_client::cas_client::auth::{AuthConfig, AuthError, TokenInfo, TokenRefresher};
use xet_client::cas_client::{Client, RemoteClient};
use xet_client::hub_client::{CredentialHelper, HubClient, Operation};
use xet_core_structures::merklehash::MerkleHash;
use xet_data::processing::configurations::{SessionContext, TranslatorConfig};
use xet_data::processing::{FileUploadSession, UploadShardCachePolicy};
use xet_runtime::core::XetContext;
use xet_runtime::file_utils::SafeFileCreator;

use crate::auth::resolve_credential;
use crate::hook::{InstallOutcome, ensure_installed};
use crate::staging::{StagedDedupSource, StagingClient};
use crate::{GitHfError, Result};

pub(crate) struct CleanSession {
    pub(crate) ctx: XetContext,
    state: OnceCell<Arc<CleanSessionState>>,
}

/// Directory under staging holding one marker per failed session. A marker lists the hex file hash
/// of each pointer the session returned, one per line. It records no paths: a pointer may be
/// committed, and its path changed or removed, before a push finds it unbacked, so push locates it
/// in the pushed commits instead.
pub(crate) const FINALIZE_FAILED_DIR: &str = "finalize-failed";

pub(crate) struct CleanSessionState {
    pub(crate) session: Arc<FileUploadSession>,
    pub(crate) dedup: Arc<StagedDedupSource>,
    // Returned pointers (file hash and pathname) that depend on this session's finalization.
    files: Mutex<Vec<(MerkleHash, Vec<u8>)>>,
    // Keep temporary metadata alive until all file cleaners and finalization finish.
    scratch: TempDir,
    staging: PathBuf,
    worktree: PathBuf,
}

impl CleanSessionState {
    pub(crate) fn record_file(&self, hash: MerkleHash, pathname: &[u8]) -> Result<()> {
        self.files
            .lock()
            .map_err(|_| GitHfError::Internal("Clean session path lock poisoned".into()))?
            .push((hash, pathname.to_vec()));
        Ok(())
    }

    /// Writes `staging/finalize-failed/<session>` atomically and returns its path.
    fn write_failure_marker(&self, files: &[(MerkleHash, Vec<u8>)]) -> Result<PathBuf> {
        let name = self.scratch.path().file_name().unwrap();
        let marker = self.staging.join(FINALIZE_FAILED_DIR).join(name);
        std::fs::create_dir_all(marker.parent().unwrap())?;
        let mut file = SafeFileCreator::new(&marker)?;
        for (hash, _) in files {
            writeln!(file, "{}", hash.hex())?;
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
            state: OnceCell::new(),
        })
    }

    /// The session state, created on first use for the repository of the current directory.
    pub(crate) async fn state(&self) -> Result<Arc<CleanSessionState>> {
        self.state.get_or_try_init(|| self.create_state()).await.cloned()
    }

    async fn remote(
        ctx: &XetContext,
        url: &GitUrl,
        branch: Option<String>,
        credential: Arc<dyn CredentialHelper>,
        id: &str,
    ) -> Result<(Arc<dyn Client>, TranslatorConfig)> {
        let hub = Arc::new(HubClient::new(
            ctx.clone(),
            &url.to_derived_http_host_url()?,
            url.repo_info()?,
            branch,
            id,
            Some(credential),
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

    async fn create_state(&self) -> Result<Arc<CleanSessionState>> {
        // The repository handle isn't Sync, so it is dropped before the first await.
        let (worktree, base, scratch, remote) = {
            let repo = GitRepo::open_from_cur_dir()?;
            let worktree = repo
                .worktree_dir()?
                .ok_or_else(|| GitHfError::NotSupported("Cleaning requires a worktree".into()))?;
            // Staged data reaches the Hub only through the pre-push hook. A failure here must not block
            // `git add`: without the hook, the server rejects pushes of unbacked pointers.
            match ensure_installed(&worktree) {
                Ok(InstallOutcome::Chained(path)) => {
                    eprintln!("git-hf: installed the pre-push hook; the existing hook now runs from {}", path.display())
                },
                Ok(_) => {},
                Err(error) => {
                    eprintln!("git-hf: could not install the pre-push hook; staged files will not upload: {error}")
                },
            }
            let base = repo.common_dir()?.join("xet").join("staging");
            std::fs::create_dir_all(&base)?;
            let scratch = tempfile::Builder::new().prefix("session-").tempdir_in(&base)?;
            // Resolved outside the time budget below, which cannot interrupt blocking calls. Lookups
            // never prompt and bound themselves: 2 s for a credential helper, a connect timeout for SSH.
            let remote = repo.remote_url().ok().and_then(|url| {
                let credential = resolve_credential(&repo, &url, Operation::Download, Interaction::Never).ok()?;
                Some((url, repo.branch_name().ok()?, credential.helper()))
            });
            (worktree, base, scratch, remote)
        };
        let id = scratch.path().file_name().unwrap().to_string_lossy().into_owned();
        let remote = match remote {
            Some((url, branch, credential)) => {
                tokio::time::timeout(Duration::from_secs(2), Self::remote(&self.ctx, &url, branch, credential, &id))
                    .await
                    .ok()
                    .and_then(|r| r.ok())
            },
            None => None,
        };
        let (remote, mut config) = match remote {
            Some((client, config)) => (Some(client), config),
            // Without a resolved CAS endpoint, no global cache namespace can safely be selected.
            None => (None, TranslatorConfig::memory_config(&self.ctx, scratch.path())?),
        };
        config.shard_session_directory = scratch.path().join("session");
        config.force_disable_progress_aggregation = true;
        let client = StagingClient::new(self.ctx.clone(), &base, remote)?;
        let dedup = StagedDedupSource::new(&self.ctx, &base.join("shards")).await?;
        let session = FileUploadSession::new_with_client(
            Arc::new(config),
            client,
            Some(dedup.clone()),
            UploadShardCachePolicy::Skip,
        )
        .await?;
        Ok(Arc::new(CleanSessionState {
            session,
            dedup,
            files: Mutex::new(Vec::new()),
            scratch,
            staging: base,
            worktree,
        }))
    }

    /// Finalizes staged work. On failure, records the affected paths before returning the error,
    /// because Git will not report it: the index already references the returned pointers.
    pub(crate) async fn finish(self) -> Result<()> {
        let Some(state) = self.state.into_inner() else {
            return Ok(());
        };
        let Err(error) = state.session.clone().finalize().await else {
            return Ok(());
        };
        let files = state
            .files
            .lock()
            .map_err(|_| GitHfError::Internal("Clean session path lock poisoned".into()))?;
        report_failure(state.write_failure_marker(&files), &state.worktree, &files);
        Err(error.into())
    }

    /// Cleans `data` as one request for `file`, blocking until done.
    #[cfg(test)]
    fn clean(&self, data: &[u8]) -> Result<Vec<u8>> {
        use crate::filter_protocol::{Content, flush_packet, write_data};

        self.ctx.runtime.handle().block_on(async {
            let mut wire = Vec::new();
            write_data(&mut wire, data).await?;
            flush_packet(&mut wire).await?;
            crate::cleaner::SelectingFileCleaner::new(self, b"file")
                .clean(&mut Content::new(&mut wire.as_slice()))
                .await
        })
    }

    #[cfg(test)]
    fn state_blocking(&self) -> Result<Arc<CleanSessionState>> {
        self.ctx.runtime.handle().block_on(self.state())
    }

    /// Finalizes, blocking until done.
    #[cfg(test)]
    fn finish_blocking(self) -> Result<()> {
        let runtime = self.ctx.runtime.clone();
        runtime.bridge_sync(self.finish())?
    }
}

fn report_failure(marker: Result<PathBuf>, worktree: &std::path::Path, files: &[(MerkleHash, Vec<u8>)]) {
    eprintln!(
        "git-hf: staging failed; these files in {} are staged as pointers without local data:",
        worktree.display()
    );
    for (_, path) in files {
        eprintln!("  {}", String::from_utf8_lossy(path));
    }
    eprintln!("git-hf: after fixing the cause, run `git add --renormalize -- <paths>` in that worktree.");
    match marker {
        Ok(marker) => eprintln!("git-hf: recorded in {}", marker.display()),
        Err(error) => eprintln!("git-hf: failed to record the affected paths: {error}"),
    }
}

/// Reads the file hashes one marker lists.
#[cfg(test)]
pub(crate) fn read_failure_marker(marker: &std::path::Path) -> Result<Vec<MerkleHash>> {
    let invalid = || GitHfError::Internal(format!("{} is not a valid failure marker", marker.display()));
    std::fs::read_to_string(marker)
        .map_err(|_| invalid())?
        .lines()
        .map(|hex| MerkleHash::from_hex(hex).map_err(|_| invalid()))
        .collect()
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
    use crate::{GitHfError, HexHash, PointerError, XetPointer};

    // The file hash a clean output's pointer names.
    fn pointer_hash(pointer: &[u8]) -> anyhow::Result<MerkleHash> {
        Ok(MerkleHash::from_hex(XetPointer::parse(pointer)?.unwrap().hash.as_str())?)
    }
    use crate::selection::{BINARY_LIMIT, TEXT_LIMIT};

    // The Hub judges the first 512 bytes, so a binary byte past the pointer-sized prefix counts.
    #[test]
    #[serial(env_var_write_read)]
    fn binary_detection_uses_the_full_sample() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(repo.path())?;
        let session = CleanSession::new()?;
        let mut data = vec![b'a'; BINARY_LIMIT + 1];
        data[300] = 0;
        let out = session.clean(&data)?;
        assert!(XetPointer::parse(&out)?.is_some());
        data[300] = b'a';
        assert_eq!(session.clean(&data)?, data);
        session.finish_blocking()?;
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn concurrent_file_cleaners_share_aggregation() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(repo.path())?;
        let session = CleanSession::new()?;
        let pointers = std::thread::scope(|scope| {
            let first = scope.spawn(|| session.clean(&vec![0; BINARY_LIMIT + 1]));
            let second = scope.spawn(|| session.clean(&vec![1; BINARY_LIMIT + 1]));
            Ok::<_, GitHfError>([first.join().unwrap()?, second.join().unwrap()?])
        })?;
        assert_ne!(pointers[0], pointers[1]);
        let root = session.state_blocking()?.staging.clone();
        session.finish_blocking()?;
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
        let a_pointer = first.clean(&a)?;
        let a_hash = pointer_hash(&a_pointer)?;
        let second = CleanSession::new()?;
        second.clean(&b)?;
        assert_ne!(first.state_blocking()?.scratch.path(), second.state_blocking()?.scratch.path());
        assert_eq!(first.state_blocking()?.staging, second.state_blocking()?.staging);
        let root = first.state_blocking()?.staging.clone();
        first.finish_blocking()?;
        let first_xorb = fs::read_dir(root.join("xorbs"))?.next().unwrap()?.file_name();
        assert_eq!(second.clean(&a)?, a_pointer);
        second.finish_blocking()?;
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
        third.state_blocking()?;
        fs::rename(root.join("xorbs"), root.join("saved-xorbs"))?;
        fs::write(root.join("xorbs"), b"not a directory")?;
        assert_eq!(third.clean(&a)?, a_pointer);
        third.finish_blocking()?;
        fs::remove_file(root.join("xorbs"))?;
        fs::rename(root.join("saved-xorbs"), root.join("xorbs"))?;
        assert_eq!(fs::read_dir(root)?.count(), 2);
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn finalize_failure_records_unbacked_hashes() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(repo.path())?;
        let cleaner = CleanSession::new()?;
        let data = vec![b'a'; TEXT_LIMIT + 1];
        let result = cleaner.clean(&data)?;
        assert_eq!(XetPointer::parse(&result)?.unwrap().size, data.len() as u64);
        let staging = GitRepo::open(repo.path())?.common_dir()?.join("xet/staging");
        let root = staging;
        // The remaining partial xorb is written at finalization. Make that destination unwritable.
        fs::rename(root.join("xorbs"), root.join("saved-xorbs"))?;
        fs::write(root.join("xorbs"), b"not a directory")?;
        assert!(cleaner.finish_blocking().is_err());
        assert_eq!(fs::read_dir(root.join("shards"))?.count(), 0);
        // The pointer above is unbacked; the marker lists its hash.
        let markers = fs::read_dir(root.join(FINALIZE_FAILED_DIR))?.collect::<std::io::Result<Vec<_>>>()?;
        assert_eq!(markers.len(), 1);
        assert_eq!(read_failure_marker(&markers[0].path())?, [pointer_hash(&result)?]);
        Ok(())
    }

    // Git's checkout conversion can give a pointer CRLF endings; clean stores it with LF endings.
    #[test]
    #[serial(env_var_write_read)]
    fn crlf_pointers_are_stored_with_lf_endings() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(repo.path())?;
        let cleaner = CleanSession::new()?;
        let pointer = XetPointer::new(HexHash::from_hex(&MerkleHash::default().hex()).unwrap(), 42)
            .to_string()
            .replace("size 42\n", "size 42\nextension hello\n");
        let crlf = pointer.replace('\n', "\r\n");
        assert_eq!(cleaner.clean(crlf.as_bytes())?, pointer.as_bytes());
        // CRLF is undone, but the pointer must still be valid.
        let broken = crlf.replace("size 42", "size x");
        assert!(matches!(
            cleaner.clean(broken.as_bytes()),
            Err(GitHfError::InvalidPointer {
                source: PointerError::InvalidSize,
                ..
            })
        ));
        assert!(cleaner.state.get().is_none());
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn selection_and_persistent_shared_xorbs() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(repo.path())?;
        let cleaner = CleanSession::new()?;
        for data in [Vec::new(), vec![b'a'; TEXT_LIMIT], vec![0; BINARY_LIMIT]] {
            assert_eq!(cleaner.clean(&data)?, data);
        }
        assert!(cleaner.state.get().is_none());
        let pointer = XetPointer::new(HexHash::from_hex(&MerkleHash::default().hex()).unwrap(), 42)
            .to_string()
            .replace("size 42\n", "size 42\nextension hello\n");
        assert_eq!(cleaner.clean(pointer.as_bytes())?, pointer.as_bytes());
        assert!(cleaner.clean(b"version https://hf.co/spec/xet/v1\n").is_err());
        let mut hashes = Vec::new();
        for byte in [b'a', b'b'] {
            let mut data = vec![byte; BINARY_LIMIT + 1];
            data[0] = 0;
            let out = cleaner.clean(&data)?;
            hashes.push(pointer_hash(&out)?);
        }
        let stage = GitRepo::open(repo.path())?.common_dir()?.join("xet/staging");
        let root = stage.clone();
        assert_eq!(fs::read_dir(root.join("shards"))?.count(), 0);
        cleaner.finish_blocking()?;
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
        second.clean(&data)?;
        second.finish_blocking()?;
        assert_eq!(fs::read_dir(stage)?.count(), 2);
        assert_eq!(fs::read_dir(root.join("xorbs"))?.count(), 2);
        Ok(())
    }
}
