//! Uploads the locally staged Xet data referenced by pushed Git history.
//!
//! Selection follows Git reachability, not the branch checked out during clean: the pointer blobs
//! newly reachable from the pushed refs name file hashes, and staged shards map those to file
//! records and xorbs. A staged xorb table lists only local xorbs not yet uploaded, so a referenced
//! xorb in some table must be uploaded (a missing file is a local error); one in no table came from
//! remote dedup or an earlier push. Pointers without a staged record are left to server-side
//! validation.
//!
//! Whole xorbs are uploaded, including chunks of unrelated files, then new shards registering just
//! the pushed files. Nothing local changes until every upload succeeds, so a failed push retries in
//! full. Afterwards the uploaded shards go to the endpoint's global shard cache, which keeps their
//! chunks available for dedup, and the uploaded data leaves staging: staged shards are rewritten
//! without the uploaded xorbs and registered files, then the xorb files are deleted. Records of
//! unpushed files stay staged. Their xorbs may now exist only on the Hub that received this push.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{BufRead, BufReader, Cursor, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;

use bytes::Bytes;
use tokio::task::JoinSet;
use xet_client::cas_client::Client;
use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::metadata_shard::file_structs::MDBFileInfo;
use xet_core_structures::metadata_shard::shard_in_memory::MDBInMemoryShard;
use xet_core_structures::metadata_shard::streaming_shard::MDBMinimalShard;
use xet_core_structures::metadata_shard::xorb_structs::MDBXorbInfo;
use xet_core_structures::metadata_shard::{
    MDB_SHARD_LOCAL_CACHE_EXPIRATION, MDB_SHARD_TARGET_SIZE, MDBShardFileHeader, MDBShardInfo,
};
use xet_core_structures::xorb_object::{SerializedXorbObject, XorbObject};

use crate::clean_session::{FINALIZE_FAILED_DIR, read_failure_marker};
use crate::staging::lock_exclusive;
use crate::{GitHfError, MAX_POINTER_SIZE, Result, XetPointer};

/// One line of pre-push input: `<local ref> <local oid> <remote ref> <remote oid>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RefUpdate {
    pub(crate) local_ref: String,
    pub(crate) local_oid: String,
    pub(crate) remote_ref: String,
    pub(crate) remote_oid: String,
}

fn is_zero_oid(oid: &str) -> bool {
    oid.bytes().all(|b| b == b'0')
}

pub(crate) fn parse_updates(input: &[u8]) -> Result<Vec<RefUpdate>> {
    let input = std::str::from_utf8(input).map_err(|_| GitHfError::Internal("Non-UTF-8 pre-push input".into()))?;
    input
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| match line.split(' ').collect::<Vec<_>>()[..] {
            [local_ref, local_oid, remote_ref, remote_oid] => Ok(RefUpdate {
                local_ref: local_ref.to_owned(),
                local_oid: local_oid.to_owned(),
                remote_ref: remote_ref.to_owned(),
                remote_oid: remote_oid.to_owned(),
            }),
            _ => Err(GitHfError::Internal(format!("Malformed pre-push input line: {line}"))),
        })
        .collect()
}

/// The ref whose write permission authorizes the upload: the first updated ref, as a branch name.
pub(crate) fn token_reference(updates: &[RefUpdate]) -> Option<String> {
    updates
        .iter()
        .find(|u| !is_zero_oid(&u.local_oid))
        .map(|u| u.remote_ref.strip_prefix("refs/heads/").unwrap_or(&u.remote_ref).to_owned())
}

/// Staged file records and locally written xorbs, read from every published staged shard.
pub(crate) struct StagedIndex {
    files: HashMap<MerkleHash, MDBFileInfo>,
    xorbs: HashMap<MerkleHash, MDBXorbInfo>,
}

impl StagedIndex {
    pub(crate) fn load(staging: &Path) -> Result<Self> {
        let mut index = Self {
            files: HashMap::new(),
            xorbs: HashMap::new(),
        };
        for path in staged_shards(staging)? {
            let shard = read_shard(&path)?;
            for i in 0..shard.num_xorb() {
                let xorb = shard.xorb(i).unwrap();
                index.xorbs.entry(xorb.xorb_hash()).or_insert_with(|| MDBXorbInfo::from(xorb));
            }
            for i in 0..shard.num_files() {
                let file = shard.file(i).unwrap();
                index.files.entry(file.file_hash()).or_insert_with(|| MDBFileInfo::from(file));
            }
        }
        Ok(index)
    }

    pub(crate) fn contains_file(&self, hash: &MerkleHash) -> bool {
        self.files.contains_key(hash)
    }
}

/// Paths of the published staged shards.
fn staged_shards(staging: &Path) -> Result<Vec<PathBuf>> {
    let entries = match std::fs::read_dir(staging.join("shards")) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "mdb") {
            paths.push(path);
        }
    }
    Ok(paths)
}

fn read_shard(path: &Path) -> Result<MDBMinimalShard> {
    Ok(MDBMinimalShard::from_reader(&mut Cursor::new(std::fs::read(path)?), true, true)?)
}

/// Fails if an earlier clean session could not finalize and any pointer it returned is still
/// unbacked. Resolution is by file hash, not by the current index: an unbacked pointer may be
/// committed even after its path is deleted or replaced. Markers whose hashes are all staged are
/// removed.
pub(crate) fn check_failure_markers(staging: &Path, index: &StagedIndex) -> Result<()> {
    let entries = match std::fs::read_dir(staging.join(FINALIZE_FAILED_DIR)) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let mut unresolved = Vec::new();
    for entry in entries {
        let path = entry?.path();
        let marker = read_failure_marker(&path)?;
        let pending: Vec<_> = marker
            .files
            .iter()
            .filter(|(hash, _)| !index.contains_file(hash))
            .map(|(_, path)| String::from_utf8_lossy(path).into_owned())
            .collect();
        if pending.is_empty() {
            std::fs::remove_file(&path)?;
            continue;
        }
        unresolved.push(format!(
            "  in {}: git add --renormalize -- {}\n  (marker: {})",
            marker.worktree.display(),
            pending.join(" "),
            path.display()
        ));
    }
    if unresolved.is_empty() {
        return Ok(());
    }
    Err(GitHfError::Staging(format!(
        "an earlier `git add` could not save these files' data. Re-stage the same content and push again:\n{}\n\
         If that content no longer exists, delete the marker; the Hub will then reject any push containing those \
         pointers.",
        unresolved.join("\n")
    )))
}

/// File hashes of the pointer blobs newly reachable from the pushed refs. Objects reachable from a
/// remote ref's previous value are excluded when present locally: the server already holds that
/// history. New refs include their full history. Deletions contribute nothing.
pub(crate) fn pushed_pointers(wd: &Path, updates: &[RefUpdate]) -> Result<Vec<MerkleHash>> {
    let repo = git2::Repository::discover(wd)?;
    let odb = repo.odb()?;
    let mut revs = String::new();
    for update in updates.iter().filter(|u| !is_zero_oid(&u.local_oid)) {
        revs.push_str(&update.local_oid);
        revs.push('\n');
    }
    if revs.is_empty() {
        return Ok(Vec::new());
    }
    for update in updates.iter().filter(|u| !is_zero_oid(&u.remote_oid)) {
        if git2::Oid::from_str(&update.remote_oid).is_ok_and(|oid| odb.exists(oid)) {
            revs.push_str(&format!("^{}\n", update.remote_oid));
        }
    }
    // Larger blobs cannot be pointers; Git omits them before they are listed. Git's limit is
    // exclusive, while a pointer may be exactly MAX_POINTER_SIZE bytes.
    let filter = format!("--filter=blob:limit={}", MAX_POINTER_SIZE + 1);
    let mut child = Command::new("git")
        .current_dir(wd)
        .args(["rev-list", "--objects", "--no-object-names", &filter, "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    // rev-list reads all of stdin before writing, so this cannot deadlock.
    child.stdin.take().unwrap().write_all(revs.as_bytes())?;
    let mut hashes = Vec::new();
    for line in BufReader::new(child.stdout.take().unwrap()).lines() {
        let oid = git2::Oid::from_str(line?.trim())?;
        let (size, kind) = odb.read_header(oid)?;
        if kind != git2::ObjectType::Blob || size > MAX_POINTER_SIZE {
            continue;
        }
        // Malformed pointers cannot be uploaded; the server rejects them.
        if let Ok(Some(pointer)) = XetPointer::parse(odb.read(oid)?.data()) {
            hashes.push(pointer.hash);
        }
    }
    let status = child.wait()?;
    if !status.success() {
        return Err(GitHfError::CommandFailed {
            reason: format!("git rev-list exited with {status}"),
            source: None,
        });
    }
    hashes.sort();
    hashes.dedup();
    Ok(hashes)
}

/// Files to register and local xorbs to upload for one push.
pub(crate) struct UploadPlan {
    files: Vec<MDBFileInfo>,
    /// Every staged xorb the files reference; each is uploaded and its block registered with the files.
    xorbs: Vec<MDBXorbInfo>,
}

impl UploadPlan {
    /// Selects the staged files for `pointers`, and verifies their local xorbs exist before any
    /// network work.
    pub(crate) fn new(staging: &Path, index: &StagedIndex, pointers: &[MerkleHash]) -> Result<Self> {
        let files: Vec<_> = pointers.iter().filter_map(|hash| index.files.get(hash).cloned()).collect();
        let mut xorbs = BTreeMap::new();
        for file in &files {
            for segment in &file.segments {
                if let Some(xorb) = index.xorbs.get(&segment.xorb_hash) {
                    xorbs.insert(segment.xorb_hash, xorb.clone());
                }
            }
        }
        for hash in xorbs.keys() {
            let path = xorb_path(staging, hash);
            if !path.is_file() {
                return Err(GitHfError::Staging(format!(
                    "{} is listed in a staged shard but missing; staging is incomplete",
                    path.display()
                )));
            }
        }
        Ok(Self {
            files,
            xorbs: xorbs.into_values().collect(),
        })
    }
}

fn xorb_path(staging: &Path, hash: &MerkleHash) -> PathBuf {
    staging.join("xorbs").join(format!("{}.xorb", hash.hex()))
}

/// Counts reported after a successful upload.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct UploadSummary {
    pub(crate) xorbs: usize,
    pub(crate) xorb_bytes: u64,
    pub(crate) files: usize,
}

/// Uploads the plan's xorbs, then registers its files in shards. Once all are uploaded, the shards
/// move to `cache`, the endpoint's global shard cache.
pub(crate) async fn upload(
    client: Arc<dyn Client>,
    prefix: &str,
    staging: &Path,
    plan: &UploadPlan,
    cache: &Path,
) -> Result<UploadSummary> {
    let mut summary = UploadSummary::default();
    let mut tasks = JoinSet::new();
    for xorb in &plan.xorbs {
        // Holding a permit before reading bounds the xorbs in memory.
        let permit = client.acquire_upload_permit().await?;
        let hash = xorb.metadata.xorb_hash;
        let (client, prefix, path) = (client.clone(), prefix.to_owned(), xorb_path(staging, &hash));
        tasks.spawn(async move {
            let data = tokio::fs::read(&path).await?;
            let xorb = XorbObject::deserialize(&mut Cursor::new(&data))?;
            let object = SerializedXorbObject {
                hash,
                raw_num_bytes: xorb.uncompressed_range_length(0, xorb.info.num_chunks)? as u64,
                num_chunks: xorb.info.num_chunks as usize,
                footer_start: Some(xorb.get_contents_length()? as u64),
                serialized_data: data,
            };
            Ok::<_, GitHfError>(client.upload_xorb(&prefix, object, None, permit).await?)
        });
        while let Some(result) = tasks.try_join_next() {
            record_xorb(result, &mut summary)?;
        }
    }
    while let Some(result) = tasks.join_next().await {
        record_xorb(result, &mut summary)?;
    }

    // Register files only after every xorb they reference is available.
    let mut shard = MDBInMemoryShard::default();
    let mut shard_files = Vec::new();
    let mut uploaded = Vec::new();
    for xorb in &plan.xorbs {
        shard.add_xorb_block(xorb.clone())?;
        if shard.shard_file_size() >= *MDB_SHARD_TARGET_SIZE {
            upload_shard(&client, std::mem::take(&mut shard), &mut shard_files, &mut uploaded, &mut summary).await?;
        }
    }
    for file in &plan.files {
        shard.add_file_reconstruction_info(file.clone())?;
        shard_files.push(file.metadata.file_hash);
        if shard.shard_file_size() >= *MDB_SHARD_TARGET_SIZE {
            upload_shard(&client, std::mem::take(&mut shard), &mut shard_files, &mut uploaded, &mut summary).await?;
        }
    }
    if !shard.is_empty() {
        upload_shard(&client, shard, &mut shard_files, &mut uploaded, &mut summary).await?;
    }
    cache_shards(&uploaded, cache);
    Ok(summary)
}

fn record_xorb(
    result: std::result::Result<Result<u64>, tokio::task::JoinError>,
    summary: &mut UploadSummary,
) -> Result<()> {
    let bytes = result.map_err(|error| GitHfError::Internal(error.to_string()))??;
    summary.xorbs += 1;
    summary.xorb_bytes += bytes;
    Ok(())
}

async fn upload_shard(
    client: &Arc<dyn Client>,
    shard: MDBInMemoryShard,
    files: &mut Vec<MerkleHash>,
    uploaded: &mut Vec<MDBInMemoryShard>,
    summary: &mut UploadSummary,
) -> Result<()> {
    let permit = client.acquire_upload_permit().await?;
    client.upload_shard(without_footer(shard.to_bytes()?)?, permit, None).await?;
    summary.files += files.len();
    files.clear();
    uploaded.push(shard);
    Ok(())
}

// The cache only enables local dedup; losing a shard there never affects the push.
fn cache_shards(shards: &[MDBInMemoryShard], cache: &Path) {
    let written = std::fs::create_dir_all(cache).map_err(GitHfError::from).and_then(|_| {
        for shard in shards {
            shard.write_to_directory(cache, Some(*MDB_SHARD_LOCAL_CACHE_EXPIRATION))?;
        }
        Ok(())
    });
    if let Err(error) = written {
        eprintln!("git-hf: could not write uploaded shards to {}: {error}", cache.display());
    }
}

/// Removes the plan's uploaded data from staging, after a successful [`upload`]. Shards are rewritten
/// before xorbs are deleted, so an interruption never leaves a listed xorb missing; a leftover
/// unlisted xorb file is harmless.
pub(crate) fn release_uploaded(staging: &Path, plan: &UploadPlan) -> Result<()> {
    let _lock = lock_exclusive(staging)?;
    let xorbs: HashSet<_> = plan.xorbs.iter().map(|xorb| xorb.metadata.xorb_hash).collect();
    let files: HashSet<_> = plan.files.iter().map(|file| file.metadata.file_hash).collect();
    let directory = staging.join("shards");
    // Shards published after the index was loaded are rescanned: they may list the same files.
    for path in staged_shards(staging)? {
        let shard = read_shard(&path)?;
        let mut kept = MDBInMemoryShard::default();
        let mut changed = false;
        for xorb in (0..shard.num_xorb()).map(|i| shard.xorb(i).unwrap()) {
            if xorbs.contains(&xorb.xorb_hash()) {
                changed = true;
            } else {
                kept.add_xorb_block(MDBXorbInfo::from(xorb))?;
            }
        }
        for file in (0..shard.num_files()).map(|i| shard.file(i).unwrap()) {
            if files.contains(&file.file_hash()) {
                changed = true;
            } else {
                kept.add_file_reconstruction_info(MDBFileInfo::from(file))?;
            }
        }
        if !changed {
            continue;
        }
        if !kept.is_empty() {
            kept.write_to_directory(&directory, None)?;
        }
        std::fs::remove_file(&path)?;
    }
    for hash in &xorbs {
        match std::fs::remove_file(xorb_path(staging, hash)) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
            _ => {},
        }
    }
    Ok(())
}

// Shards are uploaded without the footer and lookup tables, as in a normal upload session.
fn without_footer(mut bytes: Vec<u8>) -> Result<Bytes> {
    let info = MDBShardInfo::load_from_reader(&mut Cursor::new(&bytes))?;
    bytes.truncate(info.metadata.file_lookup_offset as usize);
    let mut header = info.header;
    header.footer_size = 0;
    header.serialize(&mut &mut bytes[..size_of::<MDBShardFileHeader>()])?;
    Ok(bytes.into())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use git_extension_common::GitRepo;
    use git_extension_common::process_wrapping::{run_git_captured, run_git_captured_with_input_and_output};
    use git_extension_common::test_utils::TestRepo;
    use serial_test::serial;
    use xet_client::cas_client::{DirectAccessClient, MemoryClient};
    use xet_runtime::core::XetContext;
    use xet_runtime::utils::CwdGuard;

    use super::*;
    use crate::clean_session::CleanSession;
    use crate::selection::BINARY_LIMIT;

    const ZERO: &str = "0000000000000000000000000000000000000000";

    fn binary(byte: u8, len: usize) -> Vec<u8> {
        let mut data = vec![byte; len];
        data[0] = 0;
        data
    }

    // Stages each file through a clean session and commits its pointer without a filter.
    fn commit_staged(repo: &TestRepo, files: &[(&str, &[u8])], message: &str) -> anyhow::Result<String> {
        let session = CleanSession::new()?;
        for (path, data) in files {
            let pointer = session.clean(&mut &data[..])?;
            fs::write(repo.path().join(path), pointer)?;
        }
        session.finish()?;
        run_git_captured(repo.path(), "add", ["-A"])?;
        run_git_captured(repo.path(), "commit", ["-m", message])?;
        head(repo)
    }

    fn head(repo: &TestRepo) -> anyhow::Result<String> {
        let (out, _) =
            run_git_captured_with_input_and_output(repo.path(), "rev-parse", ["HEAD"])?.wait_with_output()?;
        Ok(String::from_utf8(out)?.trim().to_owned())
    }

    fn update(local: &str, remote: &str) -> RefUpdate {
        RefUpdate {
            local_ref: "refs/heads/main".into(),
            local_oid: local.into(),
            remote_ref: "refs/heads/main".into(),
            remote_oid: remote.into(),
        }
    }

    struct Pushed {
        summary: UploadSummary,
        client: Arc<MemoryClient>,
        cache: tempfile::TempDir,
    }

    fn staging(repo: &TestRepo) -> anyhow::Result<PathBuf> {
        Ok(GitRepo::open(repo.path())?.common_dir()?.join("xet/staging"))
    }

    // Runs the pre-push steps after authentication against `client`, or a fresh remote.
    fn push(repo: &TestRepo, updates: &[RefUpdate], client: Option<Arc<MemoryClient>>) -> anyhow::Result<Pushed> {
        let ctx = XetContext::default()?;
        let client = client.unwrap_or_else(|| MemoryClient::new(ctx.clone()));
        let cache = tempfile::tempdir()?;
        let staging = staging(repo)?;
        let index = StagedIndex::load(&staging)?;
        check_failure_markers(&staging, &index)?;
        let pointers = pushed_pointers(repo.path(), updates)?;
        let plan = UploadPlan::new(&staging, &index, &pointers)?;
        let summary =
            ctx.runtime
                .handle()
                .block_on(upload(client.clone(), "default", &staging, &plan, cache.path()))?;
        release_uploaded(&staging, &plan)?;
        Ok(Pushed { summary, client, cache })
    }

    fn count(directory: &Path) -> anyhow::Result<usize> {
        Ok(fs::read_dir(directory)?.count())
    }

    fn file_hash(pointer_path: &Path) -> anyhow::Result<MerkleHash> {
        Ok(XetPointer::parse(&fs::read(pointer_path)?)?.unwrap().hash)
    }

    #[test]
    #[serial(env_var_write_read)]
    fn parses_pre_push_input() -> anyhow::Result<()> {
        let input = format!("refs/heads/a 1111 refs/heads/a {ZERO}\nrefs/tags/t {ZERO} refs/tags/t 2222\n");
        let updates = parse_updates(input.as_bytes())?;
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[1].remote_oid, "2222");
        assert_eq!(token_reference(&updates), Some("a".into()));
        assert!(parse_updates(b"too few fields\n").is_err());
        assert!(parse_updates(b"")?.is_empty());
        Ok(())
    }

    // History beyond the tip is uploaded for a new ref, shared xorbs once, and remote data reconstructs.
    // The upload then moves from staging to the global shard cache.
    #[test]
    #[serial(env_var_write_read)]
    fn new_ref_uploads_full_history_and_reconstructs() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(repo.path())?;
        let (a, b) = (binary(b'a', BINARY_LIMIT + 1), binary(b'b', BINARY_LIMIT + 1));
        commit_staged(&repo, &[("a", &a), ("b", &b)], "two files")?;
        let (a_hash, b_hash) = (file_hash(&repo.path().join("a"))?, file_hash(&repo.path().join("b"))?);
        // The file is deleted at the tip, but its history is still pushed.
        fs::remove_file(repo.path().join("a"))?;
        run_git_captured(repo.path(), "commit", ["-am", "remove a"])?;

        let pushed = push(&repo, &[update(&head(&repo)?, ZERO)], None)?;
        // Both files share one staged xorb.
        assert_eq!((pushed.summary.xorbs, pushed.summary.files), (1, 2));
        let ctx = XetContext::default()?;
        ctx.runtime.handle().block_on(async {
            assert_eq!(pushed.client.get_file_data(&a_hash, None).await?, a);
            assert_eq!(pushed.client.get_file_data(&b_hash, None).await?, b);
            anyhow::Ok(())
        })?;

        let staging = staging(&repo)?;
        assert_eq!(count(&staging.join("xorbs"))?, 0);
        assert_eq!(count(&staging.join("shards"))?, 0);
        let cached: Vec<_> = fs::read_dir(pushed.cache.path())?.collect::<std::io::Result<_>>()?;
        assert_eq!(cached.len(), 1);
        let shard = MDBShardInfo::load_from_reader(&mut fs::File::open(cached[0].path())?)?;
        assert_eq!((shard.num_xorb_entries(), shard.num_file_entries()), (1, 2));
        assert!(shard.metadata.shard_key_expiry < u64::MAX);

        // A later push of the same history has nothing left to do.
        let again = push(&repo, &[update(&head(&repo)?, ZERO)], Some(pushed.client))?;
        assert_eq!(again.summary, UploadSummary::default());
        Ok(())
    }

    // Unpushed files keep their records after a push uploads the xorb they share with pushed files,
    // and a later push registers them without uploading it again.
    #[test]
    #[serial(env_var_write_read)]
    fn unpushed_files_stay_staged_and_push_later() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(repo.path())?;
        let (a, b) = (binary(b'a', BINARY_LIMIT + 1), binary(b'b', BINARY_LIMIT + 1));
        let session = CleanSession::new()?;
        let (a_pointer, b_pointer) = (session.clean(&mut a.as_slice())?, session.clean(&mut b.as_slice())?);
        session.finish()?;
        fs::write(repo.path().join("a"), a_pointer)?;
        run_git_captured(repo.path(), "add", ["a"])?;
        run_git_captured(repo.path(), "commit", ["-m", "a"])?;
        let first = head(&repo)?;

        let pushed = push(&repo, &[update(&first, ZERO)], None)?;
        assert_eq!((pushed.summary.xorbs, pushed.summary.files), (1, 1));
        let staging = staging(&repo)?;
        assert_eq!(count(&staging.join("xorbs"))?, 0);
        let index = StagedIndex::load(&staging)?;
        assert_eq!(index.files.len(), 1);
        assert!(index.xorbs.is_empty());

        fs::write(repo.path().join("b"), b_pointer)?;
        run_git_captured(repo.path(), "add", ["b"])?;
        run_git_captured(repo.path(), "commit", ["-m", "b"])?;
        let b_hash = file_hash(&repo.path().join("b"))?;
        let again = push(&repo, &[update(&head(&repo)?, &first)], Some(pushed.client))?;
        assert_eq!((again.summary.xorbs, again.summary.files), (0, 1));
        let ctx = XetContext::default()?;
        assert_eq!(ctx.runtime.handle().block_on(again.client.get_file_data(&b_hash, None))?, b);
        assert_eq!(count(&staging.join("shards"))?, 0);
        Ok(())
    }

    // Staged data is not removed while a clean session may read it.
    #[test]
    #[serial(env_var_write_read)]
    fn release_waits_for_clean_sessions() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(repo.path())?;
        let tip = commit_staged(&repo, &[("a", &binary(b'a', BINARY_LIMIT + 1))], "a")?;
        let staging = staging(&repo)?;
        let index = StagedIndex::load(&staging)?;
        let plan = UploadPlan::new(&staging, &index, &pushed_pointers(repo.path(), &[update(&tip, ZERO)])?)?;

        let shared = crate::staging::lock_shared(&staging)?;
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| sender.send(release_uploaded(&staging, &plan)).unwrap());
            assert!(receiver.recv_timeout(std::time::Duration::from_millis(300)).is_err());
            assert_eq!(count(&staging.join("xorbs")).unwrap(), 1);
            drop(shared);
            receiver.recv().unwrap()
        })?;
        assert_eq!(count(&staging.join("xorbs"))?, 0);
        Ok(())
    }

    // An update excludes history the remote already has; deletions and unstaged pointers upload nothing.
    #[test]
    #[serial(env_var_write_read)]
    fn update_excludes_remote_history_and_skips_unstaged() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(repo.path())?;
        let first = commit_staged(&repo, &[("a", &binary(b'a', BINARY_LIMIT + 1))], "a")?;
        let second = commit_staged(&repo, &[("b", &binary(b'b', BINARY_LIMIT + 1))], "b")?;
        let b_hash = file_hash(&repo.path().join("b"))?;

        let pointers = pushed_pointers(repo.path(), &[update(&second, &first)])?;
        assert_eq!(pointers, [b_hash]);
        let deletion = RefUpdate {
            local_oid: ZERO.into(),
            ..update(ZERO, &second)
        };
        assert!(pushed_pointers(repo.path(), &[deletion])?.is_empty());

        // A pointer with no staged record (e.g. fetched from the remote) is left to the server.
        let foreign = XetPointer::new(MerkleHash::from([9, 9, 9, 9]), 1234).to_string();
        fs::write(repo.path().join("remote-only"), foreign)?;
        run_git_captured(repo.path(), "add", ["remote-only"])?;
        run_git_captured(repo.path(), "commit", ["-m", "remote-only"])?;
        let pushed = push(&repo, &[update(&head(&repo)?, &second)], None)?;
        assert_eq!(pushed.summary, UploadSummary::default());
        Ok(())
    }

    // Git's blob size limit is exclusive; a pointer of exactly MAX_POINTER_SIZE bytes is still found.
    #[test]
    #[serial(env_var_write_read)]
    fn pointer_of_maximum_size_is_selected() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let hash = MerkleHash::from([7, 7, 7, 7]);
        let pointer = XetPointer::new(hash, 1).to_string();
        let padding = MAX_POINTER_SIZE - pointer.len() - "extension \n".len();
        let pointer = format!("{pointer}extension {}\n", "a".repeat(padding));
        assert_eq!(pointer.len(), MAX_POINTER_SIZE);
        assert_eq!(XetPointer::parse(pointer.as_bytes())?.unwrap().hash, hash);
        fs::write(repo.path().join("pointer"), &pointer)?;
        run_git_captured(repo.path(), "add", ["pointer"])?;
        run_git_captured(repo.path(), "commit", ["-m", "max-size pointer"])?;
        assert_eq!(pushed_pointers(repo.path(), &[update(&head(&repo)?, ZERO)])?, [hash]);
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn missing_local_xorb_fails_before_upload() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(repo.path())?;
        let tip = commit_staged(&repo, &[("a", &binary(b'a', BINARY_LIMIT + 1))], "a")?;
        let staging = staging(&repo)?;
        for entry in fs::read_dir(staging.join("xorbs"))? {
            fs::remove_file(entry?.path())?;
        }
        let result = push(&repo, &[update(&tip, ZERO)], None);
        assert!(matches!(result.err().unwrap().downcast::<GitHfError>()?, GitHfError::Staging(_)));
        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn failure_marker_blocks_until_hashes_are_restaged() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let _cwd = CwdGuard::set(repo.path())?;
        let data = binary(b'a', BINARY_LIMIT + 1);
        // Stage a pointer whose session fails to finalize.
        let staging = staging(&repo)?;
        let session = CleanSession::new()?;
        let pointer = session.clean(&mut data.as_slice())?;
        fs::rename(staging.join("xorbs"), staging.join("saved-xorbs"))?;
        fs::write(staging.join("xorbs"), b"blocked")?;
        assert!(session.finish().is_err());
        fs::remove_file(staging.join("xorbs"))?;
        fs::rename(staging.join("saved-xorbs"), staging.join("xorbs"))?;
        fs::write(repo.path().join("file"), &pointer)?;
        run_git_captured(repo.path(), "add", ["file"])?;
        run_git_captured(repo.path(), "commit", ["-m", "unbacked"])?;

        let tip = head(&repo)?;
        let error = push(&repo, &[update(&tip, ZERO)], None)
            .err()
            .unwrap()
            .downcast::<GitHfError>()?;
        assert!(matches!(&error, GitHfError::Staging(message) if message.contains("--renormalize -- file")));

        // Deleting the path does not resolve the marker: the unbacked pointer is still in history.
        fs::remove_file(repo.path().join("file"))?;
        run_git_captured(repo.path(), "commit", ["-am", "delete"])?;
        let tip = head(&repo)?;
        let error = push(&repo, &[update(&tip, ZERO)], None)
            .err()
            .unwrap()
            .downcast::<GitHfError>()?;
        assert!(matches!(error, GitHfError::Staging(_)));

        // Re-staging the same content (as `git add --renormalize` would) clears the marker.
        let session = CleanSession::new()?;
        assert_eq!(session.clean(&mut data.as_slice())?, pointer);
        session.finish()?;
        let pushed = push(&repo, &[update(&tip, ZERO)], None)?;
        assert_eq!(pushed.summary.files, 1);
        assert!(fs::read_dir(staging.join(FINALIZE_FAILED_DIR))?.next().is_none());
        Ok(())
    }
}
