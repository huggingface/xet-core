//! Reconstructs the Xet files referenced by a local revision into the file cache.
//!
//! Pointers come from the revision's tree, not the index or worktree, and no Git refs are fetched.
//! Each file is downloaded to a temporary file and published only once complete and of the size its
//! pointer records, so an interrupted fetch resumes with just the files still missing.
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use tokio::task::JoinSet;
use xet_data::processing::{FileDownloadSession, XetFileInfo};
use xet_runtime::core::XetContext;

use crate::file_cache::FileCache;
use crate::{GitHfError, MAX_POINTER_SIZE, Result, XetPointer};

/// The distinct pointers in `revision`'s tree, by file hash. Unsupported or malformed pointers fail
/// with their paths, before any network work.
pub(crate) fn pointers_at(wd: &Path, revision: &str) -> Result<Vec<XetPointer>> {
    let repo = git2::Repository::discover(wd)?;
    let tree = repo.revparse_single(revision)?.peel_to_tree()?;
    let odb = repo.odb()?;
    let mut pointers = BTreeMap::new();
    let mut invalid = Vec::new();
    let mut error = None;
    tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        // Symlinks and submodules are never pointers.
        if entry.kind() != Some(git2::ObjectType::Blob) || entry.filemode() == 0o120000 {
            return git2::TreeWalkResult::Ok;
        }
        let parsed = odb.read_header(entry.id()).and_then(|(size, _)| {
            if size > MAX_POINTER_SIZE {
                return Ok(Ok(None));
            }
            Ok(XetPointer::parse(odb.read(entry.id())?.data()))
        });
        match parsed {
            Ok(Ok(Some(pointer))) => {
                pointers.entry(pointer.hash).or_insert(pointer);
            },
            Ok(Ok(None)) => {},
            Ok(Err(e)) => invalid.push(format!("  {root}{}: {e}", String::from_utf8_lossy(entry.name_bytes()))),
            Err(e) => {
                error = Some(e);
                return git2::TreeWalkResult::Abort;
            },
        }
        git2::TreeWalkResult::Ok
    })?;
    if let Some(error) = error {
        return Err(error.into());
    }
    if !invalid.is_empty() {
        return Err(GitHfError::NotSupported(format!(
            "{revision} contains pointers git-hf cannot fetch:\n{}",
            invalid.join("\n")
        )));
    }
    Ok(pointers.into_values().collect())
}

/// Counts reported after a fetch.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct FetchSummary {
    pub(crate) files: usize,
    pub(crate) bytes: u64,
}

/// Downloads each pointer's file into `cache`, at most the configured number of files at a time.
/// Stops at the first failure; files already published stay cached.
pub(crate) async fn fetch_files(
    ctx: &XetContext,
    session: Arc<FileDownloadSession>,
    cache: &FileCache,
    pointers: Vec<XetPointer>,
) -> Result<FetchSummary> {
    let mut summary = FetchSummary::default();
    let mut tasks = JoinSet::new();
    for pointer in pointers {
        let permit = ctx
            .common
            .file_download_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| GitHfError::Internal(e.to_string()))?;
        let (session, temp) = (session.clone(), cache.temp_path()?);
        tasks.spawn(async move {
            let _permit = permit;
            let info = XetFileInfo::new(pointer.hash.hex(), pointer.size);
            session.download_file(&info, &temp).await?;
            Ok::<_, GitHfError>((pointer, temp))
        });
        while let Some(result) = tasks.try_join_next() {
            publish(result, cache, &mut summary)?;
        }
    }
    while let Some(result) = tasks.join_next().await {
        publish(result, cache, &mut summary)?;
    }
    Ok(summary)
}

fn publish(
    result: std::result::Result<Result<(XetPointer, tempfile::TempPath)>, tokio::task::JoinError>,
    cache: &FileCache,
    summary: &mut FetchSummary,
) -> Result<()> {
    let (pointer, temp) = result.map_err(|e| GitHfError::Internal(e.to_string()))??;
    cache.publish(temp, &pointer.hash)?;
    summary.files += 1;
    summary.bytes += pointer.size;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use git_extension_common::process_wrapping::run_git_captured;
    use git_extension_common::test_utils::TestRepo;
    use serial_test::serial;
    use xet_client::cas_client::MemoryClient;
    use xet_core_structures::merklehash::MerkleHash;
    use xet_data::processing::configurations::TranslatorConfig;
    use xet_data::processing::{FileUploadSession, Sha256Policy, UploadShardCachePolicy};

    use super::*;

    // Uploads each file to `client` and returns its pointer.
    fn upload(ctx: &XetContext, client: &Arc<MemoryClient>, files: &[&[u8]]) -> anyhow::Result<Vec<XetPointer>> {
        let dir = tempfile::tempdir()?;
        ctx.runtime.handle().block_on(async {
            let config = TranslatorConfig::memory_config(ctx, dir.path())?;
            let session = FileUploadSession::new_with_client(
                Arc::new(config),
                client.clone(),
                None,
                UploadShardCachePolicy::Skip,
            )
            .await?;
            let mut pointers = Vec::new();
            for data in files {
                let (_, mut cleaner) = session.start_clean(None, None, Sha256Policy::Skip)?;
                cleaner.add_data(data).await?;
                let (info, _) = cleaner.finish().await?;
                pointers.push(XetPointer::new(MerkleHash::from_hex(&info.hash)?, data.len() as u64));
            }
            session.finalize().await?;
            anyhow::Ok(pointers)
        })
    }

    fn commit(repo: &TestRepo, files: &[(&str, String)]) -> anyhow::Result<()> {
        for (path, content) in files {
            let path = repo.path().join(path);
            fs::create_dir_all(path.parent().unwrap())?;
            fs::write(path, content)?;
        }
        run_git_captured(repo.path(), "add", ["-A"])?;
        run_git_captured(repo.path(), "commit", ["-m", "files"])?;
        Ok(())
    }

    fn fetch(ctx: &XetContext, client: Arc<MemoryClient>, repo: &TestRepo) -> Result<FetchSummary> {
        let cache = FileCache::new(&repo.path().join(".git"))?;
        let missing = pointers_at(repo.path(), "HEAD")?
            .into_iter()
            .filter(|pointer| !cache.contains(pointer))
            .collect();
        let session = FileDownloadSession::from_client(ctx, client, None);
        ctx.runtime.handle().block_on(fetch_files(ctx, session, &cache, missing))
    }

    fn cache_entries(repo: &TestRepo) -> anyhow::Result<Vec<String>> {
        let mut names = fs::read_dir(repo.path().join(".git/xet/files"))?
            .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
            .collect::<anyhow::Result<Vec<_>>>()?;
        names.sort();
        Ok(names)
    }

    #[test]
    #[serial(env_var_write_read)]
    fn lists_distinct_pointers_and_rejects_unsupported_ones() -> anyhow::Result<()> {
        let repo = TestRepo::new("main")?;
        let (a, b) = (XetPointer::new(MerkleHash::from([1; 4]), 10), XetPointer::new(MerkleHash::from([2; 4]), 20));
        commit(
            &repo,
            &[
                ("a", a.to_string()),
                ("dir/copy-of-a", a.to_string()),
                ("dir/nested/b", b.to_string()),
                ("text", "ordinary text\n".into()),
            ],
        )?;
        let mut expected = vec![a.clone(), b.clone()];
        expected.sort_by_key(|pointer| pointer.hash);
        assert_eq!(pointers_at(repo.path(), "HEAD")?, expected);

        let unsupported = a.to_string().replace("https://", "https://example.com/");
        commit(&repo, &[("dir/bad", unsupported)])?;
        let error = pointers_at(repo.path(), "HEAD").unwrap_err();
        assert!(matches!(&error, GitHfError::NotSupported(message) if message.contains("dir/bad")), "{error}");
        // The earlier revision is still fetchable.
        assert_eq!(pointers_at(repo.path(), "HEAD~1")?, expected);
        Ok(())
    }

    // Shared files are downloaded once, a repeated fetch downloads nothing, and an interrupted fetch
    // resumes with only the missing files.
    #[test]
    #[serial(env_var_write_read)]
    fn fetches_missing_files_once() -> anyhow::Result<()> {
        let ctx = XetContext::default()?;
        let client = MemoryClient::new(ctx.clone());
        let (a, b) = (vec![1u8; 300_000], vec![2u8; 200_000]);
        let pointers = upload(&ctx, &client, &[&a, &b])?;
        let repo = TestRepo::new("main")?;
        commit(
            &repo,
            &[
                ("a", pointers[0].to_string()),
                ("same-as-a", pointers[0].to_string()),
                ("b", pointers[1].to_string()),
            ],
        )?;

        let summary = fetch(&ctx, client.clone(), &repo)?;
        assert_eq!(
            summary,
            FetchSummary {
                files: 2,
                bytes: 500_000
            }
        );
        let cache = FileCache::new(&repo.path().join(".git"))?;
        assert_eq!(fs::read(cache.path(&pointers[0].hash))?, a);
        assert_eq!(fs::read(cache.path(&pointers[1].hash))?, b);

        // Nothing is missing, so a client without the files is never asked for them.
        assert_eq!(fetch(&ctx, MemoryClient::new(ctx.clone()), &repo)?, FetchSummary::default());

        fs::remove_file(cache.path(&pointers[1].hash))?;
        assert_eq!(
            fetch(&ctx, client, &repo)?,
            FetchSummary {
                files: 1,
                bytes: 200_000
            }
        );
        assert_eq!(fs::read(cache.path(&pointers[1].hash))?, b);
        Ok(())
    }

    // A failed or mis-sized download publishes nothing and leaves no temporary file behind.
    #[test]
    #[serial(env_var_write_read)]
    fn failed_downloads_publish_nothing() -> anyhow::Result<()> {
        let ctx = XetContext::default()?;
        let client = MemoryClient::new(ctx.clone());
        let data = vec![3u8; 150_000];
        let pointer = upload(&ctx, &client, &[&data])?.remove(0);
        let repo = TestRepo::new("main")?;
        let wrong_size = XetPointer::new(pointer.hash, pointer.size + 1);
        commit(&repo, &[("file", wrong_size.to_string())])?;
        assert!(fetch(&ctx, client.clone(), &repo).is_err());
        assert!(cache_entries(&repo)?.is_empty());

        commit(&repo, &[("file", pointer.to_string())])?;
        assert!(fetch(&ctx, MemoryClient::new(ctx.clone()), &repo).is_err());
        assert!(cache_entries(&repo)?.is_empty());
        assert_eq!(fetch(&ctx, client, &repo)?.files, 1);
        assert_eq!(cache_entries(&repo)?, [pointer.hash.hex()]);
        Ok(())
    }
}
