use git_extension_common::{GitRepo, Interaction};
use xet_client::hub_client::Operation;
use xet_data::processing::FileDownloadSession;
use xet_runtime::core::XetContext;
use xet_runtime::utils::UniqueId;

use crate::auth::resolve_credential;
use crate::fetch::{fetch_files, pointers_at};
use crate::file_cache::FileCache;
use crate::{GitHfError, Result, cas_token};

/// Downloads the Xet files referenced by `revision` that are not yet cached. Contacts the Hub only
/// when some file is missing.
pub(super) fn run(remote: Option<&str>, revision: &str) -> Result<()> {
    let wd = std::env::current_dir()?;
    let repo = GitRepo::open(&wd)?;
    let cache = FileCache::new(&repo.common_dir()?)?;
    let pointers = pointers_at(&wd, revision)?;
    let total = pointers.len();
    let missing: Vec<_> = pointers.into_iter().filter(|pointer| !cache.contains(pointer)).collect();
    if missing.is_empty() {
        eprintln!("git-hf: all {total} Xet files in {revision} are cached");
        return Ok(());
    }

    let url = match remote {
        Some(remote) => repo.remote_name_to_url(remote)?,
        None => repo.remote_url()?,
    };
    let credential = resolve_credential(&repo, &url, Operation::Download, Interaction::Allowed)?;
    let ctx = XetContext::default()?;
    let session_id = UniqueId::new().to_string();
    let summary = ctx.runtime.handle().block_on(async {
        // Read tokens are repository-wide, so the Hub's default revision suffices.
        let cas = cas_token::connect(&ctx, &url, None, Operation::Download, credential.helper(), &session_id)
            .await
            .map_err(|error| match credential.describe_failure(&error, &url, Operation::Download) {
                Some(message) => GitHfError::Auth(message),
                None => GitHfError::Client(error),
            })?;
        let session = FileDownloadSession::from_client(&ctx, cas.client, None);
        fetch_files(&ctx, session, &cache, missing).await
    })?;
    eprintln!(
        "git-hf: fetched {} Xet files ({} bytes); {} were already cached",
        summary.files,
        summary.bytes,
        total - summary.files
    );
    Ok(())
}
