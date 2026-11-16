use std::ffi::OsStr;
use std::io::Read;

use git_extension_common::{GitRepo, GitUrl, Interaction};
use xet_client::hub_client::Operation;
use xet_data::processing::configurations::{SessionContext, TranslatorConfig};
use xet_runtime::core::XetContext;
use xet_runtime::utils::UniqueId;

use crate::auth::resolve_credential;
use crate::constants::GIT_HF_PROGRAM;
use crate::push::{parse_updates, plan_push, release_uploaded, token_reference, upload};
use crate::{GitHfError, Result, cas_token};

/// Uploads the staged data newly referenced by the pushed refs and removes it from staging.
/// Everything before the upload is local, so pushes that reference no staged data never contact
/// the Hub.
pub(super) fn run(_remote: &str, location: &OsStr) -> Result<()> {
    let mut input = Vec::new();
    std::io::stdin().read_to_end(&mut input)?;
    let wd = std::env::current_dir()?;

    let updates = parse_updates(&input)?;
    let repo = GitRepo::open(&wd)?;
    let staging = repo.common_dir()?.join("xet").join("staging");
    // Nothing to upload: allow the push to any remote, including non-Hub ones such as a mirror.
    let Some(plan) = plan_push(&wd, &staging, &updates)? else {
        return Ok(());
    };

    // The location may embed a token, so messages show it only redacted.
    let invalid_url = |location: &str, detail: Option<String>| {
        GitHfError::InvalidGitUrl(format!(
            "{location} is not a valid repository URL{}",
            detail.map(|detail| format!(" ({detail})")).unwrap_or_default()
        ))
    };
    let url: GitUrl = location
        .to_str()
        .and_then(|location| location.parse().ok())
        .ok_or_else(|| invalid_url("the push location", None))?;
    url.repo_info().map_err(|e| invalid_url(&url.redacted(), Some(e.to_string())))?;

    // Prompting is acceptable here: push is interactive, unlike clean.
    let credential = resolve_credential(&repo, &url, Operation::Upload, Interaction::Allowed)?;
    let ctx = XetContext::default()?;
    let session_id = UniqueId::new().to_string();
    let reference = token_reference(&updates);
    let runtime = ctx.runtime.clone();
    let (summary, plan, staging) = runtime.bridge_sync(async move {
        let cas = cas_token::connect(&ctx, &url, reference, Operation::Upload, credential.helper(), &session_id)
            .await
            .map_err(|error| match credential.describe_failure(&error, &url, Operation::Upload) {
                Some(message) => GitHfError::Auth(message),
                None => GitHfError::Client(error),
            })?;
        let cache = TranslatorConfig::new(
            &ctx,
            SessionContext {
                endpoint: cas.endpoint,
                auth: cas.auth,
                custom_headers: None,
                repo_paths: vec![],
                session_id: Some(session_id.clone()),
                cache_root: None,
            },
        )?
        .shard_cache_directory;
        let summary = upload(cas.client, &ctx.config.data.default_prefix, &staging, &plan, &cache).await?;
        Ok::<_, GitHfError>((summary, plan, staging))
    })??;
    release_uploaded(&staging, &plan)?;
    eprintln!(
        "{GIT_HF_PROGRAM}: uploaded {} xorbs ({} bytes) and registered {} files",
        summary.xorbs, summary.xorb_bytes, summary.files
    );
    Ok(())
}
