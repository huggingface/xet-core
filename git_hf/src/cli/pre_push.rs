use std::ffi::OsStr;
use std::io::Read;

use git_extension_common::{GitRepo, GitUrl, Interaction};
use xet_client::hub_client::Operation;
use xet_data::processing::configurations::{SessionContext, TranslatorConfig};
use xet_runtime::core::XetContext;
use xet_runtime::utils::UniqueId;

use crate::auth::resolve_credential;
use crate::push::{
    StagedIndex, UploadPlan, check_failure_markers, parse_updates, pushed_pointers, release_uploaded, token_reference,
    upload,
};
use crate::{GitHfError, Result, cas_token, hook};

/// Runs a chained hook, then uploads the staged data newly referenced by the pushed refs and removes
/// it from staging. Everything before the upload is local, so pushes that reference no staged data
/// never contact the Hub.
pub(super) fn run(remote: &str, location: &OsStr) -> Result<()> {
    let mut input = Vec::new();
    std::io::stdin().read_to_end(&mut input)?;
    let wd = std::env::current_dir()?;
    hook::run_chained(&wd, &[OsStr::new(remote), location], &input)?;

    let updates = parse_updates(&input)?;
    let repo = GitRepo::open(&wd)?;
    let staging = repo.common_dir()?.join("xet").join("staging");
    let index = StagedIndex::load(&staging)?;
    check_failure_markers(&staging, &index)?;
    let pointers = pushed_pointers(&wd, &updates)?;
    // Pushes without staged data are not tied to a Hub destination (e.g. a mirror).
    if !pointers.iter().any(|hash| index.contains_file(hash)) {
        return Ok(());
    }

    // The location may embed a token, so messages show it only redacted.
    let not_hub = |location: &str, detail: Option<String>| {
        GitHfError::NotSupported(format!(
            "this push references staged Xet data, which can only be uploaded to a Hugging Face Hub repository; \
             {location} is not one{}",
            detail.map(|detail| format!(" ({detail})")).unwrap_or_default()
        ))
    };
    let url: GitUrl = location
        .to_str()
        .and_then(|location| location.parse().ok())
        .ok_or_else(|| not_hub("the push location", None))?;
    url.repo_info().map_err(|e| not_hub(&url.redacted(), Some(e.to_string())))?;
    let plan = UploadPlan::new(&staging, &index, &pointers)?;

    // Prompting is acceptable here: push is interactive, unlike clean.
    let credential = resolve_credential(&repo, &url, Operation::Upload, Interaction::Allowed)?;
    let ctx = XetContext::default()?;
    let session_id = UniqueId::new().to_string();
    let summary = ctx.runtime.handle().block_on(async {
        let cas = cas_token::connect(
            &ctx,
            &url,
            token_reference(&updates),
            Operation::Upload,
            credential.helper(),
            &session_id,
        )
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
        upload(cas.client, &ctx.config.data.default_prefix, &staging, &plan, &cache).await
    })?;
    release_uploaded(&staging, &plan)?;
    eprintln!(
        "git-hf: uploaded {} xorbs ({} bytes) and registered {} files",
        summary.xorbs, summary.xorb_bytes, summary.files
    );
    Ok(())
}
