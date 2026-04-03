use std::sync::Arc;

use http::header::HeaderMap;
use xet_client::cas_client::auth::DirectRefreshRouteTokenRefresher;
use xet_client::common::http_client::build_http_client;
use xet_client::hub_client::Operation;
use xet_runtime::core::XetRuntime;

use crate::auth::get_credential;
use crate::errors::Result;
use crate::git_repo::GitRepo;
use crate::git_url::GitUrl;

/// Build a [`DirectRefreshRouteTokenRefresher`] for the git-xet path,
/// deriving credentials from the git repo's credential helper.
pub fn new_git_token_refresher(
    ctx: &XetRuntime,
    repo: &GitRepo,
    remote_url: Option<GitUrl>,
    refresh_route: &str,
    operation: Operation,
    session_id: &str,
    custom_headers: Option<Arc<HeaderMap>>,
) -> Result<DirectRefreshRouteTokenRefresher> {
    let remote_url = match remote_url {
        Some(r) => r,
        None => repo.remote_url()?,
    };
    let cred_helper = get_credential(repo, &remote_url, operation)?;
    let client = build_http_client(ctx, session_id, None, custom_headers)?;
    Ok(DirectRefreshRouteTokenRefresher::new(
        ctx.clone(),
        refresh_route.to_owned(),
        client,
        Some(cred_helper),
    ))
}
