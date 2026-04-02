use std::marker::PhantomData;

use http::HeaderMap;

use crate::xet_session::XetSession;

/// Per-commit/group auth and connection configuration passed to
/// [`create_translator_config`](super::common::create_translator_config) on build.
#[derive(Default)]
pub(super) struct AuthOptions {
    pub(super) endpoint: Option<String>,
    pub(super) custom_headers: Option<HeaderMap>,
    pub(super) token_info: Option<(String, u64)>,
    pub(super) token_refresh: Option<(String, HeaderMap)>,
}

/// Generic builder for session-scoped operation groups.
///
/// `G` is the product type — [`XetUploadCommit`](super::upload_commit::XetUploadCommit),
/// [`XetFileDownloadGroup`](super::file_download_group::XetFileDownloadGroup), or
/// [`XetDownloadStreamGroup`](super::download_stream_group::XetDownloadStreamGroup).
///
/// The shared configuration methods (`with_endpoint`, `with_custom_headers`,
/// `with_token_info`, `with_token_refresh_url`) are implemented once here on
/// `AuthGroupBuilder<G>`. The `build` and `build_blocking` methods are implemented
/// separately per `G`.
///
/// Obtain via [`XetSession::new_upload_commit`](super::session::XetSession::new_upload_commit),
/// [`XetSession::new_file_download_group`](super::session::XetSession::new_file_download_group), or
/// [`XetSession::new_download_stream_group`](super::session::XetSession::new_download_stream_group).
pub struct AuthGroupBuilder<G> {
    pub(super) session: XetSession,
    pub(super) auth_options: AuthOptions,
    _marker: PhantomData<G>,
}

impl<G> AuthGroupBuilder<G> {
    pub(super) fn new(session: XetSession) -> Self {
        Self {
            session,
            auth_options: Default::default(),
            _marker: PhantomData,
        }
    }

    /// Set the Xet CAS server endpoint URL (e.g. `"https://cas.example.com"`). If this is not provided but a token refresh URL is provided,
    /// on build a request will be sent to the token refresh URL to fetch the CAS server endpoint.
    pub fn with_endpoint(self, endpoint: impl Into<String>) -> Self {
        Self {
            auth_options: AuthOptions {
                endpoint: Some(endpoint.into()),
                ..self.auth_options
            },
            ..self
        }
    }

    /// Attach custom HTTP headers that are forwarded with every CAS request.
    pub fn with_custom_headers(self, headers: HeaderMap) -> Self {
        Self {
            auth_options: AuthOptions {
                custom_headers: Some(headers),
                ..self.auth_options
            },
            ..self
        }
    }

    /// Seed an initial CAS access token and its expiry as a Unix timestamp (seconds).
    ///
    /// If endpoint is not provided but a token refresh URL is provided, this token info will be replaced by the one
    /// retrieved from the first token refresh request.
    pub fn with_token_info(self, token: impl Into<String>, expiry: u64) -> Self {
        Self {
            auth_options: AuthOptions {
                token_info: Some((token.into(), expiry)),
                ..self.auth_options
            },
            ..self
        }
    }

    /// Set a URL and authentication headers used to obtain a fresh CAS access token
    /// whenever the current one is about to expire.
    ///
    /// The client issues an authenticated HTTP GET to `url` with `headers` (which should
    /// include auth credentials, e.g. `Authorization: Bearer <hub-token>`).  The endpoint
    /// must return JSON:
    /// `{ "accessToken": "<string>", "exp": <unix_secs>, "casUrl": "<string>" }`.
    pub fn with_token_refresh_url(self, url: impl Into<String>, headers: HeaderMap) -> Self {
        Self {
            auth_options: AuthOptions {
                token_refresh: Some((url.into(), headers)),
                ..self.auth_options
            },
            ..self
        }
    }
}
