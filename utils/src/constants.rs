// This file holds constants used by the server and client.
pub const AUTHORIZATION_HEADER: &str = "authorization";
pub const USER_ID_HEADER: &str = "xet-user-id";
pub const AUTH_HEADER: &str = "xet-auth";
pub const REPO_PATHS_HEADER: &str = "xet-repo-paths-bin";
pub const REQUEST_ID_HEADER: &str = "xet-request-id";
pub const GIT_XET_VERSION_HEADER: &str = "xet-version";
pub const TRACE_ID_HEADER: &str = "uber-trace-id";
pub const CONTENT_TYPE_HEADER: &str = "Content-Type";
pub const JSON_CONTENT_TYPE: &str = "application/json";
pub const CAS_PROTOCOL_VERSION_HEADER: &str = "xet-cas-protocol-version";
pub const CLIENT_IP_HEADER: &str = "x-forwarded-for";
pub const UNKNOWN_IP: &str = "0.0.0.0";
pub const DEFAULT_USER: &str = "anonymous";
pub const DEFAULT_AUTH: &str = "unknown";
pub const DEFAULT_VERSION: &str = "0.0.0";
pub const GRPC_TIMEOUT_SEC: u64 = 60;
