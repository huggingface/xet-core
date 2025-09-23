use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::json;

use super::errors::{GitLFSProtocolError, Result};

// This file defines the protocol that Git LFS uses to talk to
// custom transfer agents. This implementation follows the protocol specification
// at https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md.

// LFS object oid string length
const OID_LEN: usize = 64;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "event", rename_all = "lowercase")]
pub enum LFSProtocolRequestEvent {
    Init(InitRequest),
    Upload(TransferRequest),
    Download(TransferRequest),
    Terminate,
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
#[serde(tag = "event", rename_all = "lowercase")]
pub enum LFSProtocolResponseEvent {
    Progress(ProgressResponse),
    Complete(CompleteResponse),
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "operation", rename_all = "lowercase")]
pub enum InitRequest {
    Upload(InitRequestInner),
    Download(InitRequestInner),
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct InitRequestInner {
    pub remote: String,   // the Git remote, this can either be a remote name like "origin" or a remote URL
    pub concurrent: bool, // if git-lfs will split the transfer workload
    pub concurrenttransfers: Option<u32>, /* the number of custom tranfer agent processes that git-lfs will spawn
                           * Note that this reflects the value of "lfs.concurrenttransfers" in Git config,
                           * and is just an "FYI" to each agent process. git-lfs splits the transfer workload evenly
                           * between the agent processes.
                           * git-lfs doesn't implement any logic to configure this using a env var, but git does
                           * expose a mechanism to temporarily set any git config using the -c option.
                           * So in summary, users can do
                           * ```
                           * git -c lfs.concurrenttransfers=<n> push/pull/fetch
                           * ```
                           */
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct TransferRequest {
    pub oid: String, // oid of the LFS object
    pub size: u64,   // size of the LFS object
    pub path: Option<PathBuf>, /* only for "upload" event, the file which this agent should read the
                      * data from */
    pub action: GitBatchApiResponseAction, // the action copied from the response from the batch API
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProgressResponse {
    pub oid: String,           // oid of the LFS object
    pub bytes_so_far: u64,     // the total number of bytes transferred so far
    pub bytes_since_last: u64, // the number of bytes transferred since the last progress message
}

#[derive(Debug, Serialize)]
pub struct CompleteResponse {
    pub oid: String, // oid of the LFS object, same as the oid in the TransferEvent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ProtocolError>, // if there was an error in the transfer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>, // only for "download" event, the file which this agent wrote the data to
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct GitBatchApiResponseAction {
    pub href: String,
    pub header: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct ProtocolError {
    code: i32,
    message: String,
}

impl LFSProtocolRequestEvent {
    /// Beyond syntax check by JSON parsers, this function checks
    /// the logical correctness of requests.
    fn validate(&self) -> Result<()> {
        match self {
            LFSProtocolRequestEvent::Init(req) => {
                let inner = match req {
                    InitRequest::Upload(inner) => inner,
                    InitRequest::Download(inner) => inner,
                };
                if inner.remote.is_empty() {
                    return Err(GitLFSProtocolError::bad_argument("invalid remote"));
                }

                Ok(())
            },
            LFSProtocolRequestEvent::Upload(req) => {
                if req.oid.len() != OID_LEN {
                    return Err(GitLFSProtocolError::bad_argument("invalid oid"));
                }

                if req.size == 0 {
                    return Err(GitLFSProtocolError::bad_argument("invalid size"));
                }

                if req.path.is_none() {
                    return Err(GitLFSProtocolError::bad_syntax("file path not provided for upload request"));
                }

                if req.action.href.is_empty() {
                    return Err(GitLFSProtocolError::bad_argument("empty action.href in server response"));
                }

                Ok(())
            },
            LFSProtocolRequestEvent::Download(req) => {
                if req.oid.len() != OID_LEN {
                    return Err(GitLFSProtocolError::bad_argument("invalid oid"));
                }

                if req.size == 0 {
                    return Err(GitLFSProtocolError::bad_argument("invalid size"));
                }

                if req.path.is_some() {
                    return Err(GitLFSProtocolError::bad_syntax("file path provided for download request"));
                }

                if req.action.href.is_empty() {
                    return Err(GitLFSProtocolError::bad_argument("empty action.href in server response"));
                }

                Ok(())
            },
            LFSProtocolRequestEvent::Terminate => Ok(()),
        }
    }
}

impl FromStr for LFSProtocolRequestEvent {
    type Err = GitLFSProtocolError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let req: LFSProtocolRequestEvent = serde_json::from_str(s).map_err(GitLFSProtocolError::bad_syntax)?;
        req.validate()?;
        Ok(req)
    }
}

impl InitRequest {
    pub fn success(self) -> impl Serialize {
        json!({})
    }

    pub fn error(self, error: impl Display) -> impl Serialize {
        json!({"error":  ProtocolError::init_error(error.to_string())})
    }
}

impl TransferRequest {
    #[allow(unused)]
    pub fn progress(&self, bytes_so_far: u64, bytes_since_last: u64) -> impl Serialize {
        LFSProtocolResponseEvent::Progress(ProgressResponse {
            oid: self.oid.clone(),
            bytes_so_far,
            bytes_since_last,
        })
    }

    pub fn success(self, response_file_path: Option<PathBuf>) -> impl Serialize {
        LFSProtocolResponseEvent::Complete(CompleteResponse {
            oid: self.oid,
            error: None,
            path: response_file_path,
        })
    }

    pub fn error(self, error: impl Display) -> impl Serialize {
        LFSProtocolResponseEvent::Complete(CompleteResponse {
            oid: self.oid,
            error: Some(ProtocolError::transfer_error(error.to_string())),
            path: None,
        })
    }
}

// The below error codes are expected by git-lfs.
// See https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md
impl ProtocolError {
    fn init_error(message: String) -> Self {
        Self { code: 32, message }
    }

    fn transfer_error(message: String) -> Self {
        Self { code: 2, message }
    }
}

pub fn to_line_delimited_json_string(value: impl Serialize) -> Result<String> {
    Ok(format!("{}\n", serde_json::to_string(&value)?))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use anyhow::Result;

    use super::*;

    #[test]
    fn test_protocol_serde_unknown_event() -> Result<()> {
        let message = r#"
            { "event": "other", "operation": "upload", "remote": "origin", "concurrent": false }"#;
        let parsed: Result<LFSProtocolRequestEvent, GitLFSProtocolError> = message.parse();

        assert!(matches!(parsed, Err(GitLFSProtocolError::Syntax(_))));

        Ok(())
    }

    #[test]
    fn test_protocol_serde_init_req_good() -> Result<()> {
        // init event with upload operation
        let message1 = r#"
            { "event": "init", "operation": "upload", "remote": "origin", "concurrent": false }"#;
        let expected1 = LFSProtocolRequestEvent::Init(InitRequest::Upload(InitRequestInner {
            remote: "origin".to_owned(),
            concurrent: false,
            concurrenttransfers: None,
        }));
        let parsed1: LFSProtocolRequestEvent = message1.parse()?;

        assert_eq!(expected1, parsed1);

        // init event with download operation
        let message2 = r#"
            { "event": "init", "operation": "download", "remote": "deploy", "concurrent": true, "concurrenttransfers": 1 }"#;
        let expected2 = LFSProtocolRequestEvent::Init(InitRequest::Download(InitRequestInner {
            remote: "deploy".to_owned(),
            concurrent: true,
            concurrenttransfers: Some(1),
        }));
        let parsed2: LFSProtocolRequestEvent = message2.parse()?;

        assert_eq!(expected2, parsed2);

        Ok(())
    }

    #[test]
    fn test_protocol_serde_init_req_bad() -> Result<()> {
        // init event with invalid operation
        let message1 = r#"
            { "event": "init", "operation": "other", "remote": "origin", "concurrent": false }"#;
        let parsed1: Result<LFSProtocolRequestEvent, GitLFSProtocolError> = message1.parse();

        assert!(matches!(parsed1, Err(GitLFSProtocolError::Syntax(_))));

        // init event missing required field
        let message2 = r#"
            { "event": "init", "operation": "upload", "remote": "origin" }"#;
        let parsed2: Result<LFSProtocolRequestEvent, GitLFSProtocolError> = message2.parse();

        assert!(matches!(parsed2, Err(GitLFSProtocolError::Syntax(_))));

        // init event with invalid remote
        let message2 = r#"
            { "event": "init", "operation": "upload", "remote": "", "concurrent": false }"#;
        let parsed2: Result<LFSProtocolRequestEvent, GitLFSProtocolError> = message2.parse();

        assert!(matches!(parsed2, Err(GitLFSProtocolError::Argument(_))));

        Ok(())
    }

    #[test]
    fn test_protocol_serde_init_error_res() -> Result<()> {
        // If an error occurs during init, git-lfs expects an error message in json format with code 32.
        let init_req = InitRequest::Upload(InitRequestInner {
            remote: "origin".to_owned(),
            concurrent: false,
            concurrenttransfers: None,
        });
        let mut error = to_line_delimited_json_string(init_req.error("agent failed initiation"))?;
        let mut expected = r#"{ "error": { "code": 32, "message": "agent failed initiation" } }"#.to_owned();

        error.retain(|c| !c.is_whitespace());
        expected.retain(|c| !c.is_whitespace());

        assert_eq!(error, expected);

        Ok(())
    }

    #[test]
    fn test_protocol_serde_transfer_req_good() -> Result<()> {
        // upload transfer event
        let message1 = r#"
            { "event": "upload", "oid": "bf3e3e2af9366a3b704ae0c31de5afa64193ebabffde2091936ad2e7510bc03a", "size": 346232,
            "path": "/path/to/file.png", "action": { "href": "nfs://server/path", "header": { "key": "value" } } }"#;
        let expected1 = LFSProtocolRequestEvent::Upload(TransferRequest {
            oid: "bf3e3e2af9366a3b704ae0c31de5afa64193ebabffde2091936ad2e7510bc03a".to_owned(),
            size: 346232,
            path: Some(Path::new("/path/to/file.png").to_path_buf()),
            action: GitBatchApiResponseAction {
                href: "nfs://server/path".to_owned(),
                header: HashMap::from([("key".to_owned(), "value".to_owned())]),
            },
        });
        let parsed1: LFSProtocolRequestEvent = message1.parse()?;

        assert_eq!(expected1, parsed1);

        // download transfer event
        let message2 = r#"
            { "event": "download", "oid": "22ab5f63670800cc7be06dbed816012b0dc411e774754c7579467d2536a9cf3e", "size": 21245,
            "action": { "href": "https://server/path", "header": { "k1": "v1", "k2": "v2" } } }"#;
        let expected2 = LFSProtocolRequestEvent::Download(TransferRequest {
            oid: "22ab5f63670800cc7be06dbed816012b0dc411e774754c7579467d2536a9cf3e".to_owned(),
            size: 21245,
            path: None,
            action: GitBatchApiResponseAction {
                href: "https://server/path".to_owned(),
                header: HashMap::from([("k1".to_owned(), "v1".to_owned()), ("k2".to_owned(), "v2".to_owned())]),
            },
        });
        let parsed2: LFSProtocolRequestEvent = message2.parse()?;

        assert_eq!(expected2, parsed2);

        Ok(())
    }

    #[test]
    fn test_protocol_serde_transfer_req_bad() -> Result<()> {
        // transfer event missing required field
        let message1 = r#"
            { "event": "upload", "oid": "bf3e3e2af9366a3b704ae0c31de5afa64193ebabffde2091936ad2e7510bc03a", "size": 346232,
            "path": "/path/to/file.png" }"#;
        let parsed1: Result<LFSProtocolRequestEvent, GitLFSProtocolError> = message1.parse();

        assert!(matches!(parsed1, Err(GitLFSProtocolError::Syntax(_))));

        // transfer event with invalid oid
        let message2 = r#"
            { "event": "upload", "oid": "bf3e3e2af9366abc03a", "size": 346232,
            "path": "/path/to/file.png", "action": { "href": "nfs://server/path", "header": { "key": "value" } } }"#;
        let parsed2: Result<LFSProtocolRequestEvent, GitLFSProtocolError> = message2.parse();

        assert!(matches!(parsed2, Err(GitLFSProtocolError::Argument(_))));

        // transfer event with invalid size
        let message3 = r#"
            { "event": "upload", "oid": "bf3e3e2af9366a3b704ae0c31de5afa64193ebabffde2091936ad2e7510bc03a", "size": 0,
            "path": "/path/to/file.png", "action": { "href": "nfs://server/path", "header": { "key": "value" } } }"#;
        let parsed3: Result<LFSProtocolRequestEvent, GitLFSProtocolError> = message3.parse();

        assert!(matches!(parsed3, Err(GitLFSProtocolError::Argument(_))));

        // upload transfer event missing path
        let message4 = r#"
            { "event": "upload", "oid": "bf3e3e2af9366a3b704ae0c31de5afa64193ebabffde2091936ad2e7510bc03a", "size": 346232,
            "action": { "href": "nfs://server/path", "header": { "key": "value" } } }"#;
        let parsed4: Result<LFSProtocolRequestEvent, GitLFSProtocolError> = message4.parse();

        assert!(matches!(parsed4, Err(GitLFSProtocolError::Syntax(_))));

        // download transfer event with path
        let message5 = r#"
            { "event": "download", "oid": "bf3e3e2af9366a3b704ae0c31de5afa64193ebabffde2091936ad2e7510bc03a", "size": 12514,
            "path": "/path/to/file.png", "action": { "href": "https://server/path", "header": { "k1": "v1", "k2": "v2" } } }"#;
        let parsed5: Result<LFSProtocolRequestEvent, GitLFSProtocolError> = message5.parse();

        assert!(matches!(parsed5, Err(GitLFSProtocolError::Syntax(_))));

        Ok(())
    }

    #[test]
    fn test_protocol_serde_transfer_error_res() -> Result<()> {
        // If an error occurs during transfer, git-lfs expects a complete message in json format
        // with error code 2 and an error message.
        let transfer_req = TransferRequest {
            oid: "bf3e3e2af9366a3b704ae0c31de5afa64193ebabffde2091936ad2e7510bc03a".to_owned(),
            size: 346232,
            path: Some(Path::new("/path/to/file.png").to_path_buf()),
            action: GitBatchApiResponseAction {
                href: "nfs://server/path".to_owned(),
                header: HashMap::from([("key".to_owned(), "value".to_owned())]),
            },
        };
        let mut error = to_line_delimited_json_string(transfer_req.error("agent failed upload"))?;
        let mut expected = r#"
            { "event": "complete", "oid": "bf3e3e2af9366a3b704ae0c31de5afa64193ebabffde2091936ad2e7510bc03a",
            "error": { "code": 2, "message": "agent failed upload" } }"#
            .to_owned();

        error.retain(|c| !c.is_whitespace());
        expected.retain(|c| !c.is_whitespace());

        assert_eq!(error, expected);

        Ok(())
    }

    #[test]
    fn test_protocol_serde_terminate_req_good() -> Result<()> {
        let message = r#"{ "event": "terminate" }"#;
        let expected = LFSProtocolRequestEvent::Terminate;
        let parsed: LFSProtocolRequestEvent = message.parse()?;

        assert_eq!(expected, parsed);

        Ok(())
    }

    #[test]
    fn test_protocol_serde_progress_res() -> Result<()> {
        let progress_res = LFSProtocolResponseEvent::Progress(ProgressResponse {
            oid: "22ab5f63670800cc7be06dbed816012b0dc411e774754c7579467d2536a9cf3e".to_owned(),
            bytes_so_far: 1234,
            bytes_since_last: 64,
        });
        let mut response = to_line_delimited_json_string(progress_res)?;
        let mut expected = r#"
            { "event": "progress", "oid": "22ab5f63670800cc7be06dbed816012b0dc411e774754c7579467d2536a9cf3e",
            "bytesSoFar": 1234, "bytesSinceLast": 64 }"#
            .to_owned();

        response.retain(|c| !c.is_whitespace());
        expected.retain(|c| !c.is_whitespace());

        assert_eq!(response, expected);

        Ok(())
    }

    #[test]
    fn test_protocol_serde_complete_res() -> Result<()> {
        // upload complete event
        let upload_req = TransferRequest {
            oid: "bf3e3e2af9366a3b704ae0c31de5afa64193ebabffde2091936ad2e7510bc03a".to_owned(),
            size: 346232,
            path: Some(Path::new("/path/to/file.png").to_path_buf()),
            action: GitBatchApiResponseAction {
                href: "nfs://server/path".to_owned(),
                header: HashMap::from([("key".to_owned(), "value".to_owned())]),
            },
        };
        let mut response = to_line_delimited_json_string(upload_req.success(None))?;
        let mut expected =
            r#"{ "event": "complete", "oid": "bf3e3e2af9366a3b704ae0c31de5afa64193ebabffde2091936ad2e7510bc03a" }"#
                .to_owned();

        response.retain(|c| !c.is_whitespace());
        expected.retain(|c| !c.is_whitespace());

        assert_eq!(response, expected);

        // download complete event
        let download_req = TransferRequest {
            oid: "22ab5f63670800cc7be06dbed816012b0dc411e774754c7579467d2536a9cf3e".to_owned(),
            size: 21245,
            path: None,
            action: GitBatchApiResponseAction {
                href: "https://server/path".to_owned(),
                header: HashMap::from([("k1".to_owned(), "v1".to_owned()), ("k2".to_owned(), "v2".to_owned())]),
            },
        };
        let mut response =
            to_line_delimited_json_string(download_req.success(Some(Path::new("/path/to/file.png").to_path_buf())))?;
        let mut expected = r#"
            { "event": "complete", "oid": "22ab5f63670800cc7be06dbed816012b0dc411e774754c7579467d2536a9cf3e", "path": "/path/to/file.png" }"#.to_owned();

        response.retain(|c| !c.is_whitespace());
        expected.retain(|c| !c.is_whitespace());

        assert_eq!(response, expected);

        Ok(())
    }
}
