use std::fmt::Display;
use std::str::FromStr;

use serde::Deserialize;

use crate::errors::{HubClientError, Result};

/// This defines the response format from the Huggingface Hub Xet CAS access token API.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CasJWTInfo {
    pub cas_url: String, // CAS server endpoint base URL
    pub exp: u64,        // access token expiry since UNIX_EPOCH
    pub access_token: String,
}

// This defines the exact three types of repos served on HF Hub.
#[derive(Debug, PartialEq)]
pub enum HFRepoType {
    Model,
    Dataset,
    Space,
}

impl FromStr for HFRepoType {
    type Err = HubClientError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "" => Ok(HFRepoType::Model), // when repo type is omitted from the URL the default type is "model"
            "model" | "models" => Ok(HFRepoType::Model),
            "dataset" | "datasets" => Ok(HFRepoType::Dataset),
            "space" | "spaces" => Ok(HFRepoType::Space),
            t => Err(HubClientError::InvalidRepoType(t.to_owned())),
        }
    }
}

impl HFRepoType {
    pub fn as_str(&self) -> &str {
        match self {
            HFRepoType::Model => "model",
            HFRepoType::Dataset => "dataset",
            HFRepoType::Space => "space",
        }
    }
}

impl Display for HFRepoType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, PartialEq)]
pub struct RepoInfo {
    // The type of a repo, one of "model | dataset | space"
    pub repo_type: HFRepoType,
    // The full name of a repo, formatted as "owner/name"
    pub full_name: String,
}

impl RepoInfo {
    pub fn try_from(repo_type: &str, repo_id: &str) -> Result<Self> {
        Ok(Self {
            repo_type: repo_type.parse()?,
            full_name: repo_id.into(),
        })
    }
}

impl Display for RepoInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.repo_type, self.full_name)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::types::CasJWTInfo;

    #[test]
    fn test_cas_jwt_response_deser() -> Result<()> {
        let bytes = r#"{"casUrl":"https://cas-server.xethub.hf.co","exp":1756489133,"accessToken":"ey...jQ"}"#;

        let info: CasJWTInfo = serde_json::from_slice(bytes.as_bytes())?;

        assert_eq!(info.cas_url, "https://cas-server.xethub.hf.co");
        assert_eq!(info.exp, 1756489133);
        assert_eq!(info.access_token, "ey...jQ");

        Ok(())
    }
}
