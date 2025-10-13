use crate::HubClient;
use cas_client::{Api, ResponseErrorLogger};
use cas_types::HexMerkleHash;
use regex::{bytes, Regex};
use reqwest::Response;
use serde::Deserialize;
use std::sync::LazyLock;

#[async_trait::async_trait]
pub trait HubRepositoryTrait {
    async fn list_files(&self, path: &str) -> crate::Result<Vec<TreeEntry>>;
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")] // tells Serde to use the "type" field to decide which variant to use
pub enum TreeEntry {
    #[serde(rename = "file")]
    File(FileEntry),

    #[serde(rename = "directory")]
    Directory(DirectoryEntry),
}

impl TreeEntry {
    pub fn is_file(&self) -> bool {
        matches!(self, TreeEntry::File(_))
    }

    pub fn is_directory(&self) -> bool {
        matches!(self, TreeEntry::Directory(_))
    }

    pub fn as_file(&self) -> Option<&FileEntry> {
        if let TreeEntry::File(file) = self {
            Some(file)
        } else {
            None
        }
    }

    pub fn as_directory(&self) -> Option<&DirectoryEntry> {
        if let TreeEntry::Directory(dir) = self {
            Some(dir)
        } else {
            None
        }
    }

    pub fn path(&self) -> &str {
        match self {
            TreeEntry::File(file) => file.path.as_str(),
            TreeEntry::Directory(dir) => dir.path.as_str(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileEntry {
    pub oid: String,
    pub size: u64,
    pub lfs: Option<LfsInfo>,
    pub xet_hash: Option<HexMerkleHash>,
    pub path: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LfsInfo {
    pub oid: String,
    pub size: u64,
    pub pointer_size: u64,
}

#[derive(Debug, Deserialize)]
pub struct DirectoryEntry {
    pub oid: String,
    pub size: u64,
    pub path: String,
}

/// Extracts the URL from a Link header of the form:
/// `<https://example.com/...>; rel="next"`
pub fn parse_link_url(response: &Response) -> Option<String> {
    let header = response.headers().get("link")?.to_str().ok()?;
    // Compile the regex once (you could make it lazy_static if used often)
    let re = LazyLock::new(|| Regex::new(r#"<([^>]+)>;\s*rel="next""#).unwrap());
    re.captures(header).and_then(|caps| caps.get(1)).map(|m| m.as_str().to_string())
}

#[async_trait::async_trait]
impl HubRepositoryTrait for HubClient {
    async fn list_files(&self, path: &str) -> crate::Result<Vec<TreeEntry>> {
        let endpoint = self.endpoint.as_str();
        let repo_type = self.repo_info.repo_type.as_str();
        let repo_id = self.repo_info.full_name.as_str();
        let rev = self.reference.as_deref().unwrap_or("main");
        // if a path is empty (root dir), should not have a slash at the beginning of the path component
        // otherwise ensure there is a slash at the beginning of the path component
        let path = if path.is_empty() || path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{}", path)
        };
        let url = format!("{endpoint}/api/{repo_type}s/{repo_id}/tree/{rev}{path}?limit=1000");

        let response = self
            .client
            .get(url)
            .with_extension(Api("tree"))
            .send()
            .await
            .process_error("list-files")?;

        let mut link = parse_link_url(&response);
        let mut entries: Vec<TreeEntry> = response.json().await?;

        while let Some(page_url) = link.take() {
            let response = self
                .client
                .get(page_url.as_str())
                .with_extension(Api("tree"))
                .send()
                .await
                .process_error("list-files-pagination")?;

            link = parse_link_url(&response);
            let page_entries: Vec<TreeEntry> = response.json().await?;
            entries.extend(page_entries);
        }

        Ok(entries)
    }
}
