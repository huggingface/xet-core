use serde::{Deserialize, Serialize};
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Action {
    pub action: String,
    pub file_path: String,
    #[serde(default)]
    pub previous_path: String,
    #[serde(default)]
    pub execute_filemode: bool,
    #[serde(default)]
    pub content: String,
}

/// JSON descriptions of the manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JSONCommand {
    pub author_name: String,
    pub author_email: String,
    pub branch: String,
    pub commit_message: String,
    #[serde(default)]
    pub create_ref: bool,
    pub actions: Vec<Action>,
}
