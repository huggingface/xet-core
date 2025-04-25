use std::sync::{Arc, Mutex};

use deduplication::{DataAggregator, DeduplicationMetrics};
use merklehash::MerkleHash;

use super::configurations::TranslatorConfig;
use super::errors::*;

/// Manages the translation of files between the
/// MerkleDB / pointer file format and the materialized version.
///
/// This class handles the clean operations.  It's meant to be a single atomic session
/// that succeeds or fails as a unit;  i.e. all files get uploaded on finalization, and all shards
/// and xorbs needed to reconstruct those files are properly uploaded and registered.
pub struct FileUploadSession {
    /// The configuration settings, if needed.
    pub(crate) config: Arc<TranslatorConfig>,

    /// Deduplicated data shared across files.
    current_session_data: Mutex<DataAggregator>,
}

impl FileUploadSession {
    pub fn new(config: Arc<TranslatorConfig>) -> Self {
        todo!()
    }

    pub(crate) async fn register_single_file_clean_completion(
        self: &Arc<Self>,
        mut file_data: DataAggregator,
        dedup_metrics: &DeduplicationMetrics,
        xorbs_dependencies: Vec<MerkleHash>,
    ) -> Result<()> {
        todo!()
    }
}
