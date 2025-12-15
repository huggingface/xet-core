// Data Retrieval Options.
// - URL to be downloaded in the future.
// - Data downloaded and held in memory..
// - Data to be loaded from cache.
// - Data to be loaded from existing file.
// - Data already present in the file (thus option bytes).

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use merklehash::MerkleHash;
use utils::rw_task_lock::RwTaskLock;

use crate::error::CasClientError;

enum DataWriteSource {
    FromBytes(Bytes),

    FromFile {
        file_path: PathBuf,
        offset: u64,
        length: u64,
    },

    AlreadyPresent {
        length: u64,
    },
}

struct DataTerm {
    term_index: usize,
    xorb_hash: MerkleHash,

    data: RwTaskLock<DataWriteSource, CasClientError>,
}
