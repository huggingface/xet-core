use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;

use crate::block::BlockRange;
use crate::CacheError;

/// A Reader provides a way to read blocks from some system.
#[async_trait]
pub trait BlockReader {
    /// Reads in data for the specified request.
    async fn get(&self, request: &BlockReadRequest) -> Result<Vec<u8>, CacheError>;
}

/// A BlockReadRequest encompasses a request for a particular block within a file.
#[derive(Debug, Clone)]
pub struct BlockReadRequest {
    /// Block range
    block_range: BlockRange,
    /// metadata of the file
    metadata: Arc<FileMetadata>,
}

impl BlockReadRequest {
    pub fn from_block_range(
        block_range: BlockRange,
        metadata: Arc<FileMetadata>,
    ) -> BlockReadRequest {
        BlockReadRequest {
            block_range,
            metadata,
        }
    }

    /// Starting offset within the block
    pub fn start_off(&self) -> u64 {
        self.block_range.start_off()
    }

    /// Ending offset within the block
    pub fn end_off(&self) -> u64 {
        self.block_range.end_off()
    }

    pub fn range(&self) -> Range<u64> {
        self.start_off()..self.end_off()
    }

    pub fn block_range(&self) -> &BlockRange {
        &self.block_range
    }

    pub fn metadata(&self) -> &Arc<FileMetadata> {
        &self.metadata
    }

    pub fn block_size(&self) -> u64 {
        self.block_range.block_size()
    }
}

impl Display for BlockReadRequest {
    /// Format string consists of the block being requested (<filename>.<block_index>)
    /// followed by the byte range within the block being requested
    /// (in Rust Range format, with the start being inclusive and the end being exclusive).
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ {}.{}:{}..{} }}",
            self.metadata.name,
            self.block_range.idx(),
            self.block_range.start_off(),
            self.block_range.end_off()
        )
    }
}

/// Relevant metadata for a file in the system.
#[derive(Debug, Clone)]
pub struct FileMetadata {
    name: String,
    size: Option<u64>,
    version: u128, // epoch_millis is a u128
}

impl FileMetadata {
    pub fn new(name: String, size: Option<u64>, version: u128) -> Self {
        FileMetadata {
            name,
            size,
            version,
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn size(&self) -> Option<u64> {
        self.size
    }
    pub fn version(&self) -> u128 {
        self.version
    }
}
