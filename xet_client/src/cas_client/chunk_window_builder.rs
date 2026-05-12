//! Server-side state machine for `GET /v2/file-chunk-hashes/{file_id}` (mirrored from
//! xetcas PR #987) plus a small driver helper used by simulation clients to produce a
//! [`FileChunkHashesResponse`] without routing through HTTP.

use xet_core_structures::merklehash::{MerkleHash, MerkleHashSubtree};
use xet_core_structures::metadata_shard::file_structs::MDBFileInfo;

use crate::cas_types::{ChunkWindow, FileChunkHashesResponse, FileRange};
use crate::error::{ClientError, Result};

pub struct ChunkWindowBuilder<'a> {
    dirty_ranges: &'a [FileRange],
    dirty_idx: usize,
    in_dirty_zone: bool,
    gap_is_first: bool,
    /// End-byte cursor: the start of the next chunk fed to the builder.
    cursor: u64,
    gap_chunks: Vec<(MerkleHash, u64)>,
    windows: Vec<FileRange>,
    hash_ranges: Vec<Option<MerkleHashSubtree>>,
}

impl<'a> ChunkWindowBuilder<'a> {
    pub fn new(dirty_ranges: &'a [FileRange]) -> Self {
        debug_assert!(
            dirty_ranges.windows(2).all(|w| w[0].end <= w[1].start),
            "dirty_ranges must be sorted and non-overlapping"
        );
        Self {
            dirty_ranges,
            dirty_idx: 0,
            in_dirty_zone: false,
            gap_is_first: true,
            cursor: 0,
            gap_chunks: Vec::new(),
            windows: Vec::with_capacity(dirty_ranges.len()),
            hash_ranges: Vec::new(),
        }
    }

    pub fn process_chunk(&mut self, hash: MerkleHash, size: u64, byte_end: u64) {
        let byte_start = self.cursor;
        let overlaps_dirty = self.overlaps_current_dirty(byte_start, byte_end);

        if !self.in_dirty_zone {
            if overlaps_dirty {
                self.open_window(byte_end);
                self.in_dirty_zone = true;
            } else {
                self.gap_chunks.push((hash, size));
            }
        } else if overlaps_dirty {
            self.windows
                .last_mut()
                .expect("in_dirty_zone implies a window has been opened")
                .end = byte_end;
            self.merge_ahead(byte_end);
        } else {
            self.dirty_idx += 1;
            self.gap_is_first = false;

            // The first clean chunk after a dirty zone may itself overlap the next
            // dirty range (back-to-back dirty ranges on adjacent chunks).
            let overlaps_next = self.overlaps_current_dirty(byte_start, byte_end);
            if overlaps_next {
                self.open_window(byte_end);
            } else {
                self.in_dirty_zone = false;
                self.gap_chunks.push((hash, size));
            }
        }
        self.cursor = byte_end;
    }

    /// Returns true when the entry ending at `byte_end` (and starting at the cursor)
    /// is fully contained within the current dirty range.
    pub fn entry_fully_dirty(&self, byte_end: u64) -> bool {
        self.dirty_idx < self.dirty_ranges.len()
            && self.dirty_ranges[self.dirty_idx].start <= self.cursor
            && byte_end <= self.dirty_ranges[self.dirty_idx].end
    }

    /// Process a fully-dirty shard entry without iterating its individual chunks.
    pub fn skip_dirty_entry(&mut self, byte_end: u64) {
        if !self.in_dirty_zone {
            self.open_window(byte_end);
            self.in_dirty_zone = true;
        } else {
            self.windows
                .last_mut()
                .expect("in_dirty_zone implies a window has been opened")
                .end = byte_end;
            self.merge_ahead(byte_end);
        }
        self.cursor = byte_end;
    }

    /// Consume the builder and return the dirty windows + N+1 gap hash ranges.
    pub fn finish(mut self) -> (Vec<FileRange>, Vec<Option<MerkleHashSubtree>>) {
        let trailing = MerkleHashSubtree::from_chunks(self.gap_is_first, &self.gap_chunks, true);
        self.hash_ranges.push(Self::to_option(trailing));
        (self.windows, self.hash_ranges)
    }

    fn open_window(&mut self, byte_end: u64) {
        let gap = MerkleHashSubtree::from_chunks(self.gap_is_first, &self.gap_chunks, false);
        self.hash_ranges.push(Self::to_option(gap));
        self.gap_chunks.clear();
        self.windows.push(FileRange::new(self.cursor, byte_end));
        self.merge_ahead(byte_end);
    }

    fn overlaps_current_dirty(&self, byte_start: u64, byte_end: u64) -> bool {
        self.dirty_idx < self.dirty_ranges.len()
            && byte_end > self.dirty_ranges[self.dirty_idx].start
            && byte_start < self.dirty_ranges[self.dirty_idx].end
    }

    fn merge_ahead(&mut self, byte_end: u64) {
        while self.dirty_idx + 1 < self.dirty_ranges.len() && byte_end > self.dirty_ranges[self.dirty_idx + 1].start {
            self.dirty_idx += 1;
        }
    }

    fn to_option(hr: MerkleHashSubtree) -> Option<MerkleHashSubtree> {
        if hr.is_empty() { None } else { Some(hr) }
    }
}

/// Drive [`ChunkWindowBuilder`] over a flat list of `(chunk_hash, size)` pairs and
/// assemble the [`FileChunkHashesResponse`]. Simulation clients pre-collect the chunks for
/// the file (typically by walking segments + xorb metadata) and call this to answer
/// `Client::get_file_chunk_hashes` locally.
///
/// Also emits `gap_verification`: for each **stable** original segment (one that lies
/// entirely outside the dirty windows), the corresponding `FileVerificationEntry` from
/// `file_info.verification` is copied into `gap_verification` in segment order. The
/// composed shard built by `upload_ranges` uses these to reconstruct its own verification
/// section without needing per-chunk hashes for the stable segments. When `file_info`
/// has no verification entries (legacy / test files), `gap_verification` is empty.
pub fn build_file_chunk_hashes_response(
    file_info: &MDBFileInfo,
    dirty_ranges: Vec<FileRange>,
    chunks: impl IntoIterator<Item = (MerkleHash, u64)>,
) -> Result<FileChunkHashesResponse> {
    let file_size = file_info.file_size();
    let dirty_ranges: Vec<FileRange> = dirty_ranges
        .into_iter()
        .map(|r| FileRange::new(r.start, r.end.min(file_size)))
        .filter(|r| r.start < r.end && r.start < file_size)
        .collect();
    if dirty_ranges.is_empty() {
        return Err(ClientError::Other("no valid dirty ranges".into()));
    }

    let mut builder = ChunkWindowBuilder::new(&dirty_ranges);
    let mut cumulative_bytes: u64 = 0;
    let mut total_chunks: u64 = 0;
    for (hash, size) in chunks {
        cumulative_bytes += size;
        builder.process_chunk(hash, size, cumulative_bytes);
        total_chunks += 1;
    }

    let (windows, hash_ranges) = builder.finish();
    if windows.is_empty() {
        return Err(ClientError::Other("dirty ranges do not overlap any chunks".into()));
    }

    // Emit one range hash per stable segment (= no overlap with any window).
    // Segments and windows are both monotonic, so a two-pointer walk is O(S+W).
    let gap_verification = if file_info.verification.len() == file_info.segments.len() {
        let mut gv = Vec::new();
        let mut acc = 0u64;
        let mut wi = 0usize;
        for (idx, seg) in file_info.segments.iter().enumerate() {
            let seg_start = acc;
            let seg_end = acc + seg.unpacked_segment_bytes as u64;
            acc = seg_end;
            while wi < windows.len() && windows[wi].end <= seg_start {
                wi += 1;
            }
            let overlaps = wi < windows.len() && windows[wi].start < seg_end;
            if !overlaps {
                gv.push(crate::cas_types::HexMerkleHash::from(file_info.verification[idx].range_hash));
            }
        }
        gv
    } else {
        Vec::new()
    };

    Ok(FileChunkHashesResponse {
        total_chunks,
        file_size,
        windows: windows
            .into_iter()
            .map(|r| ChunkWindow {
                dirty_byte_range: [r.start, r.end],
            })
            .collect(),
        hash_ranges,
        gap_verification,
    })
}
