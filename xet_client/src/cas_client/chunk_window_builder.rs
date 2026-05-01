//! State machine that classifies chunks into dirty windows and gap subtrees.
//!
//! Mirrors the server-side state machine used by `GET /v2/file-chunk-hashes/{file_id}`
//! (xetcas PR #987). We need it on the client too so that the simulation clients
//! (`MemoryClient`, `LocalClient`) can produce a [`FileChunkHashesResponse`] without
//! routing through HTTP.
//!
//! The result is `windows.len()` chunk-aligned dirty `FileRange`s and `windows.len() + 1`
//! gap subtrees (`None` for empty gaps). The producer feeds chunks in file order;
//! `finish()` returns both vectors.
//!
//! Memory note: `gap_chunks` accumulates every chunk in the current gap before being
//! rolled up into a [`MerkleHashSubtree`]. Peak memory scales with the largest contiguous
//! gap.

use xet_core_structures::merklehash::{MerkleHash, MerkleHashSubtree};

use crate::cas_types::FileRange;

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
        if hr.num_nodes() > 0 { Some(hr) } else { None }
    }
}
