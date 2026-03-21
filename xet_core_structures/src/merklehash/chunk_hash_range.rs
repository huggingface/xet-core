//! Compact representation of a range of chunk hashes with O(log n) storage.
//!
//! # Background: The Aggregated Hash Algorithm
//!
//! The existing [`aggregated_node_hash`] algorithm computes a file-level hash
//! from a sequence of chunk hashes `[(hash, size)]` by iteratively collapsing
//! groups of nodes into parent nodes.  Each pass scans left-to-right, using
//! [`next_merge_cut`] to decide where to split the sequence into groups:
//!
//! ```text
//! next_merge_cut(slice) → group_len:
//!     if len ≤ 2:            return len          (MIN_GROUP_SIZE = 2)
//!     for i in 2..MAX_GROUP_SIZE:                 (MAX_GROUP_SIZE = 9)
//!         if hash[i] % 4 == 0: return i + 1      (natural cut)
//!     return MAX_GROUP_SIZE                       (forced cut at max)
//! ```
//!
//! Each group is merged into a single parent node via [`merged_hash_of_sequence`],
//! which hashes the textual representation of `"hash : size\n"` for each child.
//! The pass is repeated on the resulting shorter sequence until one node remains.
//! With a mean branching factor of 4, this converges in O(log₄ n) passes.
//!
//! The key properties of `next_merge_cut` that this module exploits:
//!
//! - **Hash-dependent cuts**: a group boundary at position `i` is triggered when `hash[i] % 4 == 0` ("natural cut") --
//!   determined solely by that node's hash, independent of neighbors.
//! - **Bounded groups**: every group has at least `MIN_GROUP_SIZE=2` and at most `MAX_GROUP_SIZE=9` nodes.
//! - **Forward-only scanning**: `next_merge_cut` scans forward from the group start.  A group boundary depends only on
//!   nodes within the group (at most `MAX_GROUP_SIZE` nodes ahead of the start).
//!
//! # Problem
//!
//! `aggregated_node_hash` requires all chunks in memory at once -- O(n) storage.
//! For files with 100M+ chunks, this is impractical.  We want to:
//!
//! 1. Represent a sub-range of chunks compactly in O(log n) space.
//! 2. Merge adjacent sub-ranges without reconstructing the full list.
//! 3. Compute the final hash identically to `aggregated_node_hash` when all sub-ranges covering the file are merged.
//!
//! # Solution: The Hump Representation
//!
//! A `ChunkHashRange` stores the partially-aggregated state of a contiguous
//! range of chunks as a "hump" -- a structure that ascends in aggregation
//! level from left to right, peaks, then descends:
//!
//! ```text
//!   Level 3:            [  peak  ]
//!   Level 2:        [left] [peak] [right]
//!   Level 1:    [left  ]   [peak]   [  right]
//!   Level 0: [left     ]   [peak]   [     right]
//!
//! Flat storage: [L0_left, L1_left, L2_left, L3_peak, L2_right, L1_right, L0_right]
//! Levels:       [(l0, r0),  (l1, r1),  (l2, r2),  (l3, r3)]
//! ```
//!
//! At each level:
//! - **left** = unstable prefix: nodes before the first stable group boundary.
//! - **right** = unstable suffix: nodes after the last complete group.
//! - **promoted** nodes (the merged groups between the boundaries) move to the next level up, becoming the input for
//!   the next iteration.
//!
//! This is stored as a flat `Vec<Node>` with a `Vec<(left_count, right_count)>`
//! index.  Since both left and right at each level are bounded by O(1) nodes
//! (due to stable cut points), and there are O(log n) levels, total storage
//! is O(log n).
//!
//! ## Measured scaling (100M hashes streamed in batches of 1000):
//!
//! ```text
//!    total_chunks      nodes   levels   worst_nodes
//!            1000         40        4            92
//!           10000        115        5           202
//!          100000        323        5           323
//!         1000000        175        8           450
//!        10000000        318        9           567
//!       100000000        439       10           726
//! ```
//!
//! # Stable Cut Points
//!
//! The central algorithmic challenge: when a range doesn't start at position 0
//! (`at_start=false`) or doesn't end at the file boundary (`at_end=false`),
//! how do we know which groups are safe to merge?
//!
//! ## Forward stability (`find_stable_start`)
//!
//! A "stable start" is a position `m` in the sequence such that `m` is always
//! a group boundary regardless of what nodes precede it.  This lets us merge
//! everything from `m` onward without knowing the left context.
//!
//! **Algorithm**:  Scan left-to-right for positions where `is_natural_cut(hash)`
//! is true (ignoring the min/max group size bounds).  Call these "unbounded cuts".
//! When we find three consecutive unbounded cuts `c0, c1, c2` where both gaps
//! `c1-c0` and `c2-c1` are in the range `(MIN_GROUP_SIZE, MAX_GROUP_SIZE-1)`,
//! then `c1+1` is a stable group boundary.
//!
//! **Why this works**: `next_merge_cut` scans forward from a group start and
//! cuts at the first natural cut in `[MIN_GROUP_SIZE, MAX_GROUP_SIZE)`.  The
//! gap constraints ensure that `c1` cannot be absorbed into an earlier group
//! (the group starting before `c0` can reach at most `c0`, since `c0` is a
//! natural cut within the valid window).  The group starting after `c0` will
//! see `c1` within its valid window.  So `c1` is always the cut point, and
//! `c1+1` always starts a new group.
//!
//! ```text
//! Pseudocode:
//!     prev_prev_cut = None
//!     prev_cut = None
//!     for each position with is_natural_cut(hash):
//!         if prev_cut and prev_prev_cut:
//!             if valid_gap(pos - prev_cut) and valid_gap(prev_cut - prev_prev_cut):
//!                 return prev_cut + 1
//!         prev_prev_cut = prev_cut
//!         prev_cut = pos
//!     return None
//! ```
//!
//! ## Backward stability (`find_stable_end`)
//!
//! The mirror of `find_stable_start`: scans right-to-left for natural cuts
//! and finds three consecutive ones with valid gaps.  Returns `c1+1` as the
//! last position where group boundaries are stable regardless of what comes
//! after.
//!
//! ```text
//! Pseudocode:
//!     next_next_cut = None
//!     next_cut = None
//!     for each position with is_natural_cut(hash), scanning right-to-left:
//!         if next_cut and next_next_cut:
//!             if valid_gap(next_cut - pos) and valid_gap(next_next_cut - next_cut):
//!                 return next_cut + 1
//!         next_next_cut = next_cut
//!         next_cut = pos
//!     return None
//! ```
//!
//! # Building a Hump (`build_hump`)
//!
//! Given a slice of level-0 chunks and boundary flags, iteratively applies
//! `split_and_promote` to produce the hump:
//!
//! ```text
//! build_hump(chunks, at_start, at_end):
//!     current = chunks
//!     left_levels = []
//!     right_levels = []
//!
//!     loop:
//!         level_at_start = at_start AND all previous lefts are empty
//!         level_at_end   = at_end   AND all previous rights are empty
//!
//!         (prefix, promoted, suffix) = split_and_promote(current, level_at_start, level_at_end)
//!
//!         left_levels.push(prefix)
//!         right_levels.push(suffix)
//!
//!         if promoted is empty:   break
//!         if promoted.len() == 1: push to top level, break
//!         current = promoted
//!
//!     return flatten_hump(left_levels, right_levels)
//! ```
//!
//! # Split and Promote (`split_and_promote`)
//!
//! The core per-level operation.  Given a sequence of nodes at the same
//! aggregation level, partitions them into three parts:
//!
//! ```text
//! split_and_promote(nodes, at_start, at_end) → (prefix, promoted, suffix):
//!     if len ≤ 1: return (nodes, [], [])
//!
//!     // Find the mergeable region
//!     stable_start = 0              if at_start
//!                  = find_stable_start(nodes)  otherwise
//!     stable_end   = len            if at_end
//!                  = find_stable_end(nodes[stable_start..]) + stable_start  otherwise
//!
//!     if no stable region found: return (nodes, [], [])
//!
//!     prefix = nodes[..stable_start]
//!     suffix = nodes[stable_end..]
//!     mergeable = nodes[stable_start..stable_end]
//!
//!     // Merge groups within the stable region
//!     promoted = []
//!     pos = 0
//!     while pos < mergeable.len():
//!         cut_len = next_merge_cut(mergeable[pos..])
//!         promoted.push(merged_hash_of_sequence(mergeable[pos..pos+cut_len]))
//!         pos += cut_len
//!
//!     return (prefix, promoted, suffix)
//! ```
//!
//! # Merging Two Humps (`merge_two`)
//!
//! Given two adjacent `ChunkHashRange`s (left and right), produces a single
//! merged hump.  At each level, the full node sequence at that level from
//! both humps, plus any carry-up from the level below, is reassembled and
//! re-split:
//!
//! ```text
//! merge_two(left_range, right_range):
//!     carry = []
//!
//!     for level in 0..max(left.num_levels, right.num_levels):
//!         // Reassemble the full sequence at this level
//!         full = left.left_at(level)
//!              + left.right_at(level)
//!              + carry
//!              + right.left_at(level)
//!              + right.right_at(level)
//!
//!         // Determine boundary flags for this level
//!         level_at_start = combined_at_start AND all lower lefts empty
//!         level_at_end   = combined_at_end   AND all lower rights empty
//!
//!         (prefix, promoted, suffix) = split_and_promote(full, level_at_start, level_at_end)
//!
//!         new_left[level]  = prefix
//!         new_right[level] = suffix
//!         carry = promoted
//!
//!     // Handle remaining carry (may need additional levels)
//!     while carry.len() > 1:
//!         (prefix, promoted, suffix) = split_and_promote(carry, ...)
//!         push prefix/suffix as new level
//!         carry = promoted
//!     if carry.len() == 1:
//!         push as top level
//!
//!     return flatten_hump(new_left, new_right)
//! ```
//!
//! **Complexity**: The total number of nodes across all levels in each hump
//! is O(log n), so the merge processes O(log n) nodes total -- not O(n).
//!
//! # Computing the Final Hash (`final_hash`)
//!
//! When both `at_start` and `at_end` are true, `build_hump` fully collapses
//! the sequence: every level has empty left and right except the topmost,
//! which contains a single node.  That node's hash equals
//! `aggregated_node_hash(all_chunks)`.
//!
//! ```text
//! final_hash():
//!     if not (at_start and at_end): return None
//!     if empty: return default_hash
//!     // Invariant: fully-closed hump has a single node at the top level
//!     return top_level.left[0].hash
//! ```
//!
//! # Flat Storage Layout (`flatten_hump`)
//!
//! The hump is stored as a single flat `Vec<Node>` with all lefts first
//! (ascending by level), then all rights (descending by level):
//!
//! ```text
//! nodes = [L0_left..., L1_left..., ..., Lk_left..., Lk_right..., ..., L1_right..., L0_right...]
//! levels = [(l0_count, r0_count), (l1_count, r1_count), ..., (lk_count, rk_count)]
//! ```
//!
//! Access is via `left_offset(level)` and `right_offset(level)` computed
//! from the cumulative sums of the level counts.

use super::MerkleHash;
#[cfg(debug_assertions)]
use super::aggregated_hashes::aggregated_node_hash;
use super::aggregated_hashes::{
    MAX_GROUP_SIZE, MIN_GROUP_SIZE, is_natural_cut, merged_hash_of_sequence, next_merge_cut,
};

type Node = (MerkleHash, u64);

/// Scan forward for the first position where `is_natural_cut(hash)` is true,
/// ignoring `MIN_GROUP_SIZE` / `MAX_GROUP_SIZE` bounds.  Returns the index
/// within the slice, or `None` if no natural cut exists.
#[inline]
fn next_cut_unbounded(hashes: &[Node]) -> Option<usize> {
    for (i, &(h, _)) in hashes.iter().enumerate() {
        if is_natural_cut(h) {
            return Some(i);
        }
    }
    None
}

/// Scan backward for the last position where `is_natural_cut(hash)` is true,
/// ignoring `MIN_GROUP_SIZE` / `MAX_GROUP_SIZE` bounds.  Returns the index
/// within the slice, or `None` if no natural cut exists.
#[inline]
fn prev_cut_unbounded(hashes: &[Node]) -> Option<usize> {
    (0..hashes.len()).rev().find(|&i| is_natural_cut(hashes[i].0))
}

/// Find the first stable group boundary scanning left-to-right.
///
/// A position `m` is "stable" if it is always a group boundary regardless
/// of what nodes precede this slice.  This requires three consecutive
/// natural-cut positions `c0 < c1 < c2` where both gaps are in the range
/// `(MIN_GROUP_SIZE, MAX_GROUP_SIZE - 1)`.  The stable point is `c1 + 1`.
///
/// The two-gap requirement is necessary because `next_merge_cut` skips the
/// first `MIN_GROUP_SIZE` positions -- a natural cut at `c0` could be
/// absorbed into a group that started before the slice.  The gap from `c0`
/// to `c1` ensures that `c1` falls within the scan window of any group that
/// could contain `c0`.  The gap from `c1` to `c2` provides the same
/// guarantee for `c1` itself, making `c1 + 1` unconditionally stable.
///
/// Returns `None` if the slice is too short or lacks the required pattern.
pub fn find_stable_start(nodes: &[Node]) -> Option<usize> {
    if nodes.len() < MIN_GROUP_SIZE + 1 {
        return None;
    }

    let valid_gap = |gap: usize| gap > MIN_GROUP_SIZE && gap < MAX_GROUP_SIZE - 1;

    let mut prev_prev_cut: Option<usize> = None;
    let mut prev_cut: Option<usize> = None;
    let mut pos = 0;

    while pos < nodes.len() {
        if let Some(offset) = next_cut_unbounded(&nodes[pos..]) {
            let cut_pos = pos + offset;

            if let Some(pc) = prev_cut
                && valid_gap(cut_pos - pc)
                && let Some(ppc) = prev_prev_cut
                && valid_gap(pc - ppc)
            {
                return Some(pc + 1);
            }

            prev_prev_cut = prev_cut;
            prev_cut = Some(cut_pos);
            pos = cut_pos + 1;
        } else {
            break;
        }
    }

    None
}

/// Find the last stable group boundary scanning right-to-left.
///
/// The mirror of [`find_stable_start`].  A position `m` is "stable from
/// the right" if it is always a group boundary regardless of what nodes
/// are appended after this slice.  This lets us merge everything before
/// `m` without knowing the right context.
///
/// Scans right-to-left for natural-cut positions and requires three
/// consecutive ones `c0 < c1 < c2` with both gaps in the valid range.
/// Returns `c1 + 1` as the stable end point; everything from `c1 + 1`
/// onward is the unstable suffix that cannot yet be merged.
///
/// The reasoning mirrors `find_stable_start`: because `next_merge_cut`
/// scans forward with bounded lookahead, `c1` is always within the scan
/// window of any group that reaches `c0`, and `c2` guarantees `c1`
/// terminates its group.  Appending nodes after the slice can only affect
/// groups that include the last node -- groups ending before `c1 + 1` are
/// unaffected.
pub fn find_stable_end(nodes: &[Node]) -> Option<usize> {
    if nodes.len() < MIN_GROUP_SIZE + 1 {
        return None;
    }

    let valid_gap = |gap: usize| gap > MIN_GROUP_SIZE && gap < MAX_GROUP_SIZE - 1;

    let mut next_next_cut: Option<usize> = None;
    let mut next_cut: Option<usize> = None;
    let mut pos = nodes.len();

    while pos > 0 {
        if let Some(offset) = prev_cut_unbounded(&nodes[..pos]) {
            let cut_pos = offset;

            if let Some(nc) = next_cut
                && valid_gap(nc - cut_pos)
                && let Some(nnc) = next_next_cut
                && valid_gap(nnc - nc)
            {
                return Some(nc + 1);
            }

            next_next_cut = next_cut;
            next_cut = Some(cut_pos);
            pos = cut_pos;
        } else {
            break;
        }
    }

    None
}

/// Compactly represents a contiguous range of chunk hashes that have been
/// partially aggregated using the hierarchical merging algorithm.
///
/// See the [module-level documentation](self) for the full algorithmic
/// description, pseudocode, and storage analysis.
///
/// # Fields
///
/// - `nodes`:  Flat storage of all hump nodes.  Layout is all left-side nodes ascending by level, then all right-side
///   nodes descending by level.  See [`flatten_hump`] for details.
/// - `levels`: Per-level `(left_count, right_count)` pairs indexing into `nodes`.
/// - `left_offsets`: Pre-computed cumulative left offsets for O(1) access.
/// - `right_offsets`: Pre-computed cumulative right offsets for O(1) access.
/// - `at_start`: `true` if this range begins at position 0 of the full chunk sequence (left boundary is known).
/// - `at_end`: `true` if this range ends at the last chunk of the full sequence (right boundary is known).
/// - `debug_chunks`: (debug builds only) the original level-0 chunks, retained to verify that `final_hash()` matches
///   `aggregated_node_hash`.
#[derive(Clone, Debug)]
pub struct ChunkHashRange {
    nodes: Vec<Node>,
    levels: Vec<(usize, usize)>,
    /// Pre-computed: left_offsets[i] = sum of levels[0..i].0
    left_offsets: Vec<usize>,
    /// Pre-computed: right_offsets[i] = total_left + sum of levels[i+1..].1
    right_offsets: Vec<usize>,
    at_start: bool,
    at_end: bool,

    #[cfg(debug_assertions)]
    debug_chunks: Vec<Node>,
}

/// Pre-compute left and right offset arrays from levels.
#[inline]
fn compute_offsets(levels: &[(usize, usize)]) -> (Vec<usize>, Vec<usize>) {
    let n = levels.len();
    let mut left_offsets = Vec::with_capacity(n);
    let mut right_offsets = Vec::with_capacity(n);

    // left_offsets[i] = sum of levels[0..i].0
    let mut cumulative_left: usize = 0;
    for &(lc, _) in levels {
        left_offsets.push(cumulative_left);
        cumulative_left += lc;
    }

    // right_offsets[i] = total_left + sum of levels[i+1..].1
    let total_left = cumulative_left;
    let mut cumulative_right_after: usize = levels.iter().map(|&(_, rc)| rc).sum();
    for &(_, rc) in levels {
        cumulative_right_after -= rc;
        right_offsets.push(total_left + cumulative_right_after);
    }

    (left_offsets, right_offsets)
}

impl ChunkHashRange {
    /// Create a new `ChunkHashRange` from a slice of level-0 chunk hashes.
    ///
    /// - `at_start`: set `true` if `chunks[0]` is the first chunk of the entire file (left boundary is known).
    /// - `at_end`: set `true` if the last element of `chunks` is the final chunk (right boundary is known).
    ///
    /// Internally calls [`build_hump`] to produce the O(log n) hump
    /// representation.  In debug builds, retains the original chunks
    /// and verifies `final_hash()` against `aggregated_node_hash()`.
    pub fn new(at_start: bool, chunks: &[Node], at_end: bool) -> Self {
        let result = if chunks.is_empty() {
            Self {
                nodes: Vec::new(),
                levels: Vec::new(),
                left_offsets: Vec::new(),
                right_offsets: Vec::new(),
                at_start,
                at_end,
                #[cfg(debug_assertions)]
                debug_chunks: Vec::new(),
            }
        } else {
            let (nodes, levels) = build_hump(chunks, at_start, at_end);
            let (left_offsets, right_offsets) = compute_offsets(&levels);
            Self {
                nodes,
                levels,
                left_offsets,
                right_offsets,
                at_start,
                at_end,
                #[cfg(debug_assertions)]
                debug_chunks: chunks.to_vec(),
            }
        };

        #[cfg(debug_assertions)]
        result.verify_invariants();

        result
    }

    pub fn num_nodes(&self) -> usize {
        self.nodes.len()
    }

    pub fn num_levels(&self) -> usize {
        self.levels.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Slice of the left-side (prefix) nodes at the given level.
    #[inline]
    fn left_at(&self, level: usize) -> &[Node] {
        let start = self.left_offsets[level];
        &self.nodes[start..start + self.levels[level].0]
    }

    /// Slice of the right-side (suffix) nodes at the given level.
    #[inline]
    fn right_at(&self, level: usize) -> &[Node] {
        let start = self.right_offsets[level];
        &self.nodes[start..start + self.levels[level].1]
    }

    /// Merge two adjacent `ChunkHashRange`s into one.
    ///
    /// The combined range inherits `at_start` from `left_range` and
    /// `at_end` from `right_range`.
    ///
    /// At each level, the full node sequence from both humps plus any
    /// carry from the level below is reassembled:
    ///
    /// ```text
    /// full = left.left_at(l) + left.right_at(l) + carry + right.left_at(l) + right.right_at(l)
    /// ```
    ///
    /// This is then passed to [`split_and_promote`] which produces the
    /// new level's prefix, promoted nodes (carry to next level), and
    /// suffix.  The total work is O(total nodes across all levels) =
    /// O(log n).
    pub fn merge_two(left_range: &ChunkHashRange, right_range: &ChunkHashRange) -> ChunkHashRange {
        let combined_at_start = left_range.at_start;
        let combined_at_end = right_range.at_end;

        if left_range.is_empty() && right_range.is_empty() {
            return ChunkHashRange {
                nodes: Vec::new(),
                levels: Vec::new(),
                left_offsets: Vec::new(),
                right_offsets: Vec::new(),
                at_start: combined_at_start,
                at_end: combined_at_end,
                #[cfg(debug_assertions)]
                debug_chunks: Vec::new(),
            };
        }

        if left_range.is_empty() {
            return rebuild_with_flags(right_range, combined_at_start, combined_at_end);
        }

        if right_range.is_empty() {
            return rebuild_with_flags(left_range, combined_at_start, combined_at_end);
        }

        let max_levels = left_range.num_levels().max(right_range.num_levels());

        // Build output flat arrays directly to avoid Vec<Vec<Node>> overhead.
        // Left nodes are appended in level order. Right nodes are collected
        // per-level in a single vec; we track per-level counts in `levels`
        // so we can rearrange them at the end into reverse-level order.
        let estimated_total = left_range.num_nodes() + right_range.num_nodes() + 16;
        let estimated_levels = max_levels + 2;
        let mut out_left_nodes: Vec<Node> = Vec::with_capacity(estimated_total);
        let mut out_right_nodes: Vec<Node> = Vec::with_capacity(estimated_total / 2);
        let mut levels: Vec<(usize, usize)> = Vec::with_capacity(estimated_levels);
        let mut carry: Vec<Node> = Vec::new();

        // Reusable buffer for assembling full node sequence at each level
        let mut full: Vec<Node> = Vec::with_capacity(64);

        // Track at_start/at_end propagation with simple flags
        let mut all_lefts_empty = true;
        let mut all_rights_empty = true;

        for level in 0..max_levels {
            let lr_left = if level < left_range.num_levels() {
                left_range.left_at(level)
            } else {
                &[]
            };
            let lr_right = if level < left_range.num_levels() {
                left_range.right_at(level)
            } else {
                &[]
            };
            let rr_left = if level < right_range.num_levels() {
                right_range.left_at(level)
            } else {
                &[]
            };
            let rr_right = if level < right_range.num_levels() {
                right_range.right_at(level)
            } else {
                &[]
            };

            // Reuse the full buffer instead of allocating a new Vec each level
            full.clear();
            full.extend_from_slice(lr_left);
            full.extend_from_slice(lr_right);
            full.extend_from_slice(&carry);
            full.extend_from_slice(rr_left);
            full.extend_from_slice(rr_right);

            let level_at_start = combined_at_start && all_lefts_empty;
            let level_at_end = combined_at_end && all_rights_empty;

            let (prefix_len, promoted, suffix_len) = split_and_promote(&full, level_at_start, level_at_end);

            out_left_nodes.extend_from_slice(&full[..prefix_len]);
            out_right_nodes.extend_from_slice(&full[full.len() - suffix_len..]);

            if prefix_len > 0 {
                all_lefts_empty = false;
            }
            if suffix_len > 0 {
                all_rights_empty = false;
            }

            levels.push((prefix_len, suffix_len));
            carry = promoted;
        }

        while !carry.is_empty() {
            if carry.len() == 1 {
                out_left_nodes.extend_from_slice(&carry);
                levels.push((carry.len(), 0));
                carry = Vec::new();
            } else {
                let at_start_here = combined_at_start && all_lefts_empty;
                let at_end_here = combined_at_end && all_rights_empty;

                let (prefix_len, promoted, suffix_len) = split_and_promote(&carry, at_start_here, at_end_here);

                out_left_nodes.extend_from_slice(&carry[..prefix_len]);
                out_right_nodes.extend_from_slice(&carry[carry.len() - suffix_len..]);

                if prefix_len > 0 {
                    all_lefts_empty = false;
                }
                if suffix_len > 0 {
                    all_rights_empty = false;
                }

                levels.push((prefix_len, suffix_len));
                carry = promoted;
            }
        }

        // Trim empty trailing levels
        while levels.len() > 1
            && levels.last() == Some(&(0, 0))
        {
            levels.pop();
        }

        // Build final flat node array: all lefts, then rights in reverse level order.
        // out_right_nodes stores [level0_right, level1_right, ...].
        // We need them in reverse: [levelN_right, ..., level1_right, level0_right].
        let mut nodes = out_left_nodes;
        {
            let mut end = out_right_nodes.len();
            for &(_, rc) in levels.iter().rev() {
                let start = end - rc;
                nodes.extend_from_slice(&out_right_nodes[start..end]);
                end = start;
            }
        }

        let (left_offsets, right_offsets) = compute_offsets(&levels);

        let result = ChunkHashRange {
            nodes,
            levels,
            left_offsets,
            right_offsets,
            at_start: combined_at_start,
            at_end: combined_at_end,
            #[cfg(debug_assertions)]
            debug_chunks: {
                let mut c = left_range.debug_chunks.clone();
                c.extend_from_slice(&right_range.debug_chunks);
                c
            },
        };

        #[cfg(debug_assertions)]
        result.verify_invariants();

        result
    }

    /// Merge multiple adjacent ranges via left-to-right iterative pairwise merge.
    ///
    /// Equivalent to `ranges[0].merge_two(ranges[1]).merge_two(ranges[2])...`.
    /// Returns an empty fully-closed range if `ranges` is empty.
    pub fn merge(ranges: &[ChunkHashRange]) -> ChunkHashRange {
        match ranges.len() {
            0 => ChunkHashRange {
                nodes: Vec::new(),
                levels: Vec::new(),
                left_offsets: Vec::new(),
                right_offsets: Vec::new(),
                at_start: true,
                at_end: true,
                #[cfg(debug_assertions)]
                debug_chunks: Vec::new(),
            },
            1 => ranges[0].clone(),
            _ => {
                let mut result = Self::merge_two(&ranges[0], &ranges[1]);
                for range in &ranges[2..] {
                    result = Self::merge_two(&result, range);
                }
                result
            },
        }
    }

    /// Returns the final aggregated hash if both boundaries are known.
    ///
    /// Requires `at_start == true` and `at_end == true`.  When both are
    /// set, [`build_hump`] (or [`merge_two`](Self::merge_two) producing an
    /// equivalent result) fully collapses the sequence: all lower levels
    /// have `left_count == 0` and `right_count == 0`, and the topmost
    /// level contains exactly one node whose hash equals
    /// `aggregated_node_hash(all_original_chunks)`.
    ///
    /// Returns `None` if either boundary is unknown.
    pub fn final_hash(&self) -> Option<MerkleHash> {
        if !self.at_start || !self.at_end {
            return None;
        }

        if self.nodes.is_empty() {
            return Some(MerkleHash::default());
        }

        let top = self.levels.len() - 1;

        debug_assert!(
            self.levels.iter().take(top).all(|(l, r)| *l == 0 && *r == 0),
            "Fully-closed hump should have empty lower levels, but found: {:?}",
            &self.levels[..top]
        );

        let top_left = self.left_at(top);
        debug_assert!(self.right_at(top).is_empty());
        debug_assert_eq!(top_left.len(), 1);

        Some(top_left[0].0)
    }

    /// (Debug only) Verify that `final_hash()` matches `aggregated_node_hash`
    /// applied to the stored `debug_chunks`.  Only runs when both boundaries
    /// are known, providing an end-to-end correctness check.
    #[cfg(debug_assertions)]
    fn verify_invariants(&self) {
        if !self.at_start || !self.at_end {
            return;
        }
        if self.debug_chunks.is_empty() {
            return;
        }

        let expected = aggregated_node_hash(&self.debug_chunks);
        let got = self.final_hash().expect("at_start and at_end both true");
        assert_eq!(
            expected,
            got,
            "ChunkHashRange invariant: final_hash mismatch.\n\
             Expected: {expected:x}\nGot: {got:x}\n\
             Num debug_chunks: {}, num_nodes: {}",
            self.debug_chunks.len(),
            self.nodes.len(),
        );
    }
}

/// Rebuild a `ChunkHashRange` with different boundary flags.
///
/// Used when merging with an empty range changes the boundary knowledge
/// (e.g., a range that was `at_start=false` becomes `at_start=true` because
/// the empty left neighbor was `at_start=true`).  Reconstructs the level-0
/// node sequence via [`rebuild_level0`] and rebuilds the hump with the new
/// flags.  Returns a clone if flags are unchanged.
fn rebuild_with_flags(range: &ChunkHashRange, at_start: bool, at_end: bool) -> ChunkHashRange {
    if range.at_start == at_start && range.at_end == at_end {
        return range.clone();
    }

    let level0 = rebuild_level0(range);

    let (nodes, levels) = if level0.is_empty() {
        (Vec::new(), Vec::new())
    } else {
        build_hump(&level0, at_start, at_end)
    };

    let (left_offsets, right_offsets) = compute_offsets(&levels);

    let result = ChunkHashRange {
        nodes,
        levels,
        left_offsets,
        right_offsets,
        at_start,
        at_end,
        #[cfg(debug_assertions)]
        debug_chunks: range.debug_chunks.clone(),
    };

    #[cfg(debug_assertions)]
    result.verify_invariants();

    result
}

/// Reconstruct the flattened node sequence from a hump.
///
/// Walks top-down: starts with the topmost level's left + right nodes,
/// then at each lower level sandwiches the accumulated sequence between
/// that level's left and right nodes.  The result is the sequence of
/// nodes as they were before `build_hump` partitioned them.
///
/// Note: these are NOT the original level-0 chunks -- promoted nodes at
/// higher levels are already-merged hashes.  The sequence is suitable for
/// passing to `build_hump` with new flags (via [`rebuild_with_flags`]).
fn rebuild_level0(range: &ChunkHashRange) -> Vec<Node> {
    if range.levels.is_empty() {
        return Vec::new();
    }

    let top = range.levels.len() - 1;
    let mut current: Vec<Node> = Vec::with_capacity(range.nodes.len());
    current.extend_from_slice(range.left_at(top));
    current.extend_from_slice(range.right_at(top));

    for level in (0..top).rev() {
        let left = range.left_at(level);
        let right = range.right_at(level);

        let mut next = Vec::with_capacity(left.len() + current.len() + right.len());
        next.extend_from_slice(left);
        next.extend_from_slice(&current);
        next.extend_from_slice(right);
        current = next;
    }

    current
}

/// Build the hump representation from a flat slice of nodes.
///
/// Iteratively applies [`split_and_promote`] to partition nodes into
/// prefix (left), promoted (next level's input), and suffix (right).
/// The `at_start` / `at_end` flags propagate upward: a level is
/// `level_at_start` only if `at_start` is true AND all lower left
/// levels are empty (meaning no prefix was carved off below, so this
/// level truly starts at the beginning of the sequence).  Similarly
/// for `level_at_end`.
///
/// Terminates when promotion produces 0 nodes (everything went to
/// prefix/suffix) or exactly 1 node (the root of the hump).
fn build_hump(chunks: &[Node], at_start: bool, at_end: bool) -> (Vec<Node>, Vec<(usize, usize)>) {
    // Build output directly: left nodes appended in level order,
    // right nodes collected per-level then reversed at the end.
    // Conservative capacity: O(MAX_GROUP_SIZE * log_4(n)) for left+right combined.
    let est_side = MAX_GROUP_SIZE * 12;
    let mut out_left_nodes: Vec<Node> = Vec::with_capacity(est_side);
    let mut out_right_nodes: Vec<Node> = Vec::with_capacity(est_side);
    let mut levels: Vec<(usize, usize)> = Vec::with_capacity(12);
    let mut current = chunks.to_vec();

    // Track at_start/at_end propagation incrementally
    let mut all_lefts_empty = true;
    let mut all_rights_empty = true;

    loop {
        let level_at_start = at_start && all_lefts_empty;
        let level_at_end = at_end && all_rights_empty;

        let (prefix_len, promoted, suffix_len) = split_and_promote(&current, level_at_start, level_at_end);

        out_left_nodes.extend_from_slice(&current[..prefix_len]);
        out_right_nodes.extend_from_slice(&current[current.len() - suffix_len..]);

        if prefix_len > 0 {
            all_lefts_empty = false;
        }
        if suffix_len > 0 {
            all_rights_empty = false;
        }

        levels.push((prefix_len, suffix_len));

        if promoted.is_empty() {
            break;
        }
        if promoted.len() == 1 {
            out_left_nodes.extend_from_slice(&promoted);
            levels.push((promoted.len(), 0));
            break;
        }

        current = promoted;
    }

    // Build final flat node array: all lefts, then rights in reverse level order
    let mut nodes = out_left_nodes;
    {
        let mut end = out_right_nodes.len();
        for &(_, rc) in levels.iter().rev() {
            let start = end - rc;
            nodes.extend_from_slice(&out_right_nodes[start..end]);
            end = start;
        }
    }

    (nodes, levels)
}

/// The core per-level operation: partition nodes into `(prefix_len, promoted, suffix_len)`.
///
/// 1. **Determine the mergeable region**:
///    - Left boundary: position 0 if `at_start`, else [`find_stable_start`].
///    - Right boundary: `len` if `at_end`, else [`find_stable_end`].
///    - If no stable boundaries found, all nodes go to prefix (no promotion).
///
/// 2. **Merge groups within the stable region** using [`next_merge_cut`] and [`merged_hash_of_sequence`].  Each group
///    of 2-9 nodes becomes one promoted node.
///
/// 3. **Return**:
///    - `prefix_len`: number of nodes before the left stable boundary (hump's left side).
///    - `promoted`: merged parent nodes (input to the next level up).
///    - `suffix_len`: number of nodes after the right stable boundary (hump's right side).
///
/// The prefix and suffix are each bounded by `O(MAX_GROUP_SIZE * K)` where
/// `K` is the number of natural cuts needed for stability (typically 3),
/// so they contribute O(1) nodes per level.
///
/// Returns `(prefix_len, promoted_vec, suffix_len)` instead of allocating
/// prefix/suffix Vecs. The caller can slice `nodes[..prefix_len]` and
/// `nodes[nodes.len()-suffix_len..]` to get the actual data.
#[inline]
fn split_and_promote(nodes: &[Node], at_start: bool, at_end: bool) -> (usize, Vec<Node>, usize) {
    if nodes.len() <= 1 {
        return (nodes.len(), Vec::new(), 0);
    }

    let stable_start = if at_start {
        0
    } else {
        match find_stable_start(nodes) {
            Some(idx) => idx,
            None => return (nodes.len(), Vec::new(), 0),
        }
    };

    let stable_end = if at_end {
        nodes.len()
    } else {
        match find_stable_end(&nodes[stable_start..]) {
            Some(idx) => stable_start + idx,
            None => return (nodes.len(), Vec::new(), 0),
        }
    };

    if stable_start >= stable_end {
        return (nodes.len(), Vec::new(), 0);
    }

    let prefix_len = stable_start;
    let suffix_len = nodes.len() - stable_end;
    let mergeable = &nodes[stable_start..stable_end];

    // Pre-allocate: each group of ~4 nodes produces 1 promoted node
    let mut promoted = Vec::with_capacity(mergeable.len() / 3 + 1);
    let mut pos = 0;

    while pos < mergeable.len() {
        let remaining = &mergeable[pos..];
        let cut_len = next_merge_cut(remaining);
        promoted.push(merged_hash_of_sequence(&remaining[..cut_len]));
        pos += cut_len;
    }

    (prefix_len, promoted, suffix_len)
}

#[cfg(test)]
mod tests {
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::merklehash::xorb_hash;

    fn random_chunks(rng: &mut SmallRng, n: usize) -> Vec<Node> {
        (0..n)
            .map(|_| {
                let seed = rng.random::<u64>();
                (MerkleHash::random_from_seed(seed), rng.random_range(100..10000))
            })
            .collect()
    }

    fn compute_cuts_from(nodes: &[Node], start: usize) -> Vec<usize> {
        let mut cuts = Vec::new();
        let mut pos = start;
        while pos < nodes.len() {
            let remaining = &nodes[pos..];
            if remaining.is_empty() {
                break;
            }
            let cut_len = next_merge_cut(remaining);
            pos += cut_len;
            cuts.push(pos);
        }
        cuts
    }

    fn verify_stable_with_random_prefixes(nodes: &[Node], m: usize, rng: &mut SmallRng, num_prefixes: usize) -> bool {
        for _ in 0..num_prefixes {
            let prefix_len = rng.random_range(1..50);
            let prefix = random_chunks(rng, prefix_len);

            let mut combined = prefix;
            combined.extend_from_slice(nodes);

            let adjusted_m = prefix_len + m;
            let cuts = compute_cuts_from(&combined, 0);

            if !cuts.contains(&adjusted_m) {
                return false;
            }
        }
        true
    }

    fn verify_stable_end_with_random_suffixes(
        nodes: &[Node],
        m: usize,
        rng: &mut SmallRng,
        num_suffixes: usize,
    ) -> bool {
        for _ in 0..num_suffixes {
            let suffix_len = rng.random_range(1..50);
            let suffix = random_chunks(rng, suffix_len);

            let mut combined = nodes.to_vec();
            combined.extend_from_slice(&suffix);

            let cuts = compute_cuts_from(&combined, 0);

            if !cuts.contains(&m) {
                return false;
            }
        }
        true
    }

    // ========================================================================
    // Stability verification tests
    // ========================================================================

    #[test]
    fn test_find_stable_start_with_random_prefixes() {
        let mut rng = SmallRng::seed_from_u64(42);
        let mut tested = 0;

        for _ in 0..1000 {
            let n = rng.random_range(15..100);
            let nodes = random_chunks(&mut rng, n);

            if let Some(stable) = find_stable_start(&nodes) {
                tested += 1;
                assert!(
                    verify_stable_with_random_prefixes(&nodes, stable, &mut rng, 500),
                    "find_stable_start returned {stable} for n={n}, but it is NOT stable under random prefixes"
                );
            }
        }

        assert!(tested > 100, "Too few sequences had stable points: {tested}");
    }

    #[test]
    fn test_stability_exhaustive_prefix_lengths() {
        let mut rng = SmallRng::seed_from_u64(123);

        for trial in 0..500 {
            let n = rng.random_range(15..60);
            let nodes = random_chunks(&mut rng, n);

            if let Some(stable) = find_stable_start(&nodes) {
                for prefix_len in 1..=(2 * MAX_GROUP_SIZE + 2) {
                    for _ in 0..20 {
                        let prefix = random_chunks(&mut rng, prefix_len);
                        let mut combined = prefix;
                        combined.extend_from_slice(&nodes);

                        let adjusted = prefix_len + stable;
                        let cuts = compute_cuts_from(&combined, 0);

                        assert!(
                            cuts.contains(&adjusted),
                            "Trial {trial}: stable={stable} not a cut with {prefix_len}-element prefix. \
                             n={n}, adjusted={adjusted}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_find_stable_end_with_random_suffixes() {
        let mut rng = SmallRng::seed_from_u64(44);
        let mut tested = 0;

        for _ in 0..1000 {
            let n = rng.random_range(15..100);
            let nodes = random_chunks(&mut rng, n);

            if let Some(stable_end) = find_stable_end(&nodes) {
                tested += 1;
                assert!(
                    verify_stable_end_with_random_suffixes(&nodes, stable_end, &mut rng, 500),
                    "find_stable_end returned {stable_end} for n={n}, but it is NOT stable under random suffixes"
                );
            }
        }

        assert!(tested > 100, "Too few sequences had stable end points: {tested}");
    }

    #[test]
    fn test_stable_end_exhaustive_suffix_lengths() {
        let mut rng = SmallRng::seed_from_u64(125);

        for trial in 0..500 {
            let n = rng.random_range(15..60);
            let nodes = random_chunks(&mut rng, n);

            if let Some(stable_end) = find_stable_end(&nodes) {
                for suffix_len in 1..=(2 * MAX_GROUP_SIZE + 2) {
                    for _ in 0..20 {
                        let suffix = random_chunks(&mut rng, suffix_len);
                        let mut combined = nodes.clone();
                        combined.extend_from_slice(&suffix);

                        let cuts = compute_cuts_from(&combined, 0);

                        assert!(
                            cuts.contains(&stable_end),
                            "Trial {trial}: stable_end={stable_end} not a cut with {suffix_len}-element suffix. n={n}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_stable_start_implies_correct_merge() {
        let mut rng = SmallRng::seed_from_u64(555);

        for _trial in 0..500 {
            let n = rng.random_range(30..500);
            let chunks = random_chunks(&mut rng, n);

            let offset = rng.random_range(0..n.saturating_sub(20).max(1));
            if let Some(s) = find_stable_start(&chunks[offset..]) {
                let abs_stable = offset + s;
                if abs_stable < n && abs_stable > 0 {
                    let expected = xorb_hash(&chunks);
                    let r1 = ChunkHashRange::new(true, &chunks[..abs_stable], false);
                    let r2 = ChunkHashRange::new(false, &chunks[abs_stable..], true);
                    let merged = ChunkHashRange::merge_two(&r1, &r2);
                    assert_eq!(merged.final_hash().unwrap(), expected);
                }
            }
        }
    }

    #[test]
    fn test_stable_cut_found_in_long_sequences() {
        let mut rng = SmallRng::seed_from_u64(777);
        let mut found = 0;
        let trials = 1000;

        for _ in 0..trials {
            let n = rng.random_range(30..100);
            let nodes = random_chunks(&mut rng, n);
            if find_stable_start(&nodes).is_some() {
                found += 1;
            }
        }

        let rate = found as f64 / trials as f64;
        assert!(rate > 0.7, "Expected stable points in >70% of sequences of length 30-100, got {rate:.1}%");
    }

    // ========================================================================
    // ChunkHashRange correctness tests
    // ========================================================================

    #[test]
    fn test_empty() {
        let r = ChunkHashRange::new(true, &[], true);
        assert_eq!(r.final_hash(), Some(MerkleHash::default()));
    }

    #[test]
    fn test_single_chunk() {
        let h = MerkleHash::random_from_seed(42);
        let chunks = vec![(h, 1000u64)];
        let r = ChunkHashRange::new(true, &chunks, true);
        assert_eq!(r.final_hash(), Some(xorb_hash(&chunks)));
    }

    #[test]
    fn test_small_full_range() {
        let mut rng = SmallRng::seed_from_u64(12345);
        for n in 2..=30 {
            let chunks = random_chunks(&mut rng, n);
            let expected = xorb_hash(&chunks);
            let r = ChunkHashRange::new(true, &chunks, true);
            assert_eq!(r.final_hash().unwrap(), expected, "Failed for n={n}");
        }
    }

    #[test]
    fn test_no_final_hash_without_boundaries() {
        let chunks = vec![(MerkleHash::random_from_seed(1), 100)];
        assert!(ChunkHashRange::new(false, &chunks, true).final_hash().is_none());
        assert!(ChunkHashRange::new(true, &chunks, false).final_hash().is_none());
        assert!(ChunkHashRange::new(false, &chunks, false).final_hash().is_none());
    }

    #[test]
    fn test_two_way_merge_basic() {
        let mut rng = SmallRng::seed_from_u64(99);
        let chunks = random_chunks(&mut rng, 16);
        let expected = xorb_hash(&chunks);

        for split in 1..16 {
            let r1 = ChunkHashRange::new(true, &chunks[..split], false);
            let r2 = ChunkHashRange::new(false, &chunks[split..], true);
            let merged = ChunkHashRange::merge_two(&r1, &r2);
            assert_eq!(merged.final_hash().unwrap(), expected, "Failed split={split}");
        }
    }

    #[test]
    fn test_find_stable_start_basic() {
        let mut rng = SmallRng::seed_from_u64(777);
        let chunks = random_chunks(&mut rng, 200);

        if let Some(stable) = find_stable_start(&chunks) {
            assert!(stable >= MIN_GROUP_SIZE);
            assert!(stable <= chunks.len());
        }
    }

    #[test]
    fn test_two_way_merge_sweep_16() {
        let mut rng = SmallRng::seed_from_u64(42);
        for trial in 0..800 {
            let chunks = random_chunks(&mut rng, 16);
            let expected = xorb_hash(&chunks);

            let split = rng.random_range(1..16);
            let r1 = ChunkHashRange::new(true, &chunks[..split], false);
            let r2 = ChunkHashRange::new(false, &chunks[split..], true);
            let merged = ChunkHashRange::merge_two(&r1, &r2);
            assert_eq!(merged.final_hash().unwrap(), expected, "Failed trial {trial}, split at {split}");
        }
    }

    #[test]
    fn test_two_way_merge_scaling() {
        let mut rng = SmallRng::seed_from_u64(123);
        for n in (20..=200).step_by(4) {
            for _ in 0..200 {
                let chunks = random_chunks(&mut rng, n);
                let expected = xorb_hash(&chunks);

                let split = rng.random_range(1..n);
                let r1 = ChunkHashRange::new(true, &chunks[..split], false);
                let r2 = ChunkHashRange::new(false, &chunks[split..], true);
                let merged = ChunkHashRange::merge_two(&r1, &r2);
                assert_eq!(merged.final_hash().unwrap(), expected, "Failed n={n}, split={split}");
            }
        }
    }

    #[test]
    fn test_multi_way_merge() {
        let mut rng = SmallRng::seed_from_u64(987);
        for n in (16..=200).step_by(8) {
            for _ in 0..100 {
                let chunks = random_chunks(&mut rng, n);
                let expected = xorb_hash(&chunks);

                let num_splits = rng.random_range(2..=5usize.min(n - 1));
                let mut split_points: Vec<usize> = (0..num_splits).map(|_| rng.random_range(1..n)).collect();
                split_points.sort();
                split_points.dedup();
                if split_points.is_empty() {
                    split_points.push(n / 2);
                }

                let mut ranges = Vec::new();
                let mut prev = 0;
                for &sp in &split_points {
                    let is_start = prev == 0;
                    ranges.push(ChunkHashRange::new(is_start, &chunks[prev..sp], false));
                    prev = sp;
                }
                ranges.push(ChunkHashRange::new(false, &chunks[prev..], true));

                let merged = ChunkHashRange::merge(&ranges);
                assert_eq!(merged.final_hash().unwrap(), expected, "Multi-way failed n={n}, splits={split_points:?}");
            }
        }
    }

    #[test]
    fn test_storage_is_log_n() {
        let mut rng = SmallRng::seed_from_u64(456);
        for n in [100, 500, 1000, 5000] {
            let chunks = random_chunks(&mut rng, n);

            let r = ChunkHashRange::new(false, &chunks, false);

            let log_n = (n as f64).log2().ceil() as usize;
            let max_expected = MAX_GROUP_SIZE * log_n * 3;

            assert!(r.num_nodes() <= max_expected, "n={n}, nodes={}, max={max_expected}", r.num_nodes());
        }
    }

    #[test]
    fn test_merge_with_existing_reference_hashes() {
        fn rh(h: u64) -> MerkleHash {
            if h == 0 {
                [0; 4].into()
            } else {
                MerkleHash::random_from_seed(h)
            }
        }

        let test_cases: Vec<Vec<u64>> = vec![
            vec![1, 2, 3],
            vec![1, 2, 1, 2, 3, 4],
            (0..8).collect(),
            (0..8).chain([1, 1, 1, 1]).collect(),
            (0..8).flat_map(|h| [h, h]).collect(),
        ];

        for seeds in &test_cases {
            let chunks: Vec<Node> = seeds.iter().map(|&s| (rh(s), s * 100)).collect();
            let expected = xorb_hash(&chunks);

            for split in 1..chunks.len() {
                let r1 = ChunkHashRange::new(true, &chunks[..split], false);
                let r2 = ChunkHashRange::new(false, &chunks[split..], true);
                let merged = ChunkHashRange::merge_two(&r1, &r2);
                assert_eq!(merged.final_hash().unwrap(), expected, "Reference failed: seeds={seeds:?}, split={split}");
            }
        }
    }

    #[test]
    fn test_three_way_merge_all_splits() {
        let mut rng = SmallRng::seed_from_u64(321);

        for n in [8, 12, 16, 20, 24] {
            let chunks = random_chunks(&mut rng, n);
            let expected = xorb_hash(&chunks);

            for s1 in 1..n - 1 {
                for s2 in s1 + 1..n {
                    let r1 = ChunkHashRange::new(true, &chunks[..s1], false);
                    let r2 = ChunkHashRange::new(false, &chunks[s1..s2], false);
                    let r3 = ChunkHashRange::new(false, &chunks[s2..], true);
                    let merged = ChunkHashRange::merge(&[r1, r2, r3]);
                    assert_eq!(merged.final_hash().unwrap(), expected, "Three-way failed: n={n}, s1={s1}, s2={s2}");
                }
            }
        }
    }

    #[test]
    fn test_merge_preserves_log_storage() {
        let mut rng = SmallRng::seed_from_u64(789);

        for n in [100, 500, 1000] {
            let chunks = random_chunks(&mut rng, n);
            let split = n / 2;

            let r1 = ChunkHashRange::new(true, &chunks[..split], false);
            let r2 = ChunkHashRange::new(false, &chunks[split..], true);
            let merged = ChunkHashRange::merge_two(&r1, &r2);

            let log_n = (n as f64).log2().ceil() as usize;
            let max_expected = MAX_GROUP_SIZE * log_n * 3;

            assert!(
                merged.num_nodes() <= max_expected,
                "Merged n={n}: nodes={}, max={max_expected}",
                merged.num_nodes()
            );
        }
    }

    #[test]
    fn test_hump_invariants() {
        let mut rng = SmallRng::seed_from_u64(654);

        for n in [20, 50, 100, 200] {
            let chunks = random_chunks(&mut rng, n);

            for &(at_start, at_end) in &[(true, true), (true, false), (false, true), (false, false)] {
                let r = ChunkHashRange::new(at_start, &chunks, at_end);

                if r.levels.is_empty() {
                    continue;
                }

                for level in 0..r.num_levels() {
                    let left = r.left_at(level);
                    let right = r.right_at(level);
                    assert!(
                        left.len() + right.len() <= n,
                        "Level {level} has too many nodes: left={}, right={}",
                        left.len(),
                        right.len()
                    );
                }
            }
        }
    }

    #[test]
    fn test_merge_preserves_log_storage_multi() {
        let mut rng = SmallRng::seed_from_u64(790);

        for total in [200, 500, 1000, 2000] {
            let chunks = random_chunks(&mut rng, total);
            let expected = xorb_hash(&chunks);
            let log_n = (total as f64).log2().ceil() as usize;
            let max_expected = MAX_GROUP_SIZE * log_n * 3;

            let chunk_size = rng.random_range(10..50);
            let mut ranges: Vec<ChunkHashRange> = Vec::new();
            let mut pos = 0;
            while pos < total {
                let end = (pos + chunk_size).min(total);
                let is_start = pos == 0;
                let is_end = end == total;
                ranges.push(ChunkHashRange::new(is_start, &chunks[pos..end], is_end));
                pos = end;
            }

            let mut merged = ranges[0].clone();
            for range in &ranges[1..] {
                merged = ChunkHashRange::merge_two(&merged, range);
                assert!(
                    merged.num_nodes() <= max_expected,
                    "After merging, n={total}: nodes={}, max={max_expected}",
                    merged.num_nodes()
                );
            }

            assert_eq!(merged.final_hash().unwrap(), expected);
        }
    }

    /// Verify logarithmic storage growth by streaming millions of hashes
    /// in fixed-size batches. Measures node counts at milestones AND
    /// tracks worst-case node count across the entire streaming process.
    /// Uses at_end=false throughout to measure the open-ended streaming
    /// case (the one that actually matters for storage bounds).
    ///
    /// Run with: `cargo test --release -- test_storage_scaling_large --ignored --nocapture`
    #[test]
    #[ignore]
    fn test_storage_scaling_large() {
        let mut rng = SmallRng::seed_from_u64(12321);
        let batch_size = 1000;
        let milestones: Vec<usize> = vec![1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000];
        let max_milestone = *milestones.last().unwrap();

        let mut accumulated = ChunkHashRange::new(true, &random_chunks(&mut rng, batch_size), false);
        let mut total_chunks: usize = batch_size;
        let mut milestone_idx = 0;

        let mut results: Vec<(usize, usize, usize, usize)> = Vec::new();
        let mut worst_since_last: usize = accumulated.num_nodes();

        while total_chunks < max_milestone && milestone_idx < milestones.len() {
            let batch = random_chunks(&mut rng, batch_size);
            let batch_range = ChunkHashRange::new(false, &batch, false);
            accumulated = ChunkHashRange::merge_two(&accumulated, &batch_range);
            total_chunks += batch_size;

            worst_since_last = worst_since_last.max(accumulated.num_nodes());

            if total_chunks >= milestones[milestone_idx] {
                results.push((total_chunks, accumulated.num_nodes(), accumulated.num_levels(), worst_since_last));
                worst_since_last = 0;
                milestone_idx += 1;
            }
        }

        eprintln!("\n=== Storage scaling (batch_size={batch_size}, at_end=false throughout) ===");
        eprintln!("{:>15} {:>10} {:>8} {:>12} {:>12}", "total_chunks", "nodes", "levels", "nodes/log2", "worst/log2");
        for &(n, nodes, levels, worst) in &results {
            let log2_n = (n as f64).log2();
            eprintln!(
                "{n:>15} {nodes:>10} {levels:>8} {:>12.2} {:>12.2}",
                nodes as f64 / log2_n,
                worst as f64 / log2_n,
            );
        }

        for &(n, _nodes, _levels, worst) in &results {
            let log2_n = (n as f64).log2();
            let ratio = worst as f64 / log2_n;
            let bound = (MAX_GROUP_SIZE as f64) * 4.0;
            assert!(ratio < bound, "n={n}: worst_nodes={worst}, ratio={ratio:.1}, exceeds {bound:.0} * log2(n)",);
        }
    }
}
