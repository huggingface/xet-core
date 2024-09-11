use std::iter;
use std::ops::Range;

/// Default size of blocks in bytes.
pub const DEFAULT_BLOCK_SIZE_BYTES: u64 = 16 * 1024 * 1024; // 16MiB

/// Helper object for converting between absolute offsets and fixed size
/// blocks within a file.
#[derive(Debug)]
pub struct BlockConverter {
    /// Size of a block in bytes
    block_size: u64,
}

impl BlockConverter {
    pub fn new(block_size: u64) -> BlockConverter {
        BlockConverter { block_size }
    }

    /// Breaks up the given range into a series of BlockRange corresponding to the
    /// blocks contained within the range.
    ///
    /// ## Implementation note
    /// Lifetimes are explicitly indicated here because the returned iterator needs
    /// to access a method on the BlockConverter (i.e. [within_block](BlockConverter::within_block))
    /// to dynamically generate the next range. Thus, the `BlockConverter` needs to
    /// live as long as the returned iterator.
    ///
    /// We also add `+ Send` to the Iterator object to indicate to the caller that the
    /// provided iterator is safe to be used in async contexts. Without it, the caller
    /// will have a problem saving the state of the iterator on calls to await (i.e. a
    /// compilation error).
    pub fn to_block_ranges<'a>(
        &'a self,
        range: Range<u64>,
    ) -> Box<dyn Iterator<Item = BlockRange> + 'a + Send> {
        if range.start == range.end {
            return Box::new(iter::empty::<BlockRange>());
        }
        let (idx_start, off_start) = self.abs_to_block(range.start);
        let (mut idx_end, off_end) = self.abs_to_block(range.end);

        if off_end == 0 {
            // we don't need to include the last block since range.end is exclusive.
            idx_end -= 1;
        }

        Box::new((idx_start..(idx_end + 1)).map(move |idx| {
            let start = if self.within_block(idx, range.start) {
                off_start
            } else {
                0
            };
            let end = if self.within_block(idx, range.end) {
                off_end
            } else {
                self.block_size
            };
            BlockRange::new(idx, start..end, self.block_size)
        }))
    }

    /// Returns the BlockRange of the entire block located at idx. If the end of the block
    /// extends past the "total_size" of the file, then the end of the block_range will be
    /// adjusted.
    /// If the block index is past the end of the file, then None will be returned.
    pub fn get_full_block_range_for(
        &self,
        idx: u64,
        total_size: Option<u64>,
    ) -> Option<BlockRange> {
        let r = self.block_to_abs(idx);
        let mut end_off = self.block_size;
        if let Some(total_size) = total_size {
            if r.start >= total_size {
                return None;
            }
            if r.end > total_size {
                end_off = total_size - r.start
            }
        }
        Some(BlockRange::new(idx, 0..end_off, self.block_size))
    }

    /// Takes an absolute offset and converts it into block coordinates (block_idx, block_offset),
    /// where block_offset is relative to the start of the block.
    pub fn abs_to_block(&self, abs_offset: u64) -> (u64, u64) {
        (abs_offset / self.block_size, abs_offset % self.block_size)
    }

    /// takes the index of some block and returns the absolute offsets of the block as a range.
    pub fn block_to_abs(&self, block_index: u64) -> Range<u64> {
        (block_index * self.block_size)..((block_index + 1) * self.block_size)
    }

    pub fn within_block(&self, block_index: u64, abs_off: u64) -> bool {
        (block_index * self.block_size) <= abs_off
            && abs_off < ((block_index + 1) * self.block_size)
    }
}

impl Default for BlockConverter {
    fn default() -> Self {
        BlockConverter::new(DEFAULT_BLOCK_SIZE_BYTES)
    }
}

/// A BlockRange corresponds to some range within a particular fixed-size block.
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct BlockRange {
    idx: u64,
    range: Range<u64>,
    block_size: u64,
}

impl BlockRange {
    pub fn new(idx: u64, range: Range<u64>, block_size: u64) -> BlockRange {
        assert!(range.end - range.start <= block_size);
        BlockRange {
            idx,
            range,
            block_size,
        }
    }

    /// Getter for the index of the block within an overall file.
    pub fn idx(&self) -> u64 {
        self.idx
    }

    /// Getter for the starting offset within the block.
    pub fn start_off(&self) -> u64 {
        self.range.start
    }

    /// Getter for the ending offset within the block.
    pub fn end_off(&self) -> u64 {
        self.range.end
    }

    /// Getter for the block size.
    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn to_abs_offsets(&self) -> Range<u64> {
        let block_start = self.idx * self.block_size;
        (block_start + self.range.start)..(block_start + self.range.end)
    }
}

#[cfg(test)]
mod block_convertor_tests {
    use super::*;

    const CACHE_SIZE_TEST: u64 = 20;

    fn default_converter() -> BlockConverter {
        BlockConverter::new(CACHE_SIZE_TEST)
    }

    #[test]
    fn test_abs_to_block() {
        let c = default_converter();
        assert_eq!(c.abs_to_block(5), (0, 5));
        assert_eq!(c.abs_to_block(0), (0, 0));
        assert_eq!(c.abs_to_block(20), (1, 0));
        assert_eq!(c.abs_to_block(39), (1, 19));
    }

    #[test]
    fn test_within_block() {
        let c = default_converter();
        assert!(c.within_block(0, 15));
        assert!(c.within_block(0, 0));
        assert!(!c.within_block(0, 20));
        assert!(!c.within_block(0, 43));
        assert!(c.within_block(3, 65));
        assert!(c.within_block(1, 20));
    }

    #[test]
    fn test_to_block_ranges() {
        let c = default_converter();
        let mut iter = c.to_block_ranges(5..37);
        assert_eq!(iter.next().unwrap(), BlockRange::new(0, 5..20, 20));
        assert_eq!(iter.next().unwrap(), BlockRange::new(1, 0..17, 20));
        assert!(iter.next().is_none());

        let mut iter = c.to_block_ranges(0..20);
        assert_eq!(iter.next().unwrap(), BlockRange::new(0, 0..20, 20));
        assert!(iter.next().is_none());

        let mut iter = c.to_block_ranges(40..80);
        assert_eq!(iter.next().unwrap(), BlockRange::new(2, 0..20, 20));
        assert_eq!(iter.next().unwrap(), BlockRange::new(3, 0..20, 20));
        assert!(iter.next().is_none());

        let mut iter = c.to_block_ranges(25..30);
        assert_eq!(iter.next().unwrap(), BlockRange::new(1, 5..10, 20));
        assert!(iter.next().is_none());

        let mut iter = c.to_block_ranges(25..26);
        assert_eq!(iter.next().unwrap(), BlockRange::new(1, 5..6, 20));
        assert!(iter.next().is_none());

        let mut iter = c.to_block_ranges(0..0);
        assert!(iter.next().is_none());
        let mut iter = c.to_block_ranges(24..24);
        assert!(iter.next().is_none());
    }
}
