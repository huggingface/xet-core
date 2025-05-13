// Implements BG4 prediction by examinining the maximum KL divergence between
// the distribution of per-byte popcounts on the 4 slices formed by taking the
// i'th byte of each 4-byte block.  This predicts whether bg4 byte rearranging
// will help with the compression with good accuracy.  See the analysis in
// the accompaning scripts to reproduce the experiments.
//
// The below methods implement several ways to calculate the per-byte popcnt.
//
// Benchmarking these on an Mac M1, we get:
//
// Reference: 1608.03 MB/s
// V1: 1663.79 MB/s
// SWAR: 2392.19 MB/s (Fallback method on intel)
// Neon: 3222.63 MB/s (simd method with neon)
//
// The default currently is to use Neon on Aarch64 when supported and fall back
// to SWAR elsewhere.

#[derive(Default)]
pub struct BG4Predictor {
    histograms: [[u32; 9]; 4],
}

#[allow(dead_code)] // A lot of methods are only used for testing or benchmarking
impl BG4Predictor {
    /// Reference version; used for testing, ensures that the histograms are accurate.
    pub fn add_data_reference(&mut self, offset: usize, data: &[u8]) {
        for (i, &x) in data.iter().enumerate() {
            self.histograms[(i + offset) % 4][x.count_ones() as usize] += 1;
        }
    }

    /// Older method for benchmark comparison; avoids bounds checks
    #[allow(dead_code)]
    pub fn add_data_v1(&mut self, offset: usize, data: &[u8]) {
        unsafe {
            let mut ptr = data.as_ptr();
            let end_ptr = ptr.add(data.len());
            let mut idx = (offset % 4) as u32;

            let dest_ptr = self.histograms.as_mut_ptr() as *mut u32;

            while !std::ptr::eq(ptr, end_ptr) {
                let n_ones = (*ptr).count_ones();
                let loc = (idx % 4) * 9 + n_ones;
                *(dest_ptr.add(loc as usize)) += 1;
                ptr = ptr.add(1);
                idx += 1
            }
        }
    }

    #[inline(always)]
    unsafe fn apply_perbyte_popcounts(&mut self, per_byte_popcount: u128, offset: usize, byte_range: (usize, usize)) {
        let dest_ptr = self.histograms.as_mut_ptr() as *mut u32;
        let per_byte_popcnt = per_byte_popcount.to_le_bytes();

        for i in byte_range.0..byte_range.1 {
            let idx = i + offset;
            let n_ones = *per_byte_popcnt.get_unchecked(i) as usize;
            let loc = (idx % 4) * 9 + n_ones;
            *dest_ptr.add(loc) += 1;
        }
    }

    /// This is a reference version that uses the SWAR bit twiddling trick; see Hacker's delight for reference.
    /// A lot of very cheap operations on 16 bytes at a time is likely faster than running popcnt on each byte.
    /// On other archetectures, used as the fallback method.
    #[inline(always)]
    fn popcnt_u128_swar(v: u128) -> u128 {
        const M1: u128 = 0x5555_5555_5555_5555_5555_5555_5555_5555u128;
        const M2: u128 = 0x3333_3333_3333_3333_3333_3333_3333_3333u128;
        const M3: u128 = 0x0F0F_0F0F_0F0F_0F0F_0F0F_0F0F_0F0F_0F0Fu128;

        let mut v = v;
        v = v - ((v >> 1) & M1);
        v = (v & M2) + ((v >> 2) & M2);
        v = (v + (v >> 4)) & M3;

        v
    }

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    #[inline(always)]
    fn popcnt_u128_aarch64_vctnq(v: u128) -> u128 {
        unsafe {
            use core::arch::aarch64::{uint8x16_t, vcntq_u8};
            let input = core::mem::transmute::<u128, uint8x16_t>(v);
            let result = vcntq_u8(input);
            core::mem::transmute::<uint8x16_t, u128>(result)
        }
    }

    fn add_data_impl(&mut self, offset: usize, data: &[u8], calc_u128_popcnt: impl Fn(u128) -> u128) {
        if data.is_empty() {
            return;
        }
        let mut ptr = data.as_ptr();
        let mut remaining = data.len();

        // Just copy it in and run it if we have a small amount.
        if remaining <= 16 {
            unsafe {
                let mut buffer = [0u8; 16];
                core::ptr::copy_nonoverlapping(ptr, buffer.as_mut_ptr(), remaining);
                let per_byte_popcnt = calc_u128_popcnt(u128::from_le_bytes(buffer));
                self.apply_perbyte_popcounts(per_byte_popcnt, offset, (0, remaining));
            }
            return;
        }

        // How many bytes from the start of data do we need move in order to get to an alignment boundary for
        // aligned reads of u128 values?
        let n_align_bytes = ptr.align_offset(core::mem::align_of::<u128>());

        // Okay to compute one offset value for each u128 value, as it's just used
        // modulo 4 to put things in the correct histograms.
        let u128_common_offset = offset + n_align_bytes;

        // Process the first bytes that are possibly unaligned.
        if n_align_bytes != 0 {
            let head_bytes = size_of::<u128>() - n_align_bytes;

            // Copy the first `head_bytes` into the end of a temp buffer
            let mut buffer = [0u8; 16];
            unsafe {
                core::ptr::copy_nonoverlapping(ptr, buffer.as_mut_ptr().add(head_bytes), n_align_bytes);

                let per_byte_popcnt = calc_u128_popcnt(u128::from_le_bytes(buffer));
                self.apply_perbyte_popcounts(per_byte_popcnt, u128_common_offset, (head_bytes, 16));

                ptr = ptr.add(n_align_bytes);
            }
            remaining -= n_align_bytes;
        }

        // Body: aligned reads, several at once.  4 seems to benchmark the fastest.
        const BLOCK_SIZE: usize = 4;
        while remaining >= BLOCK_SIZE * 16 {
            unsafe {
                // Force the compiler to first perform an aligned read by casting to u128, then handle the endianness
                // just for consistency.  The latter part should be a no-op on little-endian machines.
                let raw_input = *(ptr as *const [u128; BLOCK_SIZE]);
                let mut popcnt_v = [0u128; BLOCK_SIZE];

                // We can add the counts directly here; as long as 9 * BLOCK_SIZE < 256 so each byte doesn't overflow
                // into the next byte over.
                for i in 0..raw_input.len() {
                    // Ensure we're handling endianess correctly.  Should optimize out endian switching calls on
                    // little-endian machines.
                    *popcnt_v.get_unchecked_mut(i) =
                        calc_u128_popcnt(u128::from_le_bytes(raw_input.get_unchecked(i).to_ne_bytes()));
                }

                // Now, translate this out to aggregated stuff
                for i in 0..raw_input.len() {
                    self.apply_perbyte_popcounts(*popcnt_v.get_unchecked(i), u128_common_offset, (0, 16));
                }

                ptr = ptr.add(BLOCK_SIZE * 16);
                remaining -= BLOCK_SIZE * 16;
            }
        }

        // Body: aligned reads
        while remaining >= 16 {
            unsafe {
                // Force the compiler to first perform an aligned read by casting to u128, then handle the endianness
                // just for consistency.  The latter part should be a no-op on little-endian machines.
                let raw_input = *(ptr as *const u128);
                let per_byte_popcnt = calc_u128_popcnt(u128::from_le_bytes(raw_input.to_ne_bytes()));
                self.apply_perbyte_popcounts(per_byte_popcnt, u128_common_offset, (0, 16));

                ptr = ptr.add(16);
                remaining -= 16;
            }
        }

        // Tail: copy final bytes into a zero-padded buffer
        if remaining > 0 {
            unsafe {
                let mut buffer = [0u8; 16];
                core::ptr::copy_nonoverlapping(ptr, buffer.as_mut_ptr(), remaining);
                let per_byte_popcnt = calc_u128_popcnt(u128::from_le_bytes(buffer));
                self.apply_perbyte_popcounts(per_byte_popcnt, u128_common_offset, (0, remaining));
            }
        }
    }

    pub fn add_data_swar(&mut self, offset: usize, data: &[u8]) {
        self.add_data_impl(offset, data, Self::popcnt_u128_swar);
    }

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    pub fn add_data(&mut self, offset: usize, data: &[u8]) {
        self.add_data_impl(offset, data, Self::popcnt_u128_aarch64_vctnq);
    }

    #[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
    pub fn add_data(&mut self, offset: usize, data: &[u8]) {
        self.add_data_impl(offset, data, Self::popcnt_u128_swar);
    }

    pub fn histograms(&self) -> [[u32; 9]; 4] {
        self.histograms
    }

    #[allow(clippy::needless_range_loop)]
    pub fn bg4_recommended(&self) -> bool {
        // Add up the histograms into one base histogram.

        // Put in a 1 as the base count to ensure that the probability of
        // a state is never zero.
        let mut base_counts = [1u32; 9];
        let mut totals = [0u32; 4];

        for i in 0..4 {
            for j in 0..9 {
                let c = self.histograms[i][j];
                base_counts[j] += c;
                totals[i] += c;
            }
        }

        let base_total: u32 = totals.iter().sum();

        let mut max_kl_div = 0f64;

        // Now, calculate the maximum kl divergence between each of the 4
        // byte group values from the base total.
        for i in 0..4 {
            let mut kl_div = 0.;
            for j in 0..9 {
                let p = self.histograms[i][j] as f64 / totals[i] as f64;
                let q = base_counts[j] as f64 / base_total as f64;
                kl_div += p * (p / q).ln();
            }

            max_kl_div = max_kl_div.max(kl_div);
        }

        // This criteria was chosen empirically by using logistic regression on
        // the sampled features of a number of models and how well they predict
        // whether bg4 is recommended.  This criteria is beautifully simple and
        // also performs as well as any.  See the full analysis in the
        // byte_grouping/compression_stats folder and code.
        max_kl_div > 0.02
    }
}

#[cfg(test)]
mod tests {
    use std::mem::align_of;

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    fn run_histogram_test(offset: usize, data: &[u8]) {
        let mut reference = BG4Predictor::default();
        let mut swar = BG4Predictor::default();
        let mut actual = BG4Predictor::default();

        reference.add_data_reference(offset, data);
        swar.add_data_swar(offset, data);
        actual.add_data(offset, data);

        assert_eq!(
            reference.histograms, swar.histograms,
            "Histogram mismatch at offset {} with data {:?} (swar method)",
            offset, &data
        );
        assert_eq!(
            reference.histograms, actual.histograms,
            "Histogram mismatch at offset {} with data {:?}",
            offset, &data
        );
    }

    #[test]
    fn test_empty_data() {
        run_histogram_test(0, &[]);
        run_histogram_test(10, &[]);
    }

    #[test]
    fn test_zeros_simple() {
        run_histogram_test(0, &[0u8; 1]);
        run_histogram_test(0, &[0u8; 16]);
        run_histogram_test(0, &[0u8; 19]);
        run_histogram_test(3, &[0u8; 1]);
        run_histogram_test(3, &[0u8; 19]);
        run_histogram_test(3, &[1u8; 1024]);
    }

    #[test]
    fn test_small_inputs() {
        run_histogram_test(2, &[0xFF]);
        run_histogram_test(1, &[0x01, 0x03, 0x07]);
        run_histogram_test(3, &[0xAA, 0x55, 0x33, 0xCC]);
    }

    #[test]
    fn test_aligned_data() {
        let mut data = [0u8; 32];
        for i in 0..32 {
            data[i] = (i * 37) as u8; // arbitrary pattern
        }

        assert_eq!(data.as_ptr() as usize % align_of::<u128>(), 0); // should be aligned
        run_histogram_test(0, &data);
        run_histogram_test(5, &data);
    }

    #[test]
    fn test_unaligned_data_prefix() {
        let mut data = vec![0u8; 64];
        for i in 0..data.len() {
            data[i] = (i * 13 + 7) as u8;
        }

        // Use unaligned subslices
        for offset in 0..8 {
            run_histogram_test(0, &data[offset..]);
            run_histogram_test(offset, &data[offset..]);
        }
    }

    #[test]
    fn test_random_data() {
        let mut rng = StdRng::seed_from_u64(0xDEADBEEF);
        let mut data = vec![0u8; 2000];
        rng.fill(&mut data[..]);

        for offset in 0..16 {
            for start_offset in 0..16 {
                // Test a few different sizes here to hit all the corner cases.
                run_histogram_test(offset, &data[start_offset..(start_offset + 1)]);
                run_histogram_test(offset, &data[start_offset..(start_offset + 16)]);
                run_histogram_test(offset, &data[start_offset..(start_offset + 19)]);
                run_histogram_test(offset, &data[start_offset..]);
            }
        }
    }

    #[test]
    fn test_tail_handling() {
        // input length not divisible by 16
        for len in 1..32 {
            let mut data = vec![0u8; len];
            for i in 0..len {
                data[i] = (i * 17 + 19) as u8;
            }
            run_histogram_test(0, &data);
            run_histogram_test(4, &data);
        }
    }
}
