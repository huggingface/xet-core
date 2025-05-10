#[derive(Default)]
pub struct BG4Predictor {
    histograms: [[u32; 9]; 4],
}

impl BG4Predictor {
    // Older, proven version; used for testing, ensures that the histograms are accurate.
    #[allow(dead_code)]
    fn add_data_reference(&mut self, offset: usize, data: &[u8]) {
        for (i, &x) in data.iter().enumerate() {
            self.histograms[(i + offset) % 4][x.count_ones() as usize] += 1;
        }
    }

    #[inline(always)]
    unsafe fn process_u128(&mut self, offset: usize, v: u128, byte_range: (usize, usize)) {
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        let per_byte_popcnt = {
            use core::arch::aarch64::{uint8x16_t, vcntq_u8};
            let input = core::mem::transmute::<[u8; 16], uint8x16_t>(v.to_le_bytes());
            let result = vcntq_u8(input);
            core::mem::transmute::<uint8x16_t, [u8; 16]>(result)
        };

        #[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
        let per_byte_popcnt = {
            const M1: u128 = 0x5555_5555_5555_5555_5555_5555_5555_5555u128;
            const M2: u128 = 0x3333_3333_3333_3333_3333_3333_3333_3333u128;
            const M3: u128 = 0x0F0F_0F0F_0F0F_0F0F_0F0F_0F0F_0F0F_0F0Fu128;

            let mut v = v;
            v = v - ((v >> 1) & M1);
            v = (v & M2) + ((v >> 2) & M2);
            v = (v + (v >> 4)) & M3;
            v.to_le_bytes()
        };

        let dest_ptr = self.histograms.as_mut_ptr() as *mut u32;

        for i in byte_range.0..byte_range.1 {
            let idx = i + offset;
            let n_ones = *per_byte_popcnt.get_unchecked(i) as usize;
            let loc = (idx % 4) * 9 + n_ones;
            *(dest_ptr.add(loc as usize)) += 1;
        }
    }

    pub fn add_data(&mut self, offset: usize, data: &[u8]) {
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
                self.process_u128(offset, u128::from_le_bytes(buffer), (0, remaining));
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

                self.process_u128(u128_common_offset, u128::from_le_bytes(buffer), (head_bytes, 16));
                ptr = ptr.add(n_align_bytes);
            }
            remaining -= n_align_bytes;
        }

        // Body: aligned reads
        while remaining >= 16 {
            unsafe {
                let chunk: u128 = *(ptr as *const u128);
                self.process_u128(u128_common_offset, chunk, (0, 16));
                ptr = ptr.add(16);
                remaining -= 16;
            }
        }

        // Tail: copy final bytes into a zero-padded buffer
        if remaining > 0 {
            unsafe {
                let mut buffer = [0u8; 16];
                core::ptr::copy_nonoverlapping(ptr, buffer.as_mut_ptr(), remaining);
                self.process_u128(u128_common_offset, u128::from_le_bytes(buffer), (0, remaining));
            }
        }
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
        let mut actual = BG4Predictor::default();

        reference.add_data_reference(offset, data);
        actual.add_data(offset, data);

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
        //run_histogram_test(0, &[0u8; 1]);
        // run_histogram_test(0, &[0u8; 16]);
        run_histogram_test(0, &[0u8; 19]);
        run_histogram_test(3, &[0u8; 1]);
        run_histogram_test(3, &[0u8; 16]);
        run_histogram_test(3, &[0u8; 19]);
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
        let mut data = vec![0u8; 217];
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
