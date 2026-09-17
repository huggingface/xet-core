use std::ptr::copy_nonoverlapping;

/// Allocates a `Vec<u8>` of length `n` without zero-initializing its contents.
///
/// # Safety
/// The caller must write every one of the `n` bytes before the vector is read
/// (including before it is dropped, since `u8` has no drop glue this only
/// matters for reads).
#[allow(clippy::uninit_vec)] // intentional: callers write every byte before reading, per the contract above
unsafe fn alloc_uninit(n: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(n);
    // SAFETY: `v` has capacity `n` and holds `u8`, which has no validity
    // invariants, so extending its length to `n` before writing is sound as
    // long as the caller's contract (every byte written before read) holds.
    unsafe { v.set_len(n) };
    v
}

pub fn bg4_split_separate(data: &[u8]) -> [Vec<u8>; 4] {
    let n = data.len();
    let split = n / 4;
    let rem = n % 4;
    // SAFETY: the loop over `0..split` below writes index `i` of every one of
    // d0..d3, and the `match rem` block writes the one remaining tail index
    // for whichever of d0..d2 has length `split + 1`; d3 never has a tail
    // element. Together every byte of d0..d3 is written before return.
    let mut d0 = unsafe { alloc_uninit(split + 1.min(rem)) };
    let mut d1 = unsafe { alloc_uninit(split + 1.min(rem.saturating_sub(1))) };
    let mut d2 = unsafe { alloc_uninit(split + 1.min(rem.saturating_sub(2))) };
    let mut d3 = unsafe { alloc_uninit(split) };

    for i in 0..split {
        d0[i] = data[4 * i];
        d1[i] = data[4 * i + 1];
        d2[i] = data[4 * i + 2];
        d3[i] = data[4 * i + 3];
    }

    match rem {
        1 => {
            d0[split] = data[4 * split];
        },
        2 => {
            d0[split] = data[4 * split];
            d1[split] = data[4 * split + 1];
        },
        3 => {
            d0[split] = data[4 * split];
            d1[split] = data[4 * split + 1];
            d2[split] = data[4 * split + 2];
        },
        _ => (),
    }

    [d0, d1, d2, d3]
}

pub fn bg4_split_together(data: &[u8]) -> Vec<u8> {
    let n = data.len();
    let split = n / 4;
    let rem = n % 4;
    // SAFETY: the loop below writes 4 bytes per iteration, one into each of
    // the four contiguous regions of `d` (sized so they partition all n
    // bytes), and the `match rem` block writes the final `rem` bytes; every
    // byte of `d` is written before return.
    let mut d = unsafe { alloc_uninit(n) };

    unsafe {
        let data = data.as_ptr();
        let d0 = d.as_mut_ptr();
        let d1 = d0.add(split + 1.min(rem));
        let d2 = d1.add(split + 1.min(rem.saturating_sub(1)));
        let d3 = d2.add(split + 1.min(rem.saturating_sub(2)));

        for i in 0..split {
            let idx = 4 * i;
            *d0.add(i) = *data.add(idx);
            *d1.add(i) = *data.add(idx + 1);
            *d2.add(i) = *data.add(idx + 2);
            *d3.add(i) = *data.add(idx + 3);
        }

        match rem {
            1 => {
                *d0.add(split) = *data.add(4 * split);
            },
            2 => {
                *d0.add(split) = *data.add(4 * split);
                *d1.add(split) = *data.add(4 * split + 1);
            },
            3 => {
                *d0.add(split) = *data.add(4 * split);
                *d1.add(split) = *data.add(4 * split + 1);
                *d2.add(split) = *data.add(4 * split + 2);
            },
            _ => (),
        }
    }

    d
}

#[inline]
pub fn bg4_split(data: &[u8]) -> Vec<u8> {
    bg4_split_together(data)
}

pub fn bg4_regroup_separate(groups: &[Vec<u8>]) -> Vec<u8> {
    let n = groups.iter().map(|g| g.len()).sum();
    let split = n / 4;
    let rem = n % 4;
    let g0 = &groups[0];
    let g1 = &groups[1];
    let g2 = &groups[2];
    let g3 = &groups[3];

    // SAFETY: the loop below writes 4 bytes per iteration (indices
    // `4*i..4*i+4`) covering `4*split` bytes, and the `match rem` block
    // writes the remaining `rem` bytes; together every byte of `data` up to
    // `n` is written before return.
    let mut data = unsafe { alloc_uninit(n) };

    for i in 0..split {
        data[4 * i] = g0[i];
        data[4 * i + 1] = g1[i];
        data[4 * i + 2] = g2[i];
        data[4 * i + 3] = g3[i];
    }

    match rem {
        1 => {
            data[4 * split] = g0[split];
        },
        2 => {
            data[4 * split] = g0[split];
            data[4 * split + 1] = g1[split];
        },
        3 => {
            data[4 * split] = g0[split];
            data[4 * split + 1] = g1[split];
            data[4 * split + 2] = g2[split];
        },
        _ => (),
    }

    data
}

/// Scalar 4-way byte interleave: writes `data[4*j..4*j+4]` from
/// `g0[j], g1[j], g2[j], g3[j]` for every `j` in `0..count`.
///
/// # Safety
/// `g0, g1, g2, g3` must each have at least `count` readable bytes starting
/// at the given pointer, and `data` must have at least `4 * count` writable
/// bytes starting at the given pointer.
#[inline]
unsafe fn regroup_together_scalar(
    g0: *const u8,
    g1: *const u8,
    g2: *const u8,
    g3: *const u8,
    data: *mut u8,
    count: usize,
) {
    for j in 0..count {
        unsafe {
            *data.add(4 * j) = *g0.add(j);
            *data.add(4 * j + 1) = *g1.add(j);
            *data.add(4 * j + 2) = *g2.add(j);
            *data.add(4 * j + 3) = *g3.add(j);
        }
    }
}

#[cfg(target_arch = "x86_64")]
mod regroup_together_x86 {
    use std::arch::x86_64::*;

    /// Interleaves 32-byte chunks from each of g0..g3 into `data` using AVX2,
    /// processing as many full 32-byte chunks as fit in `count` bytes.
    /// Returns the number of bytes consumed from each of g0..g3 (a multiple
    /// of 32).
    ///
    /// # Safety
    /// AVX2 must be available (caller must have checked
    /// `is_x86_feature_detected!("avx2")`). `g0, g1, g2, g3` must each have at
    /// least `count` readable bytes, and `data` must have at least
    /// `4 * count` writable bytes.
    #[target_feature(enable = "avx2")]
    pub unsafe fn regroup_avx2(
        g0: *const u8,
        g1: *const u8,
        g2: *const u8,
        g3: *const u8,
        data: *mut u8,
        count: usize,
    ) -> usize {
        let mut i = 0;
        while i + 32 <= count {
            unsafe {
                let a = _mm256_loadu_si256(g0.add(i) as *const __m256i);
                let b = _mm256_loadu_si256(g1.add(i) as *const __m256i);
                let c = _mm256_loadu_si256(g2.add(i) as *const __m256i);
                let d = _mm256_loadu_si256(g3.add(i) as *const __m256i);

                // Byte-interleave a,b and c,d independently within each
                // 128-bit lane, then 16-bit-interleave those pairs to get
                // 4-byte (a,b,c,d) groups, still split across the two lanes.
                let ab_lo = _mm256_unpacklo_epi8(a, b);
                let ab_hi = _mm256_unpackhi_epi8(a, b);
                let cd_lo = _mm256_unpacklo_epi8(c, d);
                let cd_hi = _mm256_unpackhi_epi8(c, d);

                let r0 = _mm256_unpacklo_epi16(ab_lo, cd_lo);
                let r1 = _mm256_unpackhi_epi16(ab_lo, cd_lo);
                let r2 = _mm256_unpacklo_epi16(ab_hi, cd_hi);
                let r3 = _mm256_unpackhi_epi16(ab_hi, cd_hi);

                // r0..r3 each hold two independently-computed 16-byte lanes
                // (low lane from source bytes 0..16, high lane from source
                // bytes 16..32) that are not contiguous in the output.
                // Re-pair lanes across registers so each store is contiguous.
                let store0 = _mm256_permute2x128_si256(r0, r1, 0x20);
                let store1 = _mm256_permute2x128_si256(r2, r3, 0x20);
                let store2 = _mm256_permute2x128_si256(r0, r1, 0x31);
                let store3 = _mm256_permute2x128_si256(r2, r3, 0x31);

                let out = data.add(4 * i) as *mut __m256i;
                _mm256_storeu_si256(out, store0);
                _mm256_storeu_si256(out.add(1), store1);
                _mm256_storeu_si256(out.add(2), store2);
                _mm256_storeu_si256(out.add(3), store3);
            }
            i += 32;
        }
        i
    }

    /// Same as [`regroup_avx2`] but using SSE2 (baseline on all x86_64 CPUs,
    /// so no runtime detection is required) over 16-byte chunks.
    ///
    /// # Safety
    /// Same requirements as [`regroup_avx2`].
    #[target_feature(enable = "sse2")]
    pub unsafe fn regroup_sse2(
        g0: *const u8,
        g1: *const u8,
        g2: *const u8,
        g3: *const u8,
        data: *mut u8,
        count: usize,
    ) -> usize {
        let mut i = 0;
        while i + 16 <= count {
            unsafe {
                let a = _mm_loadu_si128(g0.add(i) as *const __m128i);
                let b = _mm_loadu_si128(g1.add(i) as *const __m128i);
                let c = _mm_loadu_si128(g2.add(i) as *const __m128i);
                let d = _mm_loadu_si128(g3.add(i) as *const __m128i);

                let ab_lo = _mm_unpacklo_epi8(a, b);
                let ab_hi = _mm_unpackhi_epi8(a, b);
                let cd_lo = _mm_unpacklo_epi8(c, d);
                let cd_hi = _mm_unpackhi_epi8(c, d);

                // A single 128-bit lane, so no cross-lane repairing needed:
                // r0..r3 are already contiguous in output order.
                let r0 = _mm_unpacklo_epi16(ab_lo, cd_lo);
                let r1 = _mm_unpackhi_epi16(ab_lo, cd_lo);
                let r2 = _mm_unpacklo_epi16(ab_hi, cd_hi);
                let r3 = _mm_unpackhi_epi16(ab_hi, cd_hi);

                let out = data.add(4 * i) as *mut __m128i;
                _mm_storeu_si128(out, r0);
                _mm_storeu_si128(out.add(1), r1);
                _mm_storeu_si128(out.add(2), r2);
                _mm_storeu_si128(out.add(3), r3);
            }
            i += 16;
        }
        i
    }
}

/// Dispatches to the best available vectorized 4-way byte interleave, falling
/// back to the scalar loop for any remainder and on architectures without a
/// vectorized path (e.g. wasm32, or aarch64 where we don't yet have a NEON
/// implementation).
///
/// # Safety
/// `g0, g1, g2, g3` must each have at least `split` readable bytes, and
/// `data` must have at least `4 * split` writable bytes.
#[inline]
unsafe fn regroup_together_dispatch(
    g0: *const u8,
    g1: *const u8,
    g2: *const u8,
    g3: *const u8,
    data: *mut u8,
    split: usize,
) {
    #[allow(unused_mut)]
    let mut processed = 0usize;

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            processed = unsafe { regroup_together_x86::regroup_avx2(g0, g1, g2, g3, data, split) };
        }
        let remaining = split - processed;
        if remaining >= 16 {
            processed += unsafe {
                regroup_together_x86::regroup_sse2(
                    g0.add(processed),
                    g1.add(processed),
                    g2.add(processed),
                    g3.add(processed),
                    data.add(4 * processed),
                    remaining,
                )
            };
        }
    }

    unsafe {
        regroup_together_scalar(
            g0.add(processed),
            g1.add(processed),
            g2.add(processed),
            g3.add(processed),
            data.add(4 * processed),
            split - processed,
        );
    }
}

pub fn bg4_regroup_together(g: &[u8]) -> Vec<u8> {
    let n = g.len();
    let split = n / 4;
    let rem = n % 4;

    // SAFETY: `regroup_together_dispatch` writes all `4 * split` interleaved
    // bytes (via SIMD and/or scalar fallback), and the `match rem` block
    // below writes the final `rem` bytes; together every byte of `data` is
    // written before return.
    let mut data = unsafe { alloc_uninit(n) };

    unsafe {
        let data_ptr = data.as_mut_ptr();
        let g0 = g.as_ptr();
        let g1 = g0.add(split + 1.min(rem));
        let g2 = g1.add(split + 1.min(rem.saturating_sub(1)));
        let g3 = g2.add(split + 1.min(rem.saturating_sub(2)));

        regroup_together_dispatch(g0, g1, g2, g3, data_ptr, split);

        match rem {
            1 => {
                *data_ptr.add(4 * split) = *g0.add(split);
            },
            2 => {
                *data_ptr.add(4 * split) = *g0.add(split);
                *data_ptr.add(4 * split + 1) = *g1.add(split);
            },
            3 => {
                *data_ptr.add(4 * split) = *g0.add(split);
                *data_ptr.add(4 * split + 1) = *g1.add(split);
                *data_ptr.add(4 * split + 2) = *g2.add(split);
            },
            _ => (),
        }
    }

    data
}

pub fn bg4_regroup_together_combined_write_4(g: &[u8]) -> Vec<u8> {
    let n = g.len();
    let split = n / 4;
    let rem = n % 4;

    // SAFETY: the loop below writes 4 bytes per iteration (`4*split` bytes
    // total) via `copy_nonoverlapping`, and the `match rem` block writes the
    // remaining `rem` bytes; together every byte of `data` is written before
    // return.
    let mut data = unsafe { alloc_uninit(n) };

    unsafe {
        let d_ptr = data.as_mut_ptr();
        let g0 = g.as_ptr();
        let g1 = g0.add(split + 1.min(rem));
        let g2 = g1.add(split + 1.min(rem.saturating_sub(1)));
        let g3 = g2.add(split + 1.min(rem.saturating_sub(2)));

        for i in 0..split {
            let fourbytes = [*g0.add(i), *g1.add(i), *g2.add(i), *g3.add(i)];
            copy_nonoverlapping(&fourbytes as *const u8, d_ptr.add(4 * i), 4);
        }

        match rem {
            1 => {
                data[4 * split] = *g0.add(split);
            },
            2 => {
                data[4 * split] = *g0.add(split);
                data[4 * split + 1] = *g1.add(split);
            },
            3 => {
                data[4 * split] = *g0.add(split);
                data[4 * split + 1] = *g1.add(split);
                data[4 * split + 2] = *g2.add(split);
            },
            _ => (),
        }
    }

    data
}

pub fn bg4_regroup_together_combined_write_8(g: &[u8]) -> Vec<u8> {
    let n = g.len();
    let split = n / 4;
    let rem = n % 4;

    // SAFETY: the paired loop below writes 8 bytes per iteration covering
    // whole quadruple-pairs, the odd-`split` branch writes the one leftover
    // quadruple, and the `match rem` block writes the final `rem` bytes;
    // together every byte of `data` is written before return.
    let mut data = unsafe { alloc_uninit(n) };

    unsafe {
        let d_ptr = data.as_mut_ptr();
        let g0 = g.as_ptr();
        let g1 = g0.add(split + 1.min(rem));
        let g2 = g1.add(split + 1.min(rem.saturating_sub(1)));
        let g3 = g2.add(split + 1.min(rem.saturating_sub(2)));

        for i in 0..split / 2 {
            let j = i * 2;
            let k = j + 1;
            let eightbytes = [
                *g0.add(j),
                *g1.add(j),
                *g2.add(j),
                *g3.add(j),
                *g0.add(k),
                *g1.add(k),
                *g2.add(k),
                *g3.add(k),
            ];
            copy_nonoverlapping(&eightbytes as *const u8, d_ptr.add(8 * i), 8);
        }

        if !split.is_multiple_of(2) {
            let i = split - 1;
            let fourbytes = [*g0.add(i), *g1.add(i), *g2.add(i), *g3.add(i)];
            data[4 * i..4 * i + 4].copy_from_slice(&fourbytes[..]);
        }

        match rem {
            1 => {
                data[4 * split] = *g0.add(split);
            },
            2 => {
                data[4 * split] = *g0.add(split);
                data[4 * split + 1] = *g1.add(split);
            },
            3 => {
                data[4 * split] = *g0.add(split);
                data[4 * split + 1] = *g1.add(split);
                data[4 * split + 2] = *g2.add(split);
            },
            _ => (),
        }
    }

    data
}

#[inline]
pub fn bg4_regroup(g: &[u8]) -> Vec<u8> {
    bg4_regroup_together(g)
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;

    #[test]
    fn test_split_regroup_separate() {
        let mut rng = rand::rng();

        for n in [64 * 1024, 64 * 1024 - 53, 64 * 1024 + 135] {
            let data: Vec<_> = (0..n).map(|_| rng.random_range(0..255)).collect();
            let groups = bg4_split_separate(&data);
            let regrouped = bg4_regroup_separate(&groups);

            assert_eq!(regrouped, data);
        }
    }

    #[test]
    fn test_split_regroup_together() {
        let mut rng = rand::rng();

        for n in [64 * 1024, 64 * 1024 - 53, 64 * 1024 + 135] {
            let data: Vec<_> = (0..n).map(|_| rng.random_range(0..255)).collect();
            let groups = bg4_split_together(&data);

            let regrouped = bg4_regroup_together(&groups);
            assert_eq!(regrouped, data);

            let regrouped = bg4_regroup_together_combined_write_4(&groups);
            assert_eq!(regrouped, data);

            let regrouped = bg4_regroup_together_combined_write_8(&groups);
            assert_eq!(regrouped, data);
        }
    }

    /// The SIMD-dispatching `bg4_regroup_together` must produce byte-for-byte
    /// identical output to the pure scalar loop for every length in
    /// `0..=1024` (this exhaustively covers every `rem in 0..4` and every
    /// small-`split` edge case where SIMD chunk boundaries interact with the
    /// scalar remainder), plus a couple of large buffers to exercise the
    /// AVX2/SSE2 chunk loops themselves.
    #[test]
    fn test_regroup_together_simd_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(0xB6_4A55_C0DE);

        let mut lengths: Vec<usize> = (0..=1024).collect();
        lengths.push(64 * 1024);
        lengths.push(1024 * 1024);

        for n in lengths {
            let g: Vec<u8> = (0..n).map(|_| rng.random_range(0..255)).collect();

            let split = n / 4;
            let rem = n % 4;

            let simd_result = bg4_regroup_together(&g);

            let mut scalar_result = vec![0u8; n];
            unsafe {
                let data_ptr = scalar_result.as_mut_ptr();
                let g0 = g.as_ptr();
                let g1 = g0.add(split + 1.min(rem));
                let g2 = g1.add(split + 1.min(rem.saturating_sub(1)));
                let g3 = g2.add(split + 1.min(rem.saturating_sub(2)));

                regroup_together_scalar(g0, g1, g2, g3, data_ptr, split);

                match rem {
                    1 => {
                        *data_ptr.add(4 * split) = *g0.add(split);
                    },
                    2 => {
                        *data_ptr.add(4 * split) = *g0.add(split);
                        *data_ptr.add(4 * split + 1) = *g1.add(split);
                    },
                    3 => {
                        *data_ptr.add(4 * split) = *g0.add(split);
                        *data_ptr.add(4 * split + 1) = *g1.add(split);
                        *data_ptr.add(4 * split + 2) = *g2.add(split);
                    },
                    _ => (),
                }
            }

            assert_eq!(simd_result, scalar_result, "mismatch at n={n}");
        }
    }
}
