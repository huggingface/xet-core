use std::collections::HashSet;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use xet_data::deduplication::constants::TARGET_CHUNK_SIZE;
use xet_data::deduplication::{Chunk, Chunker, next_stable_chunk_boundary};
use xet_runtime::test_set_constants;

test_set_constants! {
    TARGET_CHUNK_SIZE = 1024;
}

fn make_random_data(seed: u64, len: usize) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut data = vec![0u8; len];
    rng.fill(&mut data[..]);
    data
}

fn chunk_data(data: &[u8]) -> Vec<Chunk> {
    let mut chunker = Chunker::default();
    chunker.next_block(data, true)
}

fn get_chunk_boundaries(chunks: &[Chunk]) -> Vec<usize> {
    chunks
        .iter()
        .scan(0usize, |pos, c| {
            *pos += c.data.len();
            Some(*pos)
        })
        .collect()
}

fn verify_alignment(
    original_boundaries: &[usize],
    new_boundaries: &[usize],
    stable: usize,
    file_size: usize,
    starting_position: usize,
    mutation_seed: u64,
) {
    let orig_set: HashSet<usize> = original_boundaries.iter().copied().collect();
    let new_set: HashSet<usize> = new_boundaries.iter().copied().collect();

    for &oc in original_boundaries {
        if oc >= stable && oc < file_size {
            assert!(
                new_set.contains(&oc),
                "Original chunk boundary {oc} (>= stable {stable}) missing from new chunk boundaries. \
                 starting_position={starting_position}, mutation_seed={mutation_seed}"
            );
        }
    }

    for &nc in new_boundaries {
        if nc >= stable && nc < file_size {
            assert!(
                orig_set.contains(&nc),
                "New chunk boundary {nc} (>= stable {stable}) not in original chunk boundaries. \
                 starting_position={starting_position}, mutation_seed={mutation_seed}"
            );
        }
    }
}

/// For a given data buffer, exercise `next_stable_chunk_boundary` at random
/// starting positions across the full data range with random mutations.
fn stress_test_stable_chunk_boundaries(data: &[u8], seed: u64, num_positions: usize, num_mutations: u64) {
    let file_size = data.len();
    let chunks = chunk_data(data);
    let chunk_boundaries = get_chunk_boundaries(&chunks);

    assert!(
        chunk_boundaries.len() > 10,
        "Need enough chunks for meaningful testing, got {}",
        chunk_boundaries.len()
    );

    let mut rng = StdRng::seed_from_u64(seed);
    let mut tested_stable = 0u64;

    for trial in 0..num_positions {
        let starting_position = rng.random_range(1..file_size);

        let stable = match next_stable_chunk_boundary(starting_position, &chunk_boundaries) {
            Some(s) => s,
            None => continue,
        };

        assert!(
            chunk_boundaries.contains(&stable),
            "Stable chunk boundary {stable} is not a member of original chunk boundaries"
        );
        assert!(
            stable >= starting_position,
            "Stable chunk boundary {stable} must be at or after starting_position {starting_position}"
        );

        tested_stable += 1;

        for mutation_seed in 0..num_mutations {
            let combined_seed = (trial as u64) * 10000 + mutation_seed + 1;
            let mut modified = data.to_vec();
            let mut mrng = StdRng::seed_from_u64(combined_seed);
            mrng.fill(&mut modified[..starting_position]);

            let new_chunks = chunk_data(&modified);
            let new_boundaries = get_chunk_boundaries(&new_chunks);

            verify_alignment(&chunk_boundaries, &new_boundaries, stable, file_size, starting_position, combined_seed);
        }
    }

    let min_expected = (num_positions / 4).max(1);
    assert!(
        tested_stable >= min_expected as u64,
        "Too few starting positions had stable chunk boundaries: {tested_stable} (expected >= {min_expected})"
    );
}

#[test]
fn test_stable_chunk_boundary_edge_cases() {
    let data = make_random_data(42, 50_000);
    let chunks = chunk_data(&data);
    let chunk_boundaries = get_chunk_boundaries(&chunks);

    // starting_position at 0: should still return a valid point
    let stable_0 = next_stable_chunk_boundary(0, &chunk_boundaries);
    if let Some(s) = stable_0 {
        assert!(chunk_boundaries.contains(&s));
    }

    // starting_position past all chunk boundaries: should return None
    let past_end = *chunk_boundaries.last().unwrap() + 1;
    assert!(next_stable_chunk_boundary(past_end, &chunk_boundaries).is_none());

    // starting_position near end with too few remaining points
    if chunk_boundaries.len() >= 2 {
        let near_end = chunk_boundaries[chunk_boundaries.len() - 2];
        assert!(next_stable_chunk_boundary(near_end + 1, &chunk_boundaries).is_none());
    }

    // Degenerate inputs
    assert!(next_stable_chunk_boundary(0, &[]).is_none());
    assert!(next_stable_chunk_boundary(0, &[100]).is_none());
    assert!(next_stable_chunk_boundary(0, &[100, 200]).is_none());
}

#[test]
fn test_stable_chunk_boundary_with_constant_data() {
    // Constant data produces max-chunk-sized chunks (forced boundaries).
    // With target=1024: max_chunk=2048, min_chunk=128, so max-min=1920.
    // Forced boundaries at size 2048 fail the upper bound check of < 1920.
    let data = vec![0u8; 50_000];
    let chunks = chunk_data(&data);
    let chunk_boundaries = get_chunk_boundaries(&chunks);

    let stable = next_stable_chunk_boundary(0, &chunk_boundaries);
    assert!(stable.is_none(), "Constant data should have no stable chunk boundary (all forced cuts)");
}

#[test]
fn test_stable_chunk_boundary_smoke_stress() {
    let data = make_random_data(42, 50_000);
    stress_test_stable_chunk_boundaries(&data, 42, 5, 5);
}

#[test]
fn test_stable_chunk_boundary_smoke_varied_seeds() {
    for seed in [1, 7, 255] {
        let data = make_random_data(seed, 50_000);
        stress_test_stable_chunk_boundaries(&data, seed + 77, 5, 5);
    }
}

#[cfg(not(feature = "smoke-test"))]
#[test]
fn test_stable_chunk_boundary_stress() {
    let data = make_random_data(42, 256_000);
    stress_test_stable_chunk_boundaries(&data, 42, 100, 20);
}

#[cfg(not(feature = "smoke-test"))]
#[test]
fn test_stable_chunk_boundary_stress_varied_seeds() {
    for seed in [1, 7, 13, 100, 255, 1024, 42424, 999999] {
        let data = make_random_data(seed, 100_000);
        stress_test_stable_chunk_boundaries(&data, seed + 77, 50, 20);
    }
}

#[cfg(not(feature = "smoke-test"))]
#[test]
fn test_stable_chunk_boundary_mutation_types() {
    let data = make_random_data(42, 100_000);
    let chunks = chunk_data(&data);
    let chunk_boundaries = get_chunk_boundaries(&chunks);
    let file_size = data.len();

    let mid_idx = chunk_boundaries.len() / 2;
    let starting_position = chunk_boundaries[mid_idx];

    let stable = match next_stable_chunk_boundary(starting_position, &chunk_boundaries) {
        Some(s) => s,
        None => return,
    };

    // Zero-fill
    {
        let mut modified = data.to_vec();
        modified[..starting_position].fill(0);
        let new_boundaries = get_chunk_boundaries(&chunk_data(&modified));
        verify_alignment(&chunk_boundaries, &new_boundaries, stable, file_size, starting_position, 0);
    }

    // 0xFF-fill
    {
        let mut modified = data.to_vec();
        modified[..starting_position].fill(0xFF);
        let new_boundaries = get_chunk_boundaries(&chunk_data(&modified));
        verify_alignment(&chunk_boundaries, &new_boundaries, stable, file_size, starting_position, 1);
    }

    // Reverse the prefix
    {
        let mut modified = data.to_vec();
        modified[..starting_position].reverse();
        let new_boundaries = get_chunk_boundaries(&chunk_data(&modified));
        verify_alignment(&chunk_boundaries, &new_boundaries, stable, file_size, starting_position, 2);
    }

    // XOR with a pattern
    {
        let mut modified = data.to_vec();
        for (i, byte) in modified[..starting_position].iter_mut().enumerate() {
            *byte ^= (i & 0xFF) as u8;
        }
        let new_boundaries = get_chunk_boundaries(&chunk_data(&modified));
        verify_alignment(&chunk_boundaries, &new_boundaries, stable, file_size, starting_position, 3);
    }

    // Many different random fills
    for seed in 0..200 {
        let mut modified = data.to_vec();
        let mut rng = StdRng::seed_from_u64(seed + 5000);
        rng.fill(&mut modified[..starting_position]);
        let new_boundaries = get_chunk_boundaries(&chunk_data(&modified));
        verify_alignment(&chunk_boundaries, &new_boundaries, stable, file_size, starting_position, seed + 5000);
    }
}

#[cfg(not(feature = "smoke-test"))]
#[test]
fn test_stable_chunk_boundary_is_tight() {
    // Verify the stable chunk boundary is actually needed: the chunk boundary
    // just before stable should NOT always be stable (there should exist some
    // mutation that breaks it).
    let data = make_random_data(42, 256_000);
    let chunks = chunk_data(&data);
    let chunk_boundaries = get_chunk_boundaries(&chunks);

    let mut found_non_stable_predecessor = false;

    for &starting_position in chunk_boundaries.iter().take(chunk_boundaries.len() / 2) {
        if starting_position == 0 {
            continue;
        }

        let stable = match next_stable_chunk_boundary(starting_position, &chunk_boundaries) {
            Some(s) => s,
            None => continue,
        };

        let stable_idx = chunk_boundaries.iter().position(|&x| x == stable).unwrap();
        if stable_idx == 0 {
            continue;
        }
        let predecessor = chunk_boundaries[stable_idx - 1];
        if predecessor <= starting_position {
            continue;
        }

        let orig_set: HashSet<usize> = chunk_boundaries.iter().copied().collect();
        for seed in 0..200 {
            let mut modified = data.to_vec();
            let mut rng = StdRng::seed_from_u64(seed + 90000);
            rng.fill(&mut modified[..starting_position]);
            let new_boundaries = get_chunk_boundaries(&chunk_data(&modified));
            let new_set: HashSet<usize> = new_boundaries.iter().copied().collect();

            if !new_set.contains(&predecessor)
                || new_boundaries
                    .iter()
                    .any(|&nc| nc >= predecessor && nc < stable && !orig_set.contains(&nc))
            {
                found_non_stable_predecessor = true;
                break;
            }
        }

        if found_non_stable_predecessor {
            break;
        }
    }

    assert!(
        found_non_stable_predecessor,
        "Could not find any case where the predecessor of a stable chunk boundary was actually unstable. \
         This suggests the stability condition may be too conservative."
    );
}
