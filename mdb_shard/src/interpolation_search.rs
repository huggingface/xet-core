use std::cmp::Ordering;
use std::io::{Read, Seek, SeekFrom};
use std::mem::size_of;

use utils::serialization_utils::*;

/// Performs an interpolation search on a block of sorted, possibly multile
/// u64 hash keys with a simple payload.
///
/// read_start: The byte offset in the reader that gives the start of the data.
/// num_entries: the number of key, value pairs present.
/// key: the key to search for
/// read_value_function : A function that deserializes the value.  
///
/// result : A mutable slice into which the results get written.  If the number
/// of values found equals the length of this buffer at the end, then more values may
/// be present.  
///
/// Returns the number of values found.  
pub fn search_on_sorted_u64s<
    Value: Default + Copy + std::fmt::Debug,
    R: Read + Seek,
    ReadValueFunction: Fn(&mut R) -> Result<Value, std::io::Error>,
>(
    reader: &mut R,
    read_start: u64,
    num_entries: u64,
    key: u64,
    read_value_function: ReadValueFunction,
    result: &mut [Value],
) -> Result<usize, std::io::Error> {
    //
    // A few things make this interesting:
    //
    // 1. We assume an even distribution over keys, allowing us to do interpolation search.
    //
    // 2. Multiple values may be present.  Therefore, it is not enough to find a key; rather, we need to be certain
    //    we've found all of them.
    //
    // 2. Seeks are more expensive than forward reads. We assume it's fast to read values sequentially. Therefore, once
    //    the candidate window is small enough, we just read all the values in the window.
    //
    // This is the size of the window where doing a sequential read from this point is assumed to be equivalent in speed
    // to a seek, then do a read.  If the next point is within READ_WINDOW_SIZE entries of the current point, then
    // just do a continuous read.
    const READ_WINDOW_SIZE: u64 = 256;
    const EXPECTED_MAX_NUM_DUPLICATES: u64 = 4;

    let pair_size: u64 = (size_of::<Value>() + size_of::<u64>()) as u64;

    // Where we'll write the next result.
    let mut result_write_idx = 0;

    // Make it bullet proof against corner cases.
    if result.is_empty() {
        return Ok(0);
    }

    let mut write_result = |value: Value| {
        // Only record it if there is room.
        if result_write_idx < result.len() {
            result[result_write_idx] = value;
            result_write_idx += 1;
        }
    };

    // Now, to avoid reading the ends with a seek, to make the interpolation behave we actually pretend there is 0 entry
    // key in the first position and a max valued key in the last position.  These will never get read, but they will
    // be used to calculate the interpolation.
    let mut lo = 0;
    let mut lo_key = 0;
    let mut hi = num_entries + 1; // Index of last entry, with 2 ghost entries to denote the beginning and end.
    let mut hi_key = u64::MAX;

    // Function to query the probe location.
    let compute_probe_location = |lo: u64, lo_key: u64, hi: u64, hi_key: u64| {
        (lo + ((key - lo_key) as f64 / (hi_key - lo_key) as f64 * (hi - lo) as f64).floor() as u64)
            .max(lo + 1)
            .min(hi - 1)
    };

    let mut probe_index = compute_probe_location(lo, lo_key, hi, hi_key);

    while lo + READ_WINDOW_SIZE < hi {
        // The minus 1 is to handle the shift because of making lo_key == 0
        reader.seek(SeekFrom::Start(read_start + (probe_index - 1) * pair_size))?;

        // First, probe the first entry.
        let probe_key = read_u64(reader)?;

        match key.cmp(&probe_key) {
            Ordering::Less => {
                hi = probe_index;
                hi_key = probe_key;

                // Recompute the probe index for the next go.
                let candidate_probe_index = compute_probe_location(lo, lo_key, hi, hi_key);

                // Make sure the new probe index is at least READ_WINDOW_SIZE away to make this efficient.
                if candidate_probe_index + READ_WINDOW_SIZE > probe_index {
                    // Safely set this to the current position minus READ_WINDOW_SIZE so the next probe
                    // likely just reads in all the values between that and the current one if applicable.
                    let jump_amount = (READ_WINDOW_SIZE).min(probe_index - (lo + 1));
                    probe_index -= jump_amount;
                } else {
                    probe_index = candidate_probe_index;
                }
            },
            Ordering::Equal => {
                // Read out this value.
                write_result(read_value_function(reader)?);

                // Now, read ahead until we've filled all the possible duplicates from this range..
                for _ in (probe_index + 1)..hi {
                    if read_u64(reader)? != key {
                        break;
                    }
                    write_result(read_value_function(reader)?);
                }

                hi = probe_index;
                hi_key = probe_key;

                // Since we know we're part of a block of keys,
                // and we're assuming that very few keys are actually the same (but need to account
                // for all possibilities), then set the probe index to be just a bit before this one.

                let jump_amount = (EXPECTED_MAX_NUM_DUPLICATES).min(probe_index - (lo + 1));
                probe_index -= jump_amount;
            },
            Ordering::Greater => {
                lo = probe_index;
                lo_key = probe_key;

                // Repeatedly test this new candidate probe index.
                let candidate_probe_index = compute_probe_location(lo, lo_key, hi, hi_key);

                // Jump at least READ_WINDOW_SIZE away
                if candidate_probe_index - probe_index <= READ_WINDOW_SIZE {
                    probe_index = (lo + READ_WINDOW_SIZE).min(hi - 1);
                } else {
                    probe_index = candidate_probe_index;
                }
            },
        };
    }

    // Seek to read everything in the (lo, hi) range.
    reader.seek(SeekFrom::Start(read_start + lo * pair_size))?;

    while lo + 1 < hi {
        let (probe_key, probe_value) = (read_u64(reader)?, read_value_function(reader)?);
        lo += 1;

        match key.cmp(&probe_key) {
            Ordering::Less => {
                // We're done.
                break;
            },
            Ordering::Equal => {
                write_result(probe_value);
            },
            Ordering::Greater => {
                // Keep going
                continue;
            },
        }
    }

    Ok(result_write_idx)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;

    use rand::prelude::*;

    use super::*;

    fn test_interpolation_search(keys: &[u64], alt_query_keys: &[u64]) -> Result<(), std::io::Error> {
        let mut values: Vec<(u64, u64)> = keys.iter().enumerate().map(|(i, k)| (*k, 100 + i as u64)).collect();
        values.sort_unstable();

        // First, serialize out the values, and build a
        let data_start = 0;
        let mut data = vec![0xFFu8; data_start]; // Start off with some.

        let mut all_values = HashMap::<u64, Vec<u64>>::new();

        for (k, v) in values.iter() {
            all_values.entry(*k).or_default().push(*v);
            write_u64(&mut data, *k)?;
            write_u64(&mut data, *v)?;
        }

        // Now, loop through all the values, running the query function and checking if it works.
        let mut dest_values = Vec::<u64>::new();

        for (k, v) in all_values {
            dest_values.clear();
            dest_values.resize(v.len() + 1, 0);

            let n_items_found = search_on_sorted_u64s(
                &mut Cursor::new(&data),
                data_start as u64,
                values.len() as u64,
                k,
                read_u64::<Cursor<&Vec<u8>>>,
                &mut dest_values,
            )?;

            // Make sure we found the correct amount.
            assert_eq!(n_items_found, v.len());

            // Clip off the last one, unused.
            dest_values.resize(v.len(), 0);

            // Sort it so we can do a proper comparison
            dest_values.sort_unstable();

            assert_eq!(dest_values, v);
        }

        // Now test all the other values given that are not in the map.
        dest_values.resize(8, 0);

        for k in alt_query_keys {
            let n_items_found = search_on_sorted_u64s(
                &mut Cursor::new(&data),
                data_start as u64,
                values.len() as u64,
                *k,
                read_u64::<Cursor<&Vec<u8>>>,
                &mut dest_values,
            )?;
            assert_eq!(n_items_found, 0);
        }

        Ok(())
    }

    #[test]
    fn test_sanity_1() -> Result<(), std::io::Error> {
        test_interpolation_search(&[1], &[])
    }
    #[test]
    fn test_sanity_2() -> Result<(), std::io::Error> {
        test_interpolation_search(&[1, 3], &[0, 2, 4, 6, 8])
    }

    #[test]
    fn test_empty() -> Result<(), std::io::Error> {
        test_interpolation_search(&[], &[1, 2, 4, 6, 8, u64::MAX])
    }

    #[test]
    fn test_all_zeros() -> Result<(), std::io::Error> {
        test_interpolation_search(&[0; 1], &[u64::MAX, 1, 2, 4, 6, 8])
    }

    #[test]
    fn test_all_max() -> Result<(), std::io::Error> {
        test_interpolation_search(&vec![u64::MAX; 100], &[0, 1, 2, 4, 6, 8])
    }

    #[test]
    fn test_large_random_unique() -> Result<(), std::io::Error> {
        let mut v = Vec::<u64>::new();
        let mut rng = StdRng::seed_from_u64(0);

        for _ in 0..100 {
            v.push(rng.random());
        }

        test_interpolation_search(&v[..], &[0, u64::MAX])
    }

    #[test]
    fn test_large_random_multiples() -> Result<(), std::io::Error> {
        let mut v = Vec::<u64>::new();
        let mut rng = StdRng::seed_from_u64(0);

        for _ in 0..200 {
            let len = rng.random_range(1..8);
            let x: u64 = rng.random();
            v.resize(v.len() + len, x);
        }

        test_interpolation_search(&v[..], &[0, u64::MAX])
    }
}
