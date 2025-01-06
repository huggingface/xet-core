use std::mem::size_of;
use std::time::Instant;

use cas_object::*;
use half::prelude::*;
use rand::Rng;

fn main() {
    let mut rng = rand::thread_rng();

    let n = 64 * 1024; // 64 KiB data
    let all_zeros = vec![0u8; n];
    let all_ones = vec![1u8; n];
    let all_0xff = vec![0xFF; n];
    let random_u8s: Vec<_> = (0..n).map(|_| rng.gen_range(0..255)).collect();
    let random_f32s_ng1_1: Vec<_> = (0..n / size_of::<f32>())
        .map(|_| rng.gen_range(-1.0f32..=1.0))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();
    let random_f32s_0_2: Vec<_> = (0..n / size_of::<f32>())
        .map(|_| rng.gen_range(0f32..=2.0))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();
    let random_f64s_ng1_1: Vec<_> = (0..n / size_of::<f64>())
        .map(|_| rng.gen_range(-1.0f64..=1.0))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();
    let random_f64s_0_2: Vec<_> = (0..n / size_of::<f64>())
        .map(|_| rng.gen_range(0f64..=2.0))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();

    // f16, a.k.a binary16 format: sign (1 bit), exponent (5 bit), mantissa (10 bit)
    let random_f16s_ng1_1: Vec<_> = (0..n / size_of::<f16>())
        .map(|_| f16::from_f32(rng.gen_range(-1.0f32..=1.0)))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();
    let random_f16s_0_2: Vec<_> = (0..n / size_of::<f16>())
        .map(|_| f16::from_f32(rng.gen_range(0f32..=2.0)))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();

    // bf16 format: sign (1 bit), exponent (8 bit), mantissa (7 bit)
    let random_bf16s_ng1_1: Vec<_> = (0..n / size_of::<bf16>())
        .map(|_| bf16::from_f32(rng.gen_range(-1.0f32..=1.0)))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();
    let random_bf16s_0_2: Vec<_> = (0..n / size_of::<bf16>())
        .map(|_| bf16::from_f32(rng.gen_range(0f32..=2.0)))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();

    // Results on Apple M2 Max
    let dataset = [
        all_zeros,          // bg4_lz4 speed: 2162.37 MB/s, 2114.99 MB/s; lz4 speed: 8214.33 MB/s, 29662.77 MB/s
        all_ones,           // bg4_lz4 speed: 2177.82 MB/s, 2122.07 MB/s; lz4 speed: 8137.31 MB/s, 27782.15 MB/s
        all_0xff,           // bg4_lz4 speed: 2191.94 MB/s, 2134.31 MB/s; lz4 speed: 8553.77 MB/s, 28476.06 MB/s
        random_u8s,         // bg4_lz4 speed: 1856.07 MB/s, 2158.44 MB/s; lz4 speed: 7468.16 MB/s, 15746.10 MB/s
        random_f32s_ng1_1,  // bg4_lz4 speed: 1050.41 MB/s, 1783.43 MB/s; lz4 speed: 8560.56 MB/s, 21652.06 MB/s
        random_f32s_0_2,    // bg4_lz4 speed: 1108.68 MB/s, 1726.78 MB/s; lz4 speed: 8512.21 MB/s, 20749.23 MB/s
        random_f64s_ng1_1,  // bg4_lz4 speed: 1833.69 MB/s, 2149.29 MB/s; lz4 speed: 7462.55 MB/s, 15787.39 MB/s
        random_f64s_0_2,    // bg4_lz4 speed: 1802.49 MB/s, 2147.87 MB/s; lz4 speed: 7555.05 MB/s, 16551.80 MB/s
        random_f16s_ng1_1,  // bg4_lz4 speed: 1860.93 MB/s, 2159.01 MB/s; lz4 speed: 7503.76 MB/s, 15315.53 MB/s
        random_f16s_0_2,    // bg4_lz4 speed: 1749.98 MB/s, 2147.20 MB/s; lz4 speed: 7397.90 MB/s, 15638.39 MB/s
        random_bf16s_ng1_1, // bg4_lz4 speed: 619.52 MB/s, 1496.25 MB/s; lz4 speed: 8427.57 MB/s, 21442.34 MB/s
        random_bf16s_0_2,   // bg4_lz4 speed: 774.06 MB/s, 1421.09 MB/s; lz4 speed: 8118.71 MB/s, 22107.13 MB/s
    ];

    const ITER: usize = 10000;

    for mut data in dataset {
        let mut bg4_lz4_compress_time = 0.;
        let mut bg4_lz4_decompress_time = 0.;
        let mut lz4_compress_time = 0.;
        let mut lz4_decompress_time = 0.;

        for _ in 0..ITER {
            let s = Instant::now();
            let bg4_lz4_compressed = bg4_lz4_compress_from_slice(&data).unwrap();
            bg4_lz4_compress_time += s.elapsed().as_secs_f64();

            let s = Instant::now();
            let bg4_lz4_uncompressed = bg4_lz4_decompress_from_slice(&bg4_lz4_compressed).unwrap();
            bg4_lz4_decompress_time += s.elapsed().as_secs_f64();

            let s = Instant::now();
            let lz4_compressed = lz4_compress_from_slice(&data).unwrap();
            lz4_compress_time += s.elapsed().as_secs_f64();

            let s = Instant::now();
            let lz4_uncompressed = lz4_decompress_from_slice(&lz4_compressed).unwrap();
            lz4_decompress_time += s.elapsed().as_secs_f64();

            data[0] = data[0].wrapping_mul(5).wrapping_add(13);
        }

        print!(
            "bg4_lz4 speed: compress at {:.2} MB/s, decompress at {:.2} MB/s; ",
            data.len() as f64 / 1e6 / bg4_lz4_compress_time * ITER as f64,
            data.len() as f64 / 1e6 / bg4_lz4_decompress_time * ITER as f64
        );

        println!(
            "lz4 speed: compress at {:.2} MB/s, decompress at {:.2} MB/s",
            data.len() as f64 / 1e6 / lz4_compress_time * ITER as f64,
            data.len() as f64 / 1e6 / lz4_decompress_time * ITER as f64
        );
    }
}
