use std::mem::size_of;
use std::time::Instant;

use cas_object::*;
use half::prelude::*;
use rand::Rng;

// Benchmark results on Apple M2 Max

// Compression and decompression speed in MB/s
// data         bg4-lz4 comp    bg4-lz4 decomp  lz4 comp    lz4 decomp
// all_zeros    8736.63         11079.64        12159.1     27785.79
// all_ones     8788.91         11414.08        12185.59    26168.06
// all_0xff     8898.34         11642.8         12481.1     26841.34
// u8s          7622.01         6706.2          8430.54     20285.7
// f32s_ng1_1   1906.86         4182.3          8480.45     20350.98
// f32s_0_2     1916.65         3635.25         8298.3      20220.85
// f64s_ng1_1   7820.94         7358.91         8652.34     20094.45
// f64s_0_2     7489.42         7279.63         8346.99     17511.55
// f16s_ng1_1   7916.1          7360.7          8614.53     20263.14
// f16s_0_2     6869.91         7291.86         8446.52     20361.37
// bf16s_ng1_1  787.59          3012.11         7141.22     19580.02
// bf16s_0_2    1064.27         2592.15         6217.8      19285.87

// The percentage of split or regroup runtime out of total compression / decompression runtime

// data         split / total   regroup / total
// all_zeros    34%             46%
// all_ones     33%             47%
// all_0xff     34%             47%
// u8s          25%             61%
// f32s_ng1_1   7%              35%
// f32s_0_2     7%              29%
// f64s_ng1_1   23%             57%
// f64s_0_2     23%             57%
// f16s_ng1_1   24%             57%
// f16s_0_2     21%             56%
// bf16s_ng1_1  2%              19%
// bf16s_0_2    3%              17%

fn main() {
    let mut rng = rand::thread_rng();

    let n = 64 * 1024; // 64 KiB data
    let all_zeros = vec![0u8; n];
    let all_ones = vec![1u8; n];
    let all_0xff = vec![0xFF; n];
    let u8s: Vec<_> = (0..n).map(|_| rng.gen_range(0..255)).collect();
    let f32s_ng1_1: Vec<_> = (0..n / size_of::<f32>())
        .map(|_| rng.gen_range(-1.0f32..=1.0))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();
    let f32s_0_2: Vec<_> = (0..n / size_of::<f32>())
        .map(|_| rng.gen_range(0f32..=2.0))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();
    let f64s_ng1_1: Vec<_> = (0..n / size_of::<f64>())
        .map(|_| rng.gen_range(-1.0f64..=1.0))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();
    let f64s_0_2: Vec<_> = (0..n / size_of::<f64>())
        .map(|_| rng.gen_range(0f64..=2.0))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();

    // f16, a.k.a binary16 format: sign (1 bit), exponent (5 bit), mantissa (10 bit)
    let f16s_ng1_1: Vec<_> = (0..n / size_of::<f16>())
        .map(|_| f16::from_f32(rng.gen_range(-1.0f32..=1.0)))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();
    let f16s_0_2: Vec<_> = (0..n / size_of::<f16>())
        .map(|_| f16::from_f32(rng.gen_range(0f32..=2.0)))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();

    // bf16 format: sign (1 bit), exponent (8 bit), mantissa (7 bit)
    let bf16s_ng1_1: Vec<_> = (0..n / size_of::<bf16>())
        .map(|_| bf16::from_f32(rng.gen_range(-1.0f32..=1.0)))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();
    let bf16s_0_2: Vec<_> = (0..n / size_of::<bf16>())
        .map(|_| bf16::from_f32(rng.gen_range(0f32..=2.0)))
        .map(|f| f.to_le_bytes())
        .flatten()
        .collect();

    let dataset = [
        all_zeros,
        all_ones,
        all_0xff,
        u8s,
        f32s_ng1_1,
        f32s_0_2,
        f64s_ng1_1,
        f64s_0_2,
        f16s_ng1_1,
        f16s_0_2,
        bf16s_ng1_1,
        bf16s_0_2,
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
            let _ = bg4_lz4_decompress_from_slice(&bg4_lz4_compressed).unwrap();
            bg4_lz4_decompress_time += s.elapsed().as_secs_f64();

            let s = Instant::now();
            let lz4_compressed = lz4_compress_from_slice(&data).unwrap();
            lz4_compress_time += s.elapsed().as_secs_f64();

            let s = Instant::now();
            let _ = lz4_decompress_from_slice(&lz4_compressed).unwrap();
            lz4_decompress_time += s.elapsed().as_secs_f64();

            // Prevent compilers from optimizing away iterations.
            data[0] = data[0].wrapping_mul(5).wrapping_add(13);
        }

        print!(
            "bg4_lz4 speed: compress at {:.2} MB/s, decompress at {:.2} MB/s; ",
            data.len() as f64 / 1e6 / bg4_lz4_compress_time * ITER as f64,
            data.len() as f64 / 1e6 / bg4_lz4_decompress_time * ITER as f64
        );

        print!(
            "lz4 speed: compress at {:.2} MB/s, decompress at {:.2} MB/s; ",
            data.len() as f64 / 1e6 / lz4_compress_time * ITER as f64,
            data.len() as f64 / 1e6 / lz4_decompress_time * ITER as f64
        );

        unsafe {
            println!("{BG4_SPLIT_RUNTIME} s, {BG4_LZ4_COMPRESS_RUNTIME} s , {BG4_LZ4_DECOMPRESS_RUNTIME} s, {BG4_REGROUP_RUNTIME} s");
        }

        // For CSV exporting
        unsafe {
            eprintln!(
                "{:.2}, {:.2}, {:.2}, {:.2}, {}, {}, {}, {}",
                data.len() as f64 / 1e6 / bg4_lz4_compress_time * ITER as f64,
                data.len() as f64 / 1e6 / bg4_lz4_decompress_time * ITER as f64,
                data.len() as f64 / 1e6 / lz4_compress_time * ITER as f64,
                data.len() as f64 / 1e6 / lz4_decompress_time * ITER as f64,
                BG4_SPLIT_RUNTIME,
                BG4_LZ4_COMPRESS_RUNTIME,
                BG4_LZ4_DECOMPRESS_RUNTIME,
                BG4_REGROUP_RUNTIME
            );
        }

        unsafe {
            BG4_SPLIT_RUNTIME = 0.;
            BG4_LZ4_COMPRESS_RUNTIME = 0.;
            BG4_LZ4_DECOMPRESS_RUNTIME = 0.;
            BG4_REGROUP_RUNTIME = 0.;
        }
    }
}
