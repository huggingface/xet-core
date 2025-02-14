use cas_object::*;
use rand::Rng;
use std::time::Instant;

use crate::byte_grouping::bg2;
use crate::byte_grouping::bg4;

// Benchmark results on Apple M2 Max

// bg4_split_separate speed: 2738.11 MB/s
// bg4_regroup_separate speed: 2708.04 MB/s
// bg4_split_together speed: 24439.23 MB/s
// bg4_regroup_together speed: 32791.84 MB/s
// bg4_regroup_together_cw_4 speed: 33268.32 MB/s
// bg4_regroup_together_cw_8 speed: 13925.35 MB/s
// bg2_split speed: 30051.96 MB/s
// bg2_regroup speed: 36615.42 MB/s

fn main() {
    let mut rng = rand::thread_rng();

    let n = 64 * 1024 + 123; // 64 KiB data
    let random_u8s: Vec<_> = (0..n).map(|_| rng.gen_range(0..255)).collect();

    bench_bg4_split_separate(random_u8s.clone());

    let groups = bg4::bg4_split_separate(&random_u8s);
    bench_bg4_regroup_separate(groups);

    bench_bg4_split_together(random_u8s.clone());
    bench_bg4_regroup_together(random_u8s.clone());
    bench_bg4_regroup_together_combined_write_4(random_u8s.clone());
    bench_bg4_regroup_together_combined_write_8(random_u8s.clone());

    bench_bg2_split(random_u8s.clone());
    bench_bg2_regroup(random_u8s.clone());
}

fn bench_speed_1(mut data: [Vec<u8>; 4], num_bytes: usize, f: fn(&[Vec<u8>]) -> u8, description: &str) {
    const ITER: usize = 100000;

    let mut sum = 0u64;

    let s = Instant::now();
    for _ in 0..ITER {
        sum += f(&data) as u64;
        // Prevent compilers from optimizing away iterations.
        data[0][0] = data[0][0].wrapping_mul(5).wrapping_add(13);
    }
    let runtime = s.elapsed().as_secs_f64();

    println!("{description} speed: {:.2} MB/s", num_bytes as f64 / 1e6 / runtime * ITER as f64);

    if sum == 0x5c26a6e {
        eprintln!("{sum}");
    }
}

fn bench_speed_2(mut data: Vec<u8>, num_bytes: usize, f: fn(&[u8]) -> u8, description: &str) {
    const ITER: usize = 100000;

    let mut sum = 0u64;

    let s = Instant::now();
    for _ in 0..ITER {
        sum += f(&data) as u64;
        // Prevent compilers from optimizing away iterations.
        data[0] = data[0].wrapping_mul(5).wrapping_add(13);
    }
    let runtime = s.elapsed().as_secs_f64();

    println!("{description} speed: {:.2} MB/s", num_bytes as f64 / 1e6 / runtime * ITER as f64);

    if sum == 0x5c26a6e {
        eprintln!("{sum}");
    }
}

fn bench_bg4_split_separate(data: Vec<u8>) {
    let n = data.len();
    bench_speed_2(
        data,
        n,
        |data| {
            let ret = bg4::bg4_split_separate(data);
            ret[0][0]
        },
        "bg4_split_separate",
    )
}

fn bench_bg4_split_together(data: Vec<u8>) {
    let n = data.len();
    bench_speed_2(
        data,
        n,
        |data| {
            let ret = bg4::bg4_split_together(data);
            ret[0]
        },
        "bg4_split_together",
    )
}

fn bench_bg4_regroup_separate(g: [Vec<u8>; 4]) {
    let n = g.iter().map(|g| g.len()).sum();
    bench_speed_1(
        g,
        n,
        |g| {
            let ret = bg4::bg4_regroup_separate(g);
            ret[0]
        },
        "bg4_regroup_separate",
    )
}

fn bench_bg4_regroup_together(g: Vec<u8>) {
    let n = g.len();
    bench_speed_2(
        g,
        n,
        |g| {
            let ret = bg4::bg4_regroup_together(g);
            ret[0]
        },
        "bg4_regroup_together",
    )
}

fn bench_bg4_regroup_together_combined_write_4(g: Vec<u8>) {
    let n = g.len();
    bench_speed_2(
        g,
        n,
        |g| {
            let ret = bg4::bg4_regroup_together_combined_write_4(g);
            ret[0]
        },
        "bg4_regroup_together_cw_4",
    )
}

fn bench_bg4_regroup_together_combined_write_8(g: Vec<u8>) {
    let n = g.len();
    bench_speed_2(
        g,
        n,
        |g| {
            let ret = bg4::bg4_regroup_together_combined_write_8(g);
            ret[0]
        },
        "bg4_regroup_together_cw_8",
    )
}

fn bench_bg2_split(data: Vec<u8>) {
    let n = data.len();
    bench_speed_2(
        data,
        n,
        |data| {
            let ret = bg2::bg2_split(data);
            ret[0]
        },
        "bg2_split",
    )
}

fn bench_bg2_regroup(g: Vec<u8>) {
    let n = g.len();
    bench_speed_2(
        g,
        n,
        |g| {
            let ret = bg2::bg2_regroup(g);
            ret[0]
        },
        "bg2_regroup",
    )
}
