use std::collections::VecDeque;
use std::fmt;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lazy_static::lazy_static;
use rand_chacha::rand_core::{RngCore, SeedableRng};
use rand_chacha::ChaChaRng;

pub trait RollingHash {
    fn clone(&self) -> Box<dyn RollingHash>;

    fn current_hash(&self) -> u32;
    fn reset(&mut self);
    fn accumulate_byte(&mut self, b: u8) -> u32;
    fn at_chunk_boundary(&self) -> bool;

    fn accumulate(&mut self, buf: &[u8]) -> u32 {
        for &c in buf.iter() {
            self.accumulate_byte(c);
        }
        self.current_hash()
    }

    fn scan_to_chunk_boundary(&mut self, buf: &[u8]) -> (u32, usize) {
        for (u, &c) in buf.iter().enumerate() {
            self.accumulate_byte(c);
            if self.at_chunk_boundary() {
                return (self.current_hash(), u);
            }
        }
        (self.current_hash(), buf.len())
    }
}

#[derive(Debug)]
pub struct SumHash {
    mask: u32,
    bytehash: [u32; 256],
    history: VecDeque<u8>,
    windowsize: usize,
    current_hash: u32,
}

impl SumHash {
    /// if seed is 0, each byte is added without a byte hash. i.e.
    /// the hash of a window is exactly the sum of all the bytes.
    /// Otherwise, a RNG is used to generate a fixed random value for each
    /// byte.
    pub fn init(mask: u32, windowsize: usize, seed: u64) -> SumHash {
        let mut bytehash: [u32; 256] = [0; 256];
        if seed > 0 {
            let mut rng = ChaChaRng::seed_from_u64(seed);
            #[allow(clippy::needless_range_loop)]
            for i in 0..256 {
                bytehash[i] = rng.next_u32();
            }
        } else {
            #[allow(clippy::needless_range_loop)]
            for i in 0..256 {
                bytehash[i] = i as u32;
            }
        }
        assert!(windowsize > 0);
        SumHash {
            mask,
            bytehash,
            history: VecDeque::new(),
            windowsize,
            current_hash: 0,
        }
    }
}

impl RollingHash for SumHash {
    fn clone(&self) -> Box<dyn RollingHash> {
        Box::new(SumHash {
            mask: self.mask,
            bytehash: self.bytehash,
            history: self.history.clone(),
            windowsize: self.windowsize,
            current_hash: self.current_hash,
        })
    }
    fn current_hash(&self) -> u32 {
        self.current_hash
    }
    fn reset(&mut self) {
        self.current_hash = 0_u32;
        self.history.clear();
    }
    fn at_chunk_boundary(&self) -> bool {
        self.current_hash & self.mask == 0
    }
    fn accumulate_byte(&mut self, b: u8) -> u32 {
        self.history.push_back(b);
        self.current_hash = self.current_hash.wrapping_add(self.bytehash[b as usize]);
        if self.history.len() > self.windowsize {
            let first = *(self.history.front().unwrap());
            self.current_hash = self.current_hash.wrapping_sub(self.bytehash[first as usize]);
            self.history.pop_front();
        }
        self.current_hash
    }
}

impl fmt::Display for SumHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "mask: {}, windowsize {}", self.mask, self.windowsize)
    }
}

#[derive(Debug)]
pub struct BuzHash {
    mask: u32,
    bytehash_right: [u32; 256], // bytehash to use when adding a byte to the right
    bytehash_left: [u32; 256],  // bytehash to use when removing a byte from the left
    history: VecDeque<u8>,
    windowsize: usize,
    current_hash: u32,
}

impl BuzHash {
    pub fn init(mask: u32, windowsize: usize, seed: u64) -> BuzHash {
        let mut bytehash_right: [u32; 256] = [0; 256];
        let mut bytehash_left: [u32; 256] = [0; 256];
        let mut rng = ChaChaRng::seed_from_u64(seed);
        for i in 0..256 {
            bytehash_right[i] = rng.next_u32();
            bytehash_left[i] = bytehash_right[i].rotate_left(windowsize as u32);
        }
        assert!(windowsize > 0);
        BuzHash {
            mask,
            bytehash_right,
            bytehash_left,
            history: VecDeque::new(),
            windowsize,
            current_hash: 0,
        }
    }
}

impl RollingHash for BuzHash {
    fn clone(&self) -> Box<dyn RollingHash> {
        Box::new(BuzHash {
            mask: self.mask,
            bytehash_right: self.bytehash_right,
            bytehash_left: self.bytehash_left,
            history: self.history.clone(),
            windowsize: self.windowsize,
            current_hash: self.current_hash,
        })
    }
    fn current_hash(&self) -> u32 {
        self.current_hash
    }
    fn reset(&mut self) {
        self.current_hash = 0_u32;
        self.history.clear();
    }
    fn at_chunk_boundary(&self) -> bool {
        self.current_hash & self.mask == 0
    }
    fn accumulate_byte(&mut self, b: u8) -> u32 {
        self.history.push_back(b);
        self.current_hash = self.current_hash.rotate_left(1) ^ self.bytehash_right[b as usize];
        if self.history.len() > self.windowsize {
            let first = *(self.history.front().unwrap());
            self.current_hash ^= self.bytehash_left[first as usize];
            self.history.pop_front();
        }
        self.current_hash
    }
}

impl fmt::Display for BuzHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "mask: {}, windowsize {}", self.mask, self.windowsize)
    }
}

#[derive(Debug)]
pub struct GearHash {
    mask: u32,
    bytehash: [u32; 256],
    current_hash: u32,
}

impl GearHash {
    pub fn init(mask: u32, seed: u64) -> GearHash {
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let mut bytehash: [u32; 256] = [0; 256];
        #[allow(clippy::needless_range_loop)]
        for i in 0..256 {
            bytehash[i] = rng.next_u32();
        }
        GearHash {
            mask,
            bytehash,
            current_hash: 0,
        }
    }
}

impl RollingHash for GearHash {
    fn clone(&self) -> Box<dyn RollingHash> {
        Box::new(GearHash {
            mask: self.mask,
            bytehash: self.bytehash,
            current_hash: self.current_hash,
        })
    }
    #[inline(always)]
    fn current_hash(&self) -> u32 {
        self.current_hash
    }
    fn reset(&mut self) {
        self.current_hash = 0_u32;
    }
    #[inline(always)]
    fn accumulate_byte(&mut self, b: u8) -> u32 {
        self.current_hash = self.current_hash.wrapping_shl(1).wrapping_add(self.bytehash[b as usize]);
        self.current_hash
    }
    #[inline(always)]
    fn at_chunk_boundary(&self) -> bool {
        self.current_hash & self.mask == 0
    }
}

impl fmt::Display for GearHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "mask: {}, windowsize 32", self.mask)
    }
}

const BENCH_INPUT_SEED: u64 = 0xa383d96f7becd17e;
const BUF_SIZE: usize = 128; // * 1024

lazy_static! {
    static ref BENCH_INPUT_BUF: [u8; BUF_SIZE] = {
        use rand::{RngCore, SeedableRng};
        let mut bytes = [0u8; BUF_SIZE];
        rand::rngs::StdRng::seed_from_u64(BENCH_INPUT_SEED).fill_bytes(&mut bytes);
        bytes
    };
}

fn run_rolling_hash(rolling_hash: &mut impl RollingHash) {
    let len = 0usize;
    loop {
        let (_, len) = black_box(rolling_hash.scan_to_chunk_boundary(&BENCH_INPUT_BUF[len..]));
        if len >= BUF_SIZE {
            break;
        }
    }
}

fn bench_rolling_hashes(c: &mut Criterion) {
    let mut sh = SumHash::init(0xFF, 64, 1234);
    let mut bh = BuzHash::init(0xFF, 128, 1234);
    let mut gh = GearHash::init(0xFF, 1234);

    let mut group = c.benchmark_group("Rolling Hashes");
    group.bench_function("SumHash", |b| b.iter(|| run_rolling_hash(&mut sh)));
    group.bench_function("BuzzHash", |b| b.iter(|| run_rolling_hash(&mut bh)));
    group.bench_function("GearHash", |b| b.iter(|| run_rolling_hash(&mut gh)));
    group.finish();
}

criterion_group!(benches, bench_rolling_hashes);
criterion_main!(benches);
