use std::time::Instant;

use cas_object::byte_grouping::BG4Predictor;
use rand::prelude::*;

fn main() {
    const SIZE_MB: usize = 100;
    const SIZE: usize = SIZE_MB * 1024 * 1024;

    let mut rng = StdRng::seed_from_u64(12345);
    let mut data = vec![0u8; SIZE];
    rng.fill_bytes(&mut data);

    let offset = 0;

    let mut ref_pred = BG4Predictor::default();
    let start = Instant::now();
    ref_pred.add_data_reference(offset, &data);
    let duration = start.elapsed().as_secs_f64();
    println!("Reference: {:.2} MB/s", SIZE_MB as f64 / duration);

    let mut ref_v1 = BG4Predictor::default();
    let start = Instant::now();
    ref_v1.add_data_v1(offset, &data);
    let duration = start.elapsed().as_secs_f64();
    println!("V1: {:.2} MB/s", SIZE_MB as f64 / duration);

    let mut ref_swar = BG4Predictor::default();
    let start = Instant::now();
    ref_swar.add_data_swar(offset, &data);
    let duration = start.elapsed().as_secs_f64();
    println!("SWAR: {:.2} MB/s", SIZE_MB as f64 / duration);

    let mut new_pred = BG4Predictor::default();
    let start = Instant::now();
    new_pred.add_data(offset, &data);
    let duration = start.elapsed().as_secs_f64();
    println!("Optimized: {:.2} MB/s", SIZE_MB as f64 / duration);

    assert_eq!(ref_pred.histograms(), new_pred.histograms());
}
