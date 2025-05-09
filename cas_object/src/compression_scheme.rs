use std::borrow::Cow;
use std::fmt::Display;
use std::io::{copy, Cursor, Read, Write};
use std::time::Instant;

use anyhow::anyhow;
use lz4_flex::frame::{FrameDecoder, FrameEncoder};

use crate::byte_grouping::bg4::{bg4_regroup, bg4_split};
use crate::error::{CasObjectError, Result};

pub static mut BG4_SPLIT_RUNTIME: f64 = 0.;
pub static mut BG4_REGROUP_RUNTIME: f64 = 0.;
pub static mut BG4_LZ4_COMPRESS_RUNTIME: f64 = 0.;
pub static mut BG4_LZ4_DECOMPRESS_RUNTIME: f64 = 0.;

/// Dis-allow the value of ascii capital letters as valid CompressionScheme, 65-90
#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum CompressionScheme {
    #[default]
    None = 0,
    LZ4 = 1,
    ByteGrouping4LZ4 = 2, // 4 byte groups
}
pub const NUM_COMPRESSION_SCHEMES: usize = 3;

impl Display for CompressionScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", Into::<&str>::into(self))
    }
}
impl From<&CompressionScheme> for &'static str {
    fn from(value: &CompressionScheme) -> Self {
        match value {
            CompressionScheme::None => "none",
            CompressionScheme::LZ4 => "lz4",
            CompressionScheme::ByteGrouping4LZ4 => "bg4-lz4",
        }
    }
}

impl From<CompressionScheme> for &'static str {
    fn from(value: CompressionScheme) -> Self {
        From::from(&value)
    }
}

impl TryFrom<u8> for CompressionScheme {
    type Error = CasObjectError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(CompressionScheme::None),
            1 => Ok(CompressionScheme::LZ4),
            2 => Ok(CompressionScheme::ByteGrouping4LZ4),
            _ => Err(CasObjectError::FormatError(anyhow!("cannot convert value {value} to CompressionScheme"))),
        }
    }
}

impl CompressionScheme {
    pub fn compress_from_slice<'a>(&self, data: &'a [u8]) -> Result<Cow<'a, [u8]>> {
        Ok(match self {
            CompressionScheme::None => data.into(),
            CompressionScheme::LZ4 => lz4_compress_from_slice(data).map(Cow::from)?,
            CompressionScheme::ByteGrouping4LZ4 => bg4_lz4_compress_from_slice(data).map(Cow::from)?,
        })
    }

    pub fn decompress_from_slice<'a>(&self, data: &'a [u8]) -> Result<Cow<'a, [u8]>> {
        Ok(match self {
            CompressionScheme::None => data.into(),
            CompressionScheme::LZ4 => lz4_decompress_from_slice(data).map(Cow::from)?,
            CompressionScheme::ByteGrouping4LZ4 => bg4_lz4_decompress_from_slice(data).map(Cow::from)?,
        })
    }

    pub fn decompress_from_reader<R: Read, W: Write>(&self, reader: &mut R, writer: &mut W) -> Result<u64> {
        Ok(match self {
            CompressionScheme::None => copy(reader, writer)?,
            CompressionScheme::LZ4 => lz4_decompress_from_reader(reader, writer)?,
            CompressionScheme::ByteGrouping4LZ4 => bg4_lz4_decompress_from_reader(reader, writer)?,
        })
    }

    /// Chooses the compression scheme based on a KL-divergence heuristic.
    pub fn choose_from_data(data: &[u8]) -> Self {
        let mut bg4_predictor = BG4Predictor::new();

        bg4_predictor.add_data(0, data);

        if bg4_predictor.bg4_recommended() {
            CompressionScheme::ByteGrouping4LZ4
        } else {
            CompressionScheme::LZ4
        }
    }
}

pub fn lz4_compress_from_slice(data: &[u8]) -> Result<Vec<u8>> {
    let mut enc = FrameEncoder::new(Vec::new());
    enc.write_all(data)?;
    Ok(enc.finish()?)
}

pub fn lz4_decompress_from_slice(data: &[u8]) -> Result<Vec<u8>> {
    let mut dest = vec![];
    lz4_decompress_from_reader(&mut Cursor::new(data), &mut dest)?;
    Ok(dest)
}

fn lz4_decompress_from_reader<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<u64> {
    let mut dec = FrameDecoder::new(reader);
    Ok(copy(&mut dec, writer)?)
}

pub fn bg4_lz4_compress_from_slice(data: &[u8]) -> Result<Vec<u8>> {
    let s = Instant::now();
    let groups = bg4_split(data);
    unsafe {
        BG4_SPLIT_RUNTIME += s.elapsed().as_secs_f64();
    }

    let s = Instant::now();
    let mut dest = vec![];
    let mut enc = FrameEncoder::new(&mut dest);
    enc.write_all(&groups)?;
    enc.finish()?;
    unsafe {
        BG4_LZ4_COMPRESS_RUNTIME += s.elapsed().as_secs_f64();
    }

    Ok(dest)
}

pub fn bg4_lz4_decompress_from_slice(data: &[u8]) -> Result<Vec<u8>> {
    let mut dest = vec![];
    bg4_lz4_decompress_from_reader(&mut Cursor::new(data), &mut dest)?;
    Ok(dest)
}

fn bg4_lz4_decompress_from_reader<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<u64> {
    let s = Instant::now();
    let mut g = vec![];
    FrameDecoder::new(reader).read_to_end(&mut g)?;
    unsafe {
        BG4_LZ4_DECOMPRESS_RUNTIME += s.elapsed().as_secs_f64();
    }

    let s = Instant::now();
    let regrouped = bg4_regroup(&g);
    unsafe {
        BG4_REGROUP_RUNTIME += s.elapsed().as_secs_f64();
    }

    writer.write_all(&regrouped)?;

    Ok(regrouped.len() as u64)
}

pub struct BG4Predictor {
    histograms: [[u32; 9]; 4],

    #[cfg(debug_assertions)]
    histograms_check: [[u32; 9]; 4],
}

/// Put this logic in.
impl Default for BG4Predictor {
    fn default() -> Self {
        Self::new()
    }
}

impl BG4Predictor {
    pub fn new() -> Self {
        Self {
            histograms: [[0u32; 9]; 4],

            #[cfg(debug_assertions)]
            histograms_check: [[0u32; 9]; 4],
        }
    }

    pub fn add_data(&mut self, offset: usize, data: &[u8]) {
        // Do it using pointers for optimization.
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

        #[cfg(debug_assertions)]
        {
            for (i, &x) in data.iter().enumerate() {
                self.histograms_check[(i + offset) % 4][x.count_ones() as usize] += 1;
            }
            assert_eq!(self.histograms_check, self.histograms);
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
    use std::mem::size_of;

    use half::prelude::*;
    use rand::Rng;

    use super::*;

    #[test]
    fn test_to_str() {
        assert_eq!(Into::<&str>::into(CompressionScheme::None), "none");
        assert_eq!(Into::<&str>::into(CompressionScheme::LZ4), "lz4");
        assert_eq!(Into::<&str>::into(CompressionScheme::ByteGrouping4LZ4), "bg4-lz4");
    }

    #[test]
    fn test_from_u8() {
        assert_eq!(CompressionScheme::try_from(0u8), Ok(CompressionScheme::None));
        assert_eq!(CompressionScheme::try_from(1u8), Ok(CompressionScheme::LZ4));
        assert_eq!(CompressionScheme::try_from(2u8), Ok(CompressionScheme::ByteGrouping4LZ4));
        assert!(CompressionScheme::try_from(3u8).is_err());
    }

    #[test]
    fn test_bg4_lz4() {
        let mut rng = rand::rng();

        for i in 0..4 {
            let n = 64 * 1024 + i * 23;
            let all_zeros = vec![0u8; n];
            let all_ones = vec![1u8; n];
            let all_0xff = vec![0xFF; n];
            let random_u8s: Vec<_> = (0..n).map(|_| rng.random_range(0..255)).collect();
            let random_f32s_ng1_1: Vec<_> = (0..n / size_of::<f32>())
                .map(|_| rng.random_range(-1.0f32..=1.0))
                .flat_map(|f| f.to_le_bytes())
                .collect();
            let random_f32s_0_2: Vec<_> = (0..n / size_of::<f32>())
                .map(|_| rng.random_range(0f32..=2.0))
                .flat_map(|f| f.to_le_bytes())
                .collect();
            let random_f64s_ng1_1: Vec<_> = (0..n / size_of::<f64>())
                .map(|_| rng.random_range(-1.0f64..=1.0))
                .flat_map(|f| f.to_le_bytes())
                .collect();
            let random_f64s_0_2: Vec<_> = (0..n / size_of::<f64>())
                .map(|_| rng.random_range(0f64..=2.0))
                .flat_map(|f| f.to_le_bytes())
                .collect();

            // f16, a.k.a binary16 format: sign (1 bit), exponent (5 bit), mantissa (10 bit)
            let random_f16s_ng1_1: Vec<_> = (0..n / size_of::<f16>())
                .map(|_| f16::from_f32(rng.random_range(-1.0f32..=1.0)))
                .flat_map(|f| f.to_le_bytes())
                .collect();
            let random_f16s_0_2: Vec<_> = (0..n / size_of::<f16>())
                .map(|_| f16::from_f32(rng.random_range(0f32..=2.0)))
                .flat_map(|f| f.to_le_bytes())
                .collect();

            // bf16 format: sign (1 bit), exponent (8 bit), mantissa (7 bit)
            let random_bf16s_ng1_1: Vec<_> = (0..n / size_of::<bf16>())
                .map(|_| bf16::from_f32(rng.random_range(-1.0f32..=1.0)))
                .flat_map(|f| f.to_le_bytes())
                .collect();
            let random_bf16s_0_2: Vec<_> = (0..n / size_of::<bf16>())
                .map(|_| bf16::from_f32(rng.random_range(0f32..=2.0)))
                .flat_map(|f| f.to_le_bytes())
                .collect();

            let dataset = [
                all_zeros,          // 231.58, 231.58
                all_ones,           // 231.58, 231.58
                all_0xff,           // 231.58, 231.58
                random_u8s,         // 1.00, 1.00
                random_f32s_ng1_1,  // 1.08, 1.00
                random_f32s_0_2,    // 1.15, 1.00
                random_f64s_ng1_1,  // 1.00, 1.00
                random_f64s_0_2,    // 1.00, 1.00
                random_f16s_ng1_1,  // 1.00, 1.00
                random_f16s_0_2,    // 1.00, 1.00
                random_bf16s_ng1_1, // 1.18, 1.00
                random_bf16s_0_2,   // 1.37, 1.00
            ];

            for data in dataset {
                let bg4_lz4_compressed = bg4_lz4_compress_from_slice(&data).unwrap();
                let bg4_lz4_uncompressed = bg4_lz4_decompress_from_slice(&bg4_lz4_compressed).unwrap();
                assert_eq!(data.len(), bg4_lz4_uncompressed.len());
                assert_eq!(data, bg4_lz4_uncompressed);
                let lz4_compressed = lz4_compress_from_slice(&data).unwrap();
                let lz4_uncompressed = lz4_decompress_from_slice(&lz4_compressed).unwrap();
                assert_eq!(data, lz4_uncompressed);
                let compression_scheme_predictor = CompressionScheme::choose_from_data(&data);
                println!(
                    "Compression ratio: {:.2}, {:.2} (KL predicted = {compression_scheme_predictor:?}",
                    data.len() as f32 / bg4_lz4_compressed.len() as f32,
                    data.len() as f32 / lz4_compressed.len() as f32
                );
            }
        }
    }
}
