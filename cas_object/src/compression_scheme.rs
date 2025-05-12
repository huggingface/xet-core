use std::borrow::Cow;
use std::fmt::Display;
use std::io::{copy, Cursor, Read, Write};
use std::time::Instant;

use anyhow::anyhow;
use lz4_flex::frame::{FrameDecoder, FrameEncoder};

use crate::byte_grouping::bg4::{bg4_regroup, bg4_split};
use crate::byte_grouping::BG4Predictor;
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
        let mut bg4_predictor = BG4Predictor::default();

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
