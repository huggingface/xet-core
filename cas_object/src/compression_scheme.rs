use std::borrow::Cow;
use std::fmt::Display;
use std::io::{copy, Cursor, Read, Write};

use anyhow::anyhow;
use lz4_flex::frame::{FrameDecoder, FrameEncoder};

use crate::error::{CasObjectError, Result};

/// Dis-allow the value of ascii capital letters as valid CompressionScheme, 65-90
#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum CompressionScheme {
    #[default]
    None = 0,
    LZ4 = 1,
    ByteGrouping4LZ4 = 2, // 4 byte groups
}

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
}

fn lz4_compress_from_slice(data: &[u8]) -> Result<Vec<u8>> {
    let mut enc = FrameEncoder::new(Vec::new());
    enc.write_all(data)?;
    Ok(enc.finish()?)
}

fn lz4_decompress_from_slice(data: &[u8]) -> Result<Vec<u8>> {
    let mut dest = vec![];
    lz4_decompress_from_reader(&mut Cursor::new(data), &mut dest)?;
    Ok(dest)
}

fn lz4_decompress_from_reader<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<u64> {
    let mut dec = FrameDecoder::new(reader);
    Ok(copy(&mut dec, writer)?)
}

fn bg4_lz4_compress_from_slice(data: &[u8]) -> Result<Vec<u8>> {
    let groups = bg4_split(data);
    let mut dest = vec![];
    for g in groups {
        let mut enc = FrameEncoder::new(&mut dest);
        enc.write_all(&g)?;
        enc.finish()?;
    }

    Ok(dest)
}

fn bg4_lz4_decompress_from_slice(data: &[u8]) -> Result<Vec<u8>> {
    let mut dest = vec![];
    bg4_lz4_decompress_from_reader(&mut Cursor::new(data), &mut dest)?;
    Ok(dest)
}

fn bg4_lz4_decompress_from_reader<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<u64> {
    let mut groups = vec![];
    for _ in 0..4 {
        let mut g = vec![];
        FrameDecoder::new(&mut *reader).read_to_end(&mut g)?;
        groups.push(g);
    }

    let regrouped = bg4_regroup(&groups);

    writer.write_all(&regrouped)?;

    Ok(regrouped.len() as u64)
}

fn bg4_split(data: &[u8]) -> [Vec<u8>; 4] {
    let n = data.len();
    let split = n / 4;
    let rem = n & 4;
    let mut d0 = vec![0u8; split + 1.min(rem)];
    let mut d1 = vec![0u8; split + 1.min(rem.saturating_sub(1))];
    let mut d2 = vec![0u8; split + 1.min(rem.saturating_sub(2))];
    let mut d3 = vec![0u8; split];

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

fn bg4_regroup(groups: &[Vec<u8>]) -> Vec<u8> {
    let n = groups.iter().map(|g| g.len()).sum();
    let split = n / 4;
    let rem = n & 4;
    let g0 = &groups[0];
    let g1 = &groups[1];
    let g2 = &groups[2];
    let g3 = &groups[3];

    let mut data = vec![0u8; n];

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
        let mut rng = rand::thread_rng();

        let n = 64 * 1024;
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

        let dataset = [
            all_zeros,          // 180.04, 231.58
            all_ones,           // 180.04, 231.58
            all_0xff,           // 180.04, 231.58
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
            assert_eq!(data, bg4_lz4_uncompressed);
            let lz4_compressed = lz4_compress_from_slice(&data).unwrap();
            let lz4_uncompressed = lz4_decompress_from_slice(&lz4_compressed).unwrap();
            assert_eq!(data, lz4_uncompressed);
            println!(
                "Compression ratio: {:.2}, {:.2}",
                data.len() as f32 / bg4_lz4_compressed.len() as f32,
                data.len() as f32 / lz4_compressed.len() as f32
            );
        }
    }
}
