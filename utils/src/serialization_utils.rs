use std::io::{Read, Write};
use std::mem::{size_of, transmute};

use merklehash::MerkleHash;

pub fn write_hash<W: Write>(writer: &mut W, m: &MerkleHash) -> Result<(), std::io::Error> {
    writer.write_all(m.as_bytes())
}

pub fn write_u32<W: Write>(writer: &mut W, v: u32) -> Result<(), std::io::Error> {
    writer.write_all(&v.to_le_bytes())
}

pub fn write_u64<W: Write>(writer: &mut W, v: u64) -> Result<(), std::io::Error> {
    writer.write_all(&v.to_le_bytes())
}

pub fn write_u32s<W: Write>(writer: &mut W, vs: &[u32]) -> Result<(), std::io::Error> {
    for e in vs {
        write_u32(writer, *e)?;
    }

    Ok(())
}

pub fn write_u64s<W: Write>(writer: &mut W, vs: &[u64]) -> Result<(), std::io::Error> {
    for e in vs {
        write_u64(writer, *e)?;
    }

    Ok(())
}

pub fn read_hash<R: Read>(reader: &mut R) -> Result<MerkleHash, std::io::Error> {
    let mut m = [0u8; 32];
    reader.read_exact(&mut m)?; // Not endian safe.

    Ok(MerkleHash::from(unsafe { transmute::<[u8; 32], [u64; 4]>(m) }))
}

pub fn read_u32<R: Read>(reader: &mut R) -> Result<u32, std::io::Error> {
    let mut buf = [0u8; size_of::<u32>()];
    reader.read_exact(&mut buf[..])?;
    Ok(u32::from_le_bytes(buf))
}

pub fn read_u64<R: Read>(reader: &mut R) -> Result<u64, std::io::Error> {
    let mut buf = [0u8; size_of::<u64>()];
    reader.read_exact(&mut buf[..])?;
    Ok(u64::from_le_bytes(buf))
}

pub fn read_u64s<R: Read>(reader: &mut R, vs: &mut [u64]) -> Result<(), std::io::Error> {
    for e in vs.iter_mut() {
        *e = read_u64(reader)?;
    }

    Ok(())
}
