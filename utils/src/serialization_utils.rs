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

/// Unsafe function to read a MerkleHash` value from a raw pointer.
#[inline]
pub unsafe fn read_hash_ptr(ptr: *const u8) -> MerkleHash {
    let mut hash = [0u64; 4];
    unsafe {
        read_u64s_ptr(ptr, &mut hash);
    }
    hash
}

pub fn read_u32<R: Read>(reader: &mut R) -> Result<u32, std::io::Error> {
    let mut buf = [0u8; size_of::<u32>()];
    reader.read_exact(&mut buf[..])?;
    Ok(u32::from_le_bytes(buf))
}

/// Unsafe function to read a `u32` value from a raw pointer.
pub unsafe fn read_u32_ptr(ptr: *const u8) -> u32 {
    let mut buf = [0u8; 4];
    unsafe {
        std::ptr::copy_nonoverlapping(ptr, buf.as_mut_ptr(), 4);
    }
    u32::from_le_bytes(buf)
}

pub fn read_u64<R: Read>(reader: &mut R) -> Result<u64, std::io::Error> {
    let mut buf = [0u8; size_of::<u64>()];
    reader.read_exact(&mut buf[..])?;
    Ok(u64::from_le_bytes(buf))
}

/// Unsafe function to read a `u64` value from a raw pointer.
#[inline]
pub unsafe fn read_u64_ptr(ptr: *const u8) -> u64 {
    let mut buf = [0u8; 8];
    unsafe {
        std::ptr::copy_nonoverlapping(ptr, buf.as_mut_ptr(), 8);
    }
    u64::from_le_bytes(buf)
}

pub fn read_u64s<R: Read>(reader: &mut R, vs: &mut [u64]) -> Result<(), std::io::Error> {
    for e in vs.iter_mut() {
        *e = read_u64(reader)?;
    }

    Ok(())
}

/// Unsafe function to read multiple `u64` values into a slice from a raw pointer.
#[inline]
pub unsafe fn read_u64s_ptr(ptr: *const u8, vs: &mut [u64]) {
    let num_bytes = vs.len() * 8;
    unsafe {
        std::ptr::copy_nonoverlapping(ptr, vs.as_mut_ptr() as *mut u8, num_bytes);
    }
    for v in vs.iter_mut() {
        *v = u64::from_le(*v);
    }
}
pub unsafe fn read_u32_ptr_unsafe(ptr: *const u8) -> u32 {
    let value = read_unaligned(ptr as *const u32);
    u32::from_le(value)
}

/// Unsafe function to read a `u64` value from a raw pointer.
pub unsafe fn read_u64_ptr_unsafe(ptr: *const u8) -> u64 {
    let value = read_unaligned(ptr as *const u64);
    u64::from_le(value)
}

/// Unsafe function to read multiple `u64` values into a slice from a raw pointer.
pub unsafe fn read_u64s_ptr_unsafe(ptr: *const u8, vs: &mut [u64]) {
    let num_bytes = vs.len() * size_of::<u64>();
    unsafe {
        std::ptr::copy_nonoverlapping(ptr, vs.as_mut_ptr() as *mut u8, num_bytes);
    }
    for e in vs.iter_mut() {
        *e = u64::from_le(*e);
    }
}

/// Unsafe function to read a `MerkleHash` from a raw pointer.
pub unsafe fn read_hash_ptr_unsafe(ptr: *const u8) -> MerkleHash {
    let mut hash = [0u64; 4];
    unsafe {
        read_u64s_ptr_unsafe(ptr, &mut hash);
    }
    MerkleHash::from(hash)
}
