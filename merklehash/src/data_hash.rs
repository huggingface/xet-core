use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::mem::transmute_copy;
use std::num::ParseIntError;
use std::ops::{Deref, DerefMut};
use std::{fmt, str};

// URL safe Base 64 encoding with ending characters removed.
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use safe_transmute::{transmute_to_bytes, transmute_to_bytes_mut};
use serde::{Deserialize, Serialize};

/************************************************************************* */
/*  */
/* DataHash */
/*  */
/************************************************************************* */

/// The DataHash is a transparent 256-bit value stores as `[u64; 4]`.
///
/// [compute_data_hash] and [compute_internal_node_hash] are the two main
/// ways in which a hash on data can be computed.
///
/// Many convenient trait implementations are provided for printing, comparing,
/// and parsing.
///
/// ```ignore
/// let string = "hello world";
/// let hash = compute_data_hash(slice.as_bytes());
/// println!("Hello Hash {}", hash);
/// ```
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct DataHash([u64; 4]);

impl Deref for DataHash {
    type Target = [u64; 4];
    #[inline(always)]
    fn deref(&self) -> &[u64; 4] {
        &self.0
    }
}

impl DerefMut for DataHash {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [u64; 4] {
        &mut (self.0)
    }
}

impl From<[u64; 4]> for DataHash {
    fn from(value: [u64; 4]) -> Self {
        DataHash(value)
    }
}

impl From<[u8; 32]> for DataHash {
    fn from(value: [u8; 32]) -> Self {
        unsafe { Self(transmute_copy::<[u8; 32], [u64; 4]>(&value)) }
    }
}

impl From<&[u8; 32]> for DataHash {
    fn from(value: &[u8; 32]) -> Self {
        unsafe { Self(transmute_copy::<[u8; 32], [u64; 4]>(value)) }
    }
}

impl From<DataHash> for [u8; 32] {
    fn from(val: DataHash) -> Self {
        unsafe { std::mem::transmute(val) }
    }
}

impl AsRef<[u8]> for DataHash {
    fn as_ref(&self) -> &[u8] {
        transmute_to_bytes(self.deref())
    }
}

impl AsRef<DataHash> for DataHash {
    fn as_ref(&self) -> &DataHash {
        self
    }
}

impl Default for DataHash {
    /// The default constructor returns a DataHash of 0s
    fn default() -> DataHash {
        DataHash([0; 4])
    }
}

impl PartialEq for DataHash {
    fn eq(&self, other: &Self) -> bool {
        self[0] == other[0] && self[1] == other[1] && self[2] == other[2] && self[3] == other[3]
    }
}

impl Eq for DataHash {}

impl Ord for DataHash {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for DataHash {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl core::ops::Rem<u64> for DataHash {
    type Output = u64;

    fn rem(self, rhs: u64) -> Self::Output {
        self[3] % rhs
    }
}

#[cfg(not(target_family = "wasm"))]
unsafe impl heed::bytemuck::Zeroable for DataHash {
    fn zeroed() -> Self {
        DataHash([0; 4])
    }
}

#[cfg(not(target_family = "wasm"))]
unsafe impl heed::bytemuck::Pod for DataHash {}

/// The error type that is returned if [DataHash::from_hex] fails.
#[derive(Debug, Clone)]
pub struct DataHashHexParseError;

impl Error for DataHashHexParseError {}

impl fmt::Display for DataHashHexParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid hex input for DataHash")
    }
}

impl From<ParseIntError> for DataHashHexParseError {
    fn from(_err: ParseIntError) -> Self {
        DataHashHexParseError {}
    }
}

impl DataHash {
    /// Returns the hexadecimal printout of the hash.
    pub fn hex(&self) -> String {
        format!("{:016x}{:016x}{:016x}{:016x}", self.0[0], self.0[1], self.0[2], self.0[3])
    }

    /// Gives a compact base64 representation of the hash that is more compact than hex and is
    /// also url and file name safe.    
    pub fn base64(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.as_bytes())
    }

    /// Parses a hexadecimal string as a DataHash, returning
    /// Err(DataHashHexParseError) on failure.
    pub fn from_hex(h: &str) -> Result<DataHash, DataHashHexParseError> {
        if h.len() != 64 {
            return Err(DataHashHexParseError {});
        }
        let mut ret: DataHash = Default::default();

        let good = h.as_bytes().iter().all(|c| c.is_ascii_hexdigit());
        if !good {
            return Err(DataHashHexParseError {});
        }
        ret.0[0] = u64::from_str_radix(&h[..16], 16)?;
        ret.0[1] = u64::from_str_radix(&h[16..32], 16)?;
        ret.0[2] = u64::from_str_radix(&h[32..48], 16)?;
        ret.0[3] = u64::from_str_radix(&h[48..64], 16)?;
        Ok(ret)
    }

    // Converts the hash from base64 (created by base64 above) to this.  Used for testing.
    pub fn from_base64(b64: &str) -> Result<DataHash, DataHashBytesParseError> {
        let bytes = URL_SAFE_NO_PAD.decode(b64.as_bytes()).map_err(|_| DataHashBytesParseError {})?;
        DataHash::from_slice(&bytes)
    }

    /// Returns the datahash as a raw byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        transmute_to_bytes(&self.0[..])
    }

    pub fn from_slice(value: &[u8]) -> Result<Self, DataHashBytesParseError> {
        if value.len() != 32 {
            return Err(DataHashBytesParseError);
        }
        let mut hash: DataHash = DataHash::default();
        unsafe {
            let src = value.as_ptr();
            let dst = hash.0.as_mut_ptr() as *mut u8;
            std::ptr::copy_nonoverlapping(src, dst, 32);
        }
        Ok(hash)
    }

    pub fn random_from_seed(seed: u64) -> Self {
        let mut s = Self::default();
        let mut rng = SmallRng::seed_from_u64(seed);
        rng.fill_bytes(transmute_to_bytes_mut(&mut s.0[..]));
        s
    }
}

// Just use the same type here; rename for code legibility.
pub type HMACKey = DataHash;

impl DataHash {
    /**  Given a 256 bit key and a MerkleHash (the message), generate an HMAC hash from self.
     */
    pub fn hmac(&self, key: HMACKey) -> Self {
        // Use the blake 3 keyed hash method as the HMac
        Self::from(*blake3::keyed_hash(&key.into(), self.as_bytes()).as_bytes())
    }

    /// A marker struct of all 1 bits to use, as all zeros refers to the segment of zero bytes.
    pub fn marker() -> Self {
        DataHash([!0u64; 4])
    }
}

/// The error type that is returned if TryFrom<&[u8]> fails.
#[derive(Debug, Clone)]
pub struct DataHashBytesParseError;

impl Error for DataHashBytesParseError {}

impl fmt::Display for DataHashBytesParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid bytes input for DataHash")
    }
}

impl TryFrom<&[u8]> for DataHash {
    type Error = DataHashBytesParseError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::from_slice(value)
    }
}

impl From<DataHash> for Vec<u8> {
    fn from(val: DataHash) -> Self {
        val.as_bytes().into()
    }
}

impl From<&DataHash> for Vec<u8> {
    fn from(val: &DataHash) -> Self {
        val.as_bytes().into()
    }
}

// this is already a nice hash function. We just give the last 64-bits
// for use in hashtables etc.
impl Hash for DataHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0[0]);
    }
}
// as generated from random.org
/// The hash key used for [compute_data_hash]
const DATA_KEY: [u8; 32] = [
    102, 151, 245, 119, 91, 149, 80, 222, 49, 53, 203, 172, 165, 151, 24, 28, 157, 228, 33, 16, 155, 235, 43, 88, 180,
    208, 176, 75, 147, 173, 242, 41,
];

/// The hash key used for [compute_internal_node_hash]
const INTERNAL_NODE_HASH: [u8; 32] = [
    1, 126, 197, 199, 165, 71, 41, 150, 253, 148, 102, 102, 180, 138, 2, 230, 93, 221, 83, 111, 55, 199, 109, 210, 248,
    99, 82, 230, 74, 83, 113, 63,
];

/// Hash function used to compute a leaf hash of the MerkleTree
/// from any user-provided sequence of bytes. You should be using
/// [compute_internal_node_hash] if this hash is to be used for interior
/// nodes.
///
/// Example:
/// ```ignore
/// let string = "hello world";
/// let hash = compute_data_hash(slice.as_bytes());
/// println!("Hello Hash {}", hash);
/// ```
pub fn compute_data_hash(slice: &[u8]) -> DataHash {
    let digest = blake3::keyed_hash(&DATA_KEY, slice);
    DataHash::from(digest.as_bytes())
}

/// Hash function used to compute the hash of an interior node.
///
/// Note that this method also accepts a slice
/// of `&[u8]` and it is up to the caller to format the string appropriately.
/// For instance: the string could be simply the child hashes printed out
/// consecutively.
///
/// The reason why this method does not simply take an array of Hashes, and
/// instead require the caller to format the input as a string is to allow the
/// user to add additional information to the string being hashed (beyond just
/// the hashes itself). i.e. the string being hashed could be a concatenation
/// of "hashes of children + children metadata".
///
/// Example:
/// ```ignore
///     let mut buf = String::with_capacity(1024);
///     for node in nodes.iter() {
///         writeln!(buf, "{:x} : {}", node.hash(), node.len()).unwrap();
///     }
///     compute_internal_node_hash(buf.as_bytes())
/// ```
pub fn compute_internal_node_hash(slice: &[u8]) -> DataHash {
    let digest = blake3::keyed_hash(&INTERNAL_NODE_HASH, slice);
    DataHash::from(digest.as_bytes())
}

impl fmt::LowerHex for DataHash {
    /// Allow the DataHash to be printed with
    /// `println!("{:x}", hash)`
    /// This prints the hexadecimal representation of the Hash.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.hex())
    }
}

impl fmt::Display for DataHash {
    /// Allow the DataHash to be printed with
    /// `println!("{}", hash)`
    /// This prints the hexadecimal representation of the Hash.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.hex())
    }
}
impl fmt::Debug for DataHash {
    /// Allow the DataHash to be printed with
    /// `println!("{:?}", hash)`
    /// This prints the hexadecimal representation of the Hash.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.hex())
    }
}

/// Wrapper around a Write trait that allows computation of the hash at the end.
///
/// It is recommended to wrap this in a BufWriter object: i.e.
///
/// let out_file = std::fs::OpenOptions::new().create(true).write(true).open("temp.fs")?;
///
/// let hashed_write = HashedWrite::new(out_file);
///
/// {
///    let buf_writer = BufWriter::new(&mut hashed_write);
///
///    // Do the writing against buf_writer
///
///    
/// }
///
/// let h = hashed_write.hash();
///  
pub struct HashedWrite<W: Write> {
    hasher: blake3::Hasher,
    writer: W,
}

impl<W: Write> HashedWrite<W> {
    pub fn new(writer: W) -> Self {
        Self {
            hasher: blake3::Hasher::new_keyed(&DATA_KEY),
            writer,
        }
    }

    pub fn hash(&self) -> DataHash {
        let digest = self.hasher.finalize();
        DataHash::from(digest.as_bytes())
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: Write> Write for HashedWrite<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        self.writer.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

pub mod hex {
    pub mod serde {
        use std::fmt;

        use serde::de::{self, Visitor};
        use serde::{Deserializer, Serializer};

        use crate::DataHash;

        pub fn serialize<S>(value: &DataHash, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let hex = value.hex();
            serializer.serialize_str(&hex)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<DataHash, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_str(HexVisitor)
        }

        // Visitor for deserialization
        struct HexVisitor;

        impl Visitor<'_> for HexVisitor {
            type Value = DataHash;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a merklehash")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                DataHash::from_hex(v).map_err(|e| serde::de::Error::custom(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use rand::prelude::*;

    use crate::{compute_data_hash, DataHash, HashedWrite};

    #[test]
    fn test_try_from_bytes() {
        let hash_bytes_proper = [1u8; 32].to_vec();
        assert!(DataHash::try_from(hash_bytes_proper.as_slice()).is_ok());

        let hash_bytes_improper = [1u8; 31];
        assert!(DataHash::try_from(hash_bytes_improper.as_slice()).is_err());
    }

    #[test]
    fn test_hashed_write() -> std::io::Result<()> {
        let mut written_data = Vec::<u8>::with_capacity(300);
        let mut raw_data = vec![0u8; 300];

        let mut rng = StdRng::seed_from_u64(0);
        rng.fill_bytes(&mut raw_data[..]);

        let mut hashed_write = HashedWrite::new(&mut written_data);

        for i in 0..30 {
            hashed_write.write_all(&raw_data[(10 * i)..(10 * (i + 1))])?;
        }

        assert_eq!(hashed_write.hash(), compute_data_hash(&raw_data[..]));
        assert_eq!(written_data, raw_data);

        Ok(())
    }

    #[test]
    fn test_hmac_hash_key_change() {
        // Prepare a fixed message
        let message_bytes = [0u8; 32];
        let message = DataHash::from(message_bytes);

        // Prepare two different keys
        let key1 = [0u8; 32];
        let key2 = [1u8; 32];

        // Compute HMAC hashes with different keys
        let output1 = message.hmac(key1.into());
        let output2 = message.hmac(key2.into());

        // Verify that the outputs are different
        assert_ne!(output1, output2,);
    }

    #[test]
    fn test_hmac_hash_message_change() {
        // Prepare a fixed key
        let key = [0u8; 32];

        // Prepare two different messages
        let message1_bytes = [0u8; 32];
        let message2_bytes = [1u8; 32];

        let message1 = DataHash::from(message1_bytes);
        let message2 = DataHash::from(message2_bytes);

        // Compute HMAC hashes with different messages
        let output1 = message1.hmac(key.into());
        let output2 = message2.hmac(key.into());

        // Verify that the outputs are different
        assert_ne!(output1, output2,);
    }

    // Test the base64 usage.
    #[test]
    fn test_base64() {
        let hash = compute_data_hash(&[0, 1, 2, 3]);

        let b64 = hash.base64();

        assert_eq!(hash, DataHash::from_base64(&b64).unwrap());
    }
}
