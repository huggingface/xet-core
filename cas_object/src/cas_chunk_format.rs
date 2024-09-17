
const CAS_CHUNK_COMPRESSION_UNCOMPRESSED: u8 = 0;
const CAS_CHUNK_COMPRESSION_LZ4: u8 = 1;

#[repr(C, packed)]
#[derive(Debug, Copy, Clone)]
pub struct CASChunkHeader {
    pub version: u8,              // 1 byte
    compressed_length: [u8; 3],   // 3 bytes
    pub compression_scheme: u8,   // 1 byte
    uncompressed_length: [u8; 3], // 3 bytes
}

impl CASChunkHeader {
    // Helper function to set compressed length from u32
    pub fn set_compressed_length(&mut self, length: u32) {
        let bytes = length.to_le_bytes(); // Convert u32 to little-endian bytes
        self.compressed_length.copy_from_slice(&bytes[0..3]); // Use the first 3 bytes
    }

    // Helper function to get compressed length as u32
    pub fn get_compressed_length(&self) -> u32 {
        let mut bytes = [0u8; 4]; // Create 4-byte array
        bytes[0..3].copy_from_slice(&self.compressed_length); // Copy 3 bytes
        u32::from_le_bytes(bytes) // Convert back to u32
    }

    // Helper function to set uncompressed length from u32
    pub fn set_uncompressed_length(&mut self, length: u32) {
        let bytes = length.to_le_bytes();
        self.uncompressed_length.copy_from_slice(&bytes[0..3]);
    }

    // Helper function to get uncompressed length as u32
    pub fn get_uncompressed_length(&self) -> u32 {
        let mut bytes = [0u8; 4];
        bytes[0..3].copy_from_slice(&self.uncompressed_length);
        u32::from_le_bytes(bytes)
    }
}
