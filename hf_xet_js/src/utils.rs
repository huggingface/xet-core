use sha2::{Digest, Sha256};
use std::io::{Read, Result};

pub fn sha256_from_reader<R: Read>(reader: &mut R) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 1024];

    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    let result = hasher.finalize();
    Ok(hex::encode(result))
}
