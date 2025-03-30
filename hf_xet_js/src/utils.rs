use futures::{AsyncRead, AsyncReadExt};
use sha2::{Digest, Sha256};

pub async fn sha256_from_async_reader<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<String> {
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 1024];

    loop {
        let n = reader.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    let result = hasher.finalize();
    Ok(hex::encode(result))
}
