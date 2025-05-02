use std::sync::Arc;

use cas_object::CompressionScheme;
use futures::AsyncReadExt;
use tokio::sync::mpsc;
use utils::auth::AuthConfig;
use wasm_bindgen::prelude::*;
use wasm_thread as thread;
use wasm_xet::blob_reader::BlobReader;
use wasm_xet::configurations::{DataConfig, RepoSalt, ShardConfig, TranslatorConfig};
use wasm_xet::wasm_file_upload_session::FileUploadSession;

fn main() {
    #[cfg(target_arch = "wasm32")]
    {
        console_log::init().unwrap();
        console_error_panic_hook::set_once();
    }

    #[cfg(not(target_arch = "wasm32"))]
    env_logger::init_from_env(env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"));

    log::info!("Starting init wasm_xet...");

    log::info!("Done");
}

#[wasm_bindgen]
pub async fn test_async_blob_reader(file: web_sys::File) -> String {
    log::info!("test_async_reader called");

    let Ok(blob) = file.slice() else {
        log::info!("failed to convert a file to blob");
        return "".to_owned();
    };

    let Ok(mut reader) = BlobReader::new(blob) else {
        log::info!("failed to get a reader for blob");
        return "".to_owned();
    };

    let mut total_ret = 0;

    let mut total_bytes = vec![];
    loop {
        const READ_BUF_SIZE: usize = 1024 * 1024;
        let mut buf = vec![0u8; READ_BUF_SIZE];
        log::info!("buf created");
        let read_bytes = reader.read(&mut buf).await;
        log::info!("wf read returned");
        let Ok(read_bytes) = read_bytes else {
            log::info!("{read_bytes:?}");
            return "".to_owned();
        };

        log::info!("read {read_bytes} bytes");
        if read_bytes == 0 {
            break;
        }

        total_bytes.extend_from_slice(std::mem::take(&mut &buf[..read_bytes]));
    }

    log::info!("file total size: {}", total_bytes.len());

    let nbyte = total_bytes.len();
    let mut threads = vec![];
    let mut inputs = vec![];
    let mut outputs = vec![];
    for t in 0..5 {
        let split = nbyte / 5;
        let data_local = total_bytes[split * t..split * (t + 1)].to_vec();
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(2);
        let (o_tx, o_rx) = mpsc::channel::<u32>(2);
        outputs.push(o_rx);
        threads.push(thread::spawn(move || {
            wasm_bindgen_futures::spawn_local(async move {
                let mut sum = 0;
                while let Some(data_local) = rx.recv().await {
                    let s: u32 = data_local.iter().map(|&x| x as u32).sum();
                    sum += s;
                }
                o_tx.send(sum).await;
            })
        }));
        let Ok(()) = tx.send(data_local).await else {
            log::info!("failed to send to thread {t}");
            return "".to_owned();
        };
        inputs.push(tx);
        log::info!("data sent");
    }

    let mut id = inputs.len() - 1;
    for (id, input) in inputs.into_iter().enumerate() {
        log::info!("closing input {id}");
        drop(input);
    }

    for (id, handle) in threads.into_iter().enumerate() {
        let Ok(()) = handle.join_async().await else {
            log::info!("thread {id} joined with error");
            return "".to_owned();
        };
    }

    for (id, mut output) in outputs.into_iter().enumerate() {
        let s = output.recv().await.expect("failed to recv from thread {id}");
        total_ret += s;
        log::info!("thread {id} joined with {s}");
    }

    total_ret.to_string()
}

#[wasm_bindgen]
pub async fn clean_file(file: web_sys::File, endpoint: String, jwt_token: String, expiration: u64) -> String {
    log::info!("clean_file called with {file:?}, {endpoint}, {jwt_token}, {expiration}");

    let Ok(blob) = file.slice() else {
        log::info!("failed to convert a file to blob");
        return "".to_owned();
    };

    let Ok(mut reader) = BlobReader::new(blob) else {
        log::info!("failed to get a reader for blob");
        return "".to_owned();
    };

    let config = TranslatorConfig {
        data_config: DataConfig {
            endpoint,
            compression: Some(CompressionScheme::LZ4),
            auth: AuthConfig::maybe_new(Some(jwt_token), Some(expiration), None),
            prefix: "default".to_owned(),
        },
        shard_config: ShardConfig {
            prefix: "default-merkledb".to_owned(),
            repo_salt: RepoSalt::default(),
        },
    };

    let upload_session = Arc::new(FileUploadSession::new(Arc::new(config)));

    let mut handle = upload_session.start_clean("".to_string());

    const READ_BUF_SIZE: usize = 8 * 1024 * 1024;
    let mut buf = vec![0u8; READ_BUF_SIZE];
    let mut total_read = 0;
    loop {
        let Ok(bytes) = reader.read(&mut buf).await else {
            log::info!("failed to read from reader");
            return "".to_owned();
        };
        if bytes == 0 {
            break;
        }

        total_read += bytes;

        log::info!("adding {bytes} bytes to cleaner");

        let Ok(()) = handle.add_data(&buf[0..bytes]).await else {
            log::info!("failed to add data into cleaner");
            return "".to_owned();
        };

        log::info!("processed {total_read} bytes");
    }
    let Ok((file_hash, metrics)) = handle.finish().await else {
        log::info!("failed to finish cleaner");
        return "".to_owned();
    };

    log::info!("cleaner finished with {file_hash}");

    let Ok(()) = upload_session.finalize().await else {
        log::info!("failed to finalize upload session");
        return "".to_owned();
    };

    file_hash.to_string()
}
