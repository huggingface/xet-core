use std::sync::Arc;

use cas_object::CompressionScheme;
use futures::AsyncReadExt;
use hf_xet_wasm::blob_reader::BlobReader;
use hf_xet_wasm::configurations::{DataConfig, ShardConfig, TranslatorConfig};
use hf_xet_wasm::wasm_file_upload_session::FileUploadSession;
use hf_xet_wasm::wasm_timer::ConsoleTimer;
use log::Level;
use tokio::sync::mpsc;
use utils::auth::AuthConfig;
use wasm_bindgen::prelude::*;
use wasm_thread as thread;

fn main() {
    #[cfg(target_arch = "wasm32")]
    {
        console_log::init_with_level(Level::Info).unwrap();
        console_error_panic_hook::set_once();
    }

    #[cfg(not(target_arch = "wasm32"))]
    env_logger::init_from_env(env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"));

    log::info!("Starting init hf_xet_wasm...");

    log::info!("Done");
}

#[wasm_bindgen]
pub async fn thread_async_channel() -> String {
    // Exchange a series of messages over async channel.
    let (thread_tx, mut main_rx) = mpsc::channel::<String>(2);
    let (main_tx, mut thread_rx) = mpsc::channel::<String>(2);

    thread::spawn(|| {
        futures::executor::block_on(async move {
            thread::sleep(std::time::Duration::from_millis(100));
            thread_tx.send("Hello".to_string()).await.unwrap();
            let mut msg = thread_rx.recv().await.unwrap();
            msg.push_str("!");
            thread_tx.send(msg).await.unwrap();
        })
    });

    let mut msg = main_rx.recv().await.unwrap();
    msg.push_str(" world");
    main_tx.send(msg).await.unwrap();

    let result = main_rx.recv().await.unwrap();
    assert_eq!(result, "Hello world!");

    result
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
                let _ = o_tx.send(sum).await;
            })
        }));
        let Ok(()) = tx.send(data_local).await else {
            log::info!("failed to send to thread {t}");
            return "".to_owned();
        };
        inputs.push(tx);
        log::info!("data sent");
    }

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
    log::debug!("clean_file called with {file:?}, {endpoint}, {jwt_token}, {expiration}");

    let _timer = ConsoleTimer::new_enforce_report("clean file main");

    let filename = file.name();
    let filesize = file.size();

    let Ok(blob) = file.slice() else {
        log::error!("failed to convert a file to blob for file: {filename}");
        return "".to_owned();
    };

    let Ok(mut reader) = BlobReader::new(blob) else {
        log::error!("failed to get a reader for blob");
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
        },
        session_id: uuid::Uuid::new_v4().to_string(),
    };

    let upload_session = Arc::new(FileUploadSession::new(Arc::new(config)));

    let mut handle = upload_session.start_clean(0, None);

    const READ_BUF_SIZE: usize = 8 * 1024 * 1024;
    let mut buf = vec![0u8; READ_BUF_SIZE];
    let mut total_read = 0;
    let mut last_report = 0.;
    loop {
        let _timer = ConsoleTimer::new(format!("read file at {total_read}"));
        let Ok(bytes) = reader.read(&mut buf).await else {
            log::error!("failed to read from reader");
            return "".to_owned();
        };
        drop(_timer);
        if bytes == 0 {
            break;
        }

        total_read += bytes;

        log::debug!("adding {bytes} bytes to cleaner");

        let Ok(()) = handle.add_data(&buf[0..bytes]).await else {
            log::error!("failed to add data into cleaner");
            return "".to_owned();
        };

        log::debug!("read {total_read} bytes");

        let percentage = total_read as f64 / filesize * 100.;
        if (percentage - last_report) > 10. {
            log::info!("processing {percentage:.2}% of file");
            last_report = percentage;
        }
    }
    let Ok((file_hash, sha256, _metrics)) = handle.finish().await else {
        log::error!("failed to finish cleaner");
        return "".to_owned();
    };

    log::debug!("cleaner finished with xet hash {file_hash}, sha256 {sha256}");

    let Ok(()) = upload_session.finalize().await else {
        log::error!("failed to finalize upload session");
        return "".to_owned();
    };

    sha256.to_string()
}
