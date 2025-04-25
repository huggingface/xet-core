use futures::AsyncReadExt;
use wasm_bindgen::prelude::*;
use wasm_thread as thread;
use wasm_xet::blob_reader::BlobReader;

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
    let mut threads: Vec<thread::JoinHandle<u32>> = vec![];
    for t in 0..5 {
        let split = nbyte / 5;
        let data_local = total_bytes[split * t..split * (t + 1)].to_vec();
        threads.push(thread::spawn(move || {
            let sum = data_local.iter().map(|&x| x as u32).sum();
            sum
        }));
    }

    let mut ret = 0;
    for (id, handle) in threads.into_iter().enumerate() {
        let Ok(s) = handle.join_async().await else {
            log::info!("thread {id} joined with error");
            return "".to_owned();
        };
        ret += s;
        log::info!("thread {id} joined with {s}");
    }

    total_ret += ret;

    total_ret.to_string()
}
