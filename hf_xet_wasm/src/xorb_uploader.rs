use std::mem::take;
// #![cfg(target_arch = "wasm32")]
use std::result::Result as stdResult;
use std::sync::Arc;

use cas_client::{CasClientError, Client};
use merklehash::MerkleHash;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{self};
use wasm_thread as thread;

use crate::errors::*;

pub struct XorbUploader {
    input_queues: Vec<mpsc::Sender<XorbUploadType>>,
    output_queues: Vec<mpsc::Receiver<stdResult<usize, CasClientError>>>,
    workers: Vec<thread::JoinHandle<()>>,

    index: usize,
}

type XorbUploadType = (MerkleHash, Vec<u8>, Vec<(MerkleHash, u32)>);

impl XorbUploader {
    pub fn new(client: Arc<dyn Client + Send + Sync>, cas_prefix: &str, upload_concurrency: usize) -> Self {
        let mut input_queues = vec![];
        let mut output_queues = vec![];
        let mut workers = vec![];
        for _ in 0..upload_concurrency {
            let (input_tx, input_rx) = mpsc::channel(2);
            let (output_tx, output_rx) = mpsc::channel(2);
            input_queues.push(input_tx);
            output_queues.push(output_rx);
            workers.push(upload_worker(input_rx, output_tx, client.clone(), cas_prefix.to_owned()));
        }

        Self {
            input_queues,
            output_queues,
            workers,
            index: 0,
        }
    }

    pub async fn upload_xorb(&mut self, input: XorbUploadType) -> Result<()> {
        log::info!("uploading one xorb");
        // get the results
        loop {
            let ret = self.output_queues[self.index].try_recv();
            match ret {
                Ok(internal_ret) => {
                    let _ = internal_ret?;
                },
                Err(TryRecvError::Empty) => {
                    log::info!("output queue of worker {} now empty", self.index);
                    break;
                },
                Err(e) => {
                    return Err(DataProcessingError::internal(e));
                },
            }
        }

        // round-robin scheduling
        self.input_queues[self.index]
            .send(input)
            .await
            .map_err(DataProcessingError::internal);

        self.index = (self.index + 1) % self.workers.len();

        Ok(())
    }

    pub async fn finalize(mut self) -> Result<()> {
        for (id, input) in take(&mut self.input_queues).into_iter().enumerate() {
            log::info!("closing input {id}");
            drop(input);
        }

        for (id, handle) in take(&mut self.workers).into_iter().enumerate() {
            log::info!("joining worker {id}");
            handle.join_async().await.map_err(DataProcessingError::internal)?;
        }

        for (id, mut output) in take(&mut self.output_queues).into_iter().enumerate() {
            log::info!("draining output {id}");
            while let Some(ret) = output.recv().await {
                log::info!("output recving...");
                ret?;
            }
        }

        Ok(())
    }
}

fn upload_worker(
    mut input: mpsc::Receiver<XorbUploadType>,
    output: mpsc::Sender<stdResult<usize, CasClientError>>,
    client: Arc<dyn Client + Send + Sync>,
    cas_prefix: String,
) -> thread::JoinHandle<()> {
    wasm_thread::spawn(move || {
        wasm_bindgen_futures::spawn_local(async move {
            log::info!("worker waiting for message");
            while let Some((xorb_hash, xorb_data, chunks_and_boundaries)) = input.recv().await {
                log::info!("worker get value message, uploading");
                let ret = client.put(&cas_prefix, &xorb_hash, xorb_data, chunks_and_boundaries).await;

                output.send(ret).await;
            }
        })
    })
}
