use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use cas_types::{FileRange, QueryReconstructionResponse};
use merklehash::MerkleHash;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;

use crate::Client;
use crate::error::Result;

pub struct XorbData {
    pub fetch_url: String,
    pub url_age: Instant,
    pub xorb_range: (u32, u32),
    pub xorb_hash: MerkleHash,
    pub retrieved_data: RwLock<Option<Bytes>>,
}

#[derive(Clone)]
pub struct FileXorbTerm {
    pub term_hash: MerkleHash,
    pub range_within_xorb_data: (u32, u32),
    pub xorb_data: Arc<XorbData>,
}


struct FileXorb { 





}

#[derive(Default)]
struct RetrivedXorbTerms {
    

    // The index of file reconstruction terms at which the vector of terms starts.
    terms_vec_offset: usize,

    // If the final index is known, it's placed here. 
    final_index : Option<usize>,

    // The data in the deque that  
    terms: VecDeque<FileXorbTerm>,
}




pub struct XorbTermRetriever {
    retrieved_terms: Mutex<RetrivedXorbTerms>,

    target_block_size: u64,

    target_prefetch_size: u64,

    client: u64,
}


impl XorbTermRetriever {
    pub fn new(client: Arc<dyn Client>) -> Arc<Self> {
        todo!();
    }

    pub async fn get_next_term() -> Result<Option<(usize, FileXorbTerm)>> {

        // If the term has been retrieved, return that term.
        todo!()
    }

    pub async fn terms_



    /// Retrieve the next prefetch block size. 
    async fn retrieve_next_terms() -> Result<()> {
        todo!()

    }


    fn map_file_info_to_download_terms. 


}


