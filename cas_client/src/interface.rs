use crate::error::Result;
use async_trait::async_trait;
use merklehash::MerkleHash;
use std::sync::Arc;

/// A Client to the CAS (Content Addressed Storage) service to allow storage and
/// management of XORBs (Xet Object Remote Block). A XORB represents a collection
/// of arbitrary bytes. These bytes are hashed according to a Xet Merkle Hash
/// producing a Merkle Tree. XORBs in the CAS are identified by a combination of
/// a prefix namespacing the XORB and the hash at the root of the Merkle Tree.
#[async_trait]
pub trait Client: core::fmt::Debug {
    /// Insert the provided data into the CAS as a XORB indicated by the prefix and hash.
    /// The hash will be verified on the server-side according to the chunk boundaries.
    /// Chunk Boundaries must be complete; i.e. the last entry in chunk boundary
    /// must be the length of data. For instance, if data="helloworld" with 2 chunks
    /// ["hello" "world"], chunk_boundaries should be [5, 10].
    /// Empty data and empty chunk boundaries are not accepted.
    ///
    /// Note that put may background in some implementations and a flush()
    /// will be needed.
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<()>;

    /// Clients may do puts in the background. A flush is necessary
    /// to enforce completion of all puts. If an error occured during any
    /// background put it will be returned here.
    async fn flush(&self) -> Result<()>;

    /// Reads all of the contents for the indicated XORB, returning the data or an error
    /// if an issue occurred.
    async fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>>;

    /// Reads the requested ranges for the indicated object. Each range is a tuple of
    /// start byte (inclusive) to end byte (exclusive). Will return the contents for
    /// the ranges if they exist in the order specified. If there are issues fetching
    /// any of the ranges, then an Error will be returned.
    async fn get_object_range(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>>;

    /// Gets the length of the XORB or an error if an issue occurred.
    async fn get_length(&self, prefix: &str, hash: &MerkleHash) -> Result<u64>;
}

/*
 * If T implements Client, Arc<T> also implements Client
 */
#[async_trait]
impl<T: Client + Sync + Send> Client for Arc<T> {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<()> {
        (**self).put(prefix, hash, data, chunk_boundaries).await
    }

    async fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>> {
        (**self).get(prefix, hash).await
    }

    /// Clients may do puts in the background. A flush is necessary
    /// to enforce completion of all puts. If an error occured during any
    /// background put it will be returned here.force completion of all puts.
    async fn flush(&self) -> Result<()> {
        (**self).flush().await
    }

    async fn get_object_range(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>> {
        (**self).get_object_range(prefix, hash, ranges).await
    }

    async fn get_length(&self, prefix: &str, hash: &MerkleHash) -> Result<u64> {
        (**self).get_length(prefix, hash).await
    }
}
