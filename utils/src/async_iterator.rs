use async_trait::async_trait;

#[async_trait]
pub trait AsyncIterator<E: Send + Sync + 'static>: Send + Sync {
    type Item: Send + Sync;

    /// The traditional next method for iterators, with a Result and Error
    /// type.  Returns None when everything is done.
    async fn next(&mut self) -> Result<Option<Self::Item>, E>;
}

#[async_trait]
pub trait BatchedAsyncIterator<E: Send + Sync + 'static>: AsyncIterator<E> {
    /// Return a block of items.  If the stream is done, then an empty vector is returned;
    /// otherwise, at least one item is returned.
    ///
    /// If given, max_num dictates the maximum number of items to return.  If None, then all
    /// available items are returned.
    async fn next_batch(&mut self, max_num: Option<usize>) -> Result<Vec<Self::Item>, E>;

    /// Returns the number of items remaining in the stream
    /// if known, and None otherwise.  Returns Some(0) if
    /// there are no items remaining.
    fn items_remaining(&self) -> Option<usize>;
}

#[async_trait]
impl<E: Send + Sync + 'static> AsyncIterator<E> for Vec<u8> {
    type Item = Vec<u8>;

    async fn next(&mut self) -> Result<Option<Self::Item>, E> {
        if self.is_empty() {
            Ok(None)
        } else {
            Ok(Some(std::mem::take(self)))
        }
    }
}
