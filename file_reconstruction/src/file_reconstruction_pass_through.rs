impl FileReconstructor {
    pub fn new(client: Arc<dyn Client>) -> Self {
        Self { client }
    }

    pub async fn reconstruct_file(
        &self,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        output_provider: SequentialOutput,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64> {
        let bytes_written = self
            .client
            .clone()
            .get_file_with_sequential_writer(hash, byte_range, output_provider, progress_updater)
            .await?;
        Ok(bytes_written)
    }

    pub async fn reconstruct_file_to_path(
        &self,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        output_path: impl AsRef<Path>,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64> {
        let output = sequential_output_from_filepath(output_path)?;
        self.reconstruct_file(hash, byte_range, output, progress_updater).await
    }
}
