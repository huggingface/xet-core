use std::time::Duration;

use crate::utils::ByteSize;

crate::config_group!({

    /// The minimum size of a single fetch request during reconstruction.
    /// Individual fetches will request reconstruction terms representing at least this amount of data.
    ///
    /// The default value is 256MB.
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_MIN_RECONSTRUCTION_FETCH_SIZE` to set this value.
    ref min_reconstruction_fetch_size: ByteSize = ByteSize::from("256mb");

    /// The maximum size of a single fetch request during reconstruction.
    /// Individual fetches will not request reconstruction terms representing more than this amount of data.
    ///
    /// The default value is 8GB.
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_MAX_RECONSTRUCTION_FETCH_SIZE` to set this value.
    ref max_reconstruction_fetch_size: ByteSize = ByteSize::from("8gb");

    /// The amount of download buffer always available for file reconstruction.
    /// The full buffer size will be this plus the number of simultaneous active
    /// file downloads times the per file size up to the global limit of
    /// download_buffer_limit.
    ///
    /// The default is derived from usable memory (the minimum of host RAM and the
    /// effective cgroup limit): usable/16, clamped to [64MB, 16GB]. If memory cannot
    /// be determined, or derivation is disabled via
    /// `HF_XET_DISABLE_MEMORY_DERIVED_DOWNLOAD_BUFFERS=1`, the default is 2GB.
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_SIZE` to set this value.
    ref download_buffer_size: ByteSize = crate::utils::system_memory::default_download_buffer_sizes().size;

    /// The additional download buffer allocated per active file download.
    /// Each active file download increases the total buffer by this amount.
    ///
    /// The default is derived from usable memory: usable/64, clamped to [16MB, 2GB].
    /// If memory cannot be determined, or derivation is disabled via
    /// `HF_XET_DISABLE_MEMORY_DERIVED_DOWNLOAD_BUFFERS=1`, the default is 512MB.
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_PERFILE_SIZE` to set this value.
    ref download_buffer_perfile_size: ByteSize = crate::utils::system_memory::default_download_buffer_sizes().perfile;

    /// The maximum total download buffer allowed during file reconstruction.
    /// The buffer will not grow beyond this limit regardless of the number of concurrent downloads.
    ///
    /// The default is derived from usable memory: usable/4, clamped to [264MB, 64GB].
    /// If memory cannot be determined, or derivation is disabled via
    /// `HF_XET_DISABLE_MEMORY_DERIVED_DOWNLOAD_BUFFERS=1`, the default is 8GB.
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_LIMIT` to set this value.
    ref download_buffer_limit: ByteSize = crate::utils::system_memory::default_download_buffer_sizes().limit;

    /// The half-life in count of observations for the exponentially weighted moving average used to estimate
    /// completion rate during reconstruction prefetching.
    ///
    /// The default value is 4 observations..
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_COMPLETION_RATE_ESTIMATOR_HALF_LIFE` to set this value.
    ref completion_rate_estimator_half_life: f64 = 4.;

    /// The target time for completing a prefetch block during reconstruction.
    /// This is used to determine how much data to prefetch ahead.
    ///
    /// The default value is 15 minutes.
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_TARGET_BLOCK_COMPLETION_TIME` to set this value.
    ref target_block_completion_time: Duration = Duration::from_secs(15 * 60);

    /// The minimum size of the prefetch buffer during reconstruction.
    /// The prefetch system will maintain terms representing at least this much always prefetched,
    /// no matter the estimated completion time.
    ///
    /// The default value is 1gb.
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_MIN_PREFETCH_BUFFER` to set this value.
    ref min_prefetch_buffer: ByteSize = ByteSize::from("1gb");

    /// Whether to use vectorized writes (write_vectored) during file reconstruction.
    /// When true, multiple pending writes are batched and written using write_vectored.
    /// When false, standard sequential writes are used.
    ///
    /// The default value is true.
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_USE_VECTORED_WRITE` to set this value.
    ref use_vectored_write: bool = true;

});

impl ConfigValueGroup {
    /// Ensure the download buffer values are mutually coherent: `download_buffer_limit`
    /// must be at least `download_buffer_size`, since the buffer semaphore uses the two
    /// as its (floor, ceiling) bounds. A user raising only the size (e.g. via the env
    /// var) gets the limit raised to match rather than a panic at context construction.
    pub fn normalize(&mut self) {
        if self.download_buffer_limit < self.download_buffer_size {
            tracing::warn!(
                "download_buffer_limit ({}) is below download_buffer_size ({}); raising the limit to match",
                self.download_buffer_limit,
                self.download_buffer_size
            );
            self.download_buffer_limit = self.download_buffer_size;
        }
    }
}
