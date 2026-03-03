use std::time::Duration;

use utils::ByteSize;

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
    /// The default value is 2GB.
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_SIZE` to set this value.
    ref download_buffer_size: ByteSize = ByteSize::from("2gb");

    /// The additional download buffer allocated per active file download.
    /// Each active file download increases the total buffer by this amount.
    ///
    /// The default value is 512MB.
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_PERFILE_SIZE` to set this value.
    ref download_buffer_perfile_size: ByteSize = ByteSize::from("512mb");

    /// The maximum total download buffer allowed during file reconstruction.
    /// The buffer will not grow beyond this limit regardless of the number of concurrent downloads.
    ///
    /// The default value is 8GB.
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_LIMIT` to set this value.
    ref download_buffer_limit: ByteSize = ByteSize::from("8gb");

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
