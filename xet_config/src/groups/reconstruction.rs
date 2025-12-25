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

    /// The maximum amount of data that can be buffered in flight during file reconstruction.
    /// This controls memory usage by limiting how much data can be downloaded but not yet written.
    ///
    /// The default value is 8GB.
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_SIZE` to set this value.
    ref download_buffer_size: ByteSize = ByteSize::from("8gb");

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

    /// Whether to use the V1 reconstruction algorithm.
    /// When true, the old FileReconstructorV1 is used for file reconstruction.
    /// When false, the new FileReconstructor (V2) is used.
    ///
    /// The default value is false.
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_USE_V1` to set this value.
    ref use_v1: bool = false;

    /// Whether to use vectorized writes (write_vectored) during file reconstruction.
    /// When true, multiple pending writes are batched and written using write_vectored.
    /// When false, standard sequential writes are used.
    ///
    /// Vectorized writes can improve performance for writers that benefit from batching,
    /// such as network sockets or buffered file writers.
    ///
    /// The default value is false.
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_USE_VECTORED_WRITE` to set this value.
    ref use_vectored_write: bool = true;
});
