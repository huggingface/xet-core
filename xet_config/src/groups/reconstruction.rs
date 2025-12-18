use utils::ByteSize;

crate::config_group!({

    /// The initial size of data to fetch when reconstructing a file.
    /// This is used to fetch the beginning of a file before the full reconstruction starts.
    ///
    /// The default value is 256MB.
    ///
    /// Use the environment variable `HF_XET_RECONSTRUCTION_INITIAL_RECONSTRUCTION_FETCH_SIZE` to set this value.
    ref initial_reconstruction_fetch_size: ByteSize = ByteSize::from("256mb");



});
