crate::config::macros::xet_config_group!(session, {

    /// Subdirectory name for shard session data within the staging directory.
    ///
    /// The default value is "shard-session".
    ///
    /// Use the environment variable `HF_XET_SESSION_DIR_NAME` to set this value.
    ref dir_name: String = "shard-session".to_string();
});
