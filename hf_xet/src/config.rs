use std::env::current_dir;
use std::fs;
use data::configurations::{Auth, CacheConfig, DedupConfig, Endpoint, FileQueryPolicy, RepoInfo, StorageConfig, TranslatorConfig};
use data::{DEFAULT_BLOCK_SIZE, errors};

pub const SMALL_FILE_THRESHOLD: usize = 1;

pub fn default_config(endpoint: String, token: Option<String>) -> errors::Result<TranslatorConfig> {
    let path = current_dir()?.join(".xet");
    fs::create_dir_all(&path)?;

    let translator_config = TranslatorConfig {
        file_query_policy: FileQueryPolicy::ServerOnly,
        cas_storage_config: StorageConfig {
            endpoint: Endpoint::Server(endpoint.clone()),
            auth: Auth {
                token: token.clone(),
            },
            prefix: "default".into(),
            cache_config: Some(CacheConfig {
                cache_directory: path.join("cache"),
                cache_size: 10 * 1024 * 1024 * 1024, // 10 GiB
                cache_blocksize: DEFAULT_BLOCK_SIZE,
            }),
            staging_directory: None,
        },
        shard_storage_config: StorageConfig {
            endpoint: Endpoint::Server(endpoint),
            auth: Auth {
                token: token,
            },
            prefix: "default-merkledb".into(),
            cache_config: Some(CacheConfig {
                cache_directory: path.join("shard-cache"),
                cache_size: 0,      // ignored
                cache_blocksize: 0, // ignored
            }),
            staging_directory: Some(path.join("shard-session")),
        },
        dedup_config: Some(DedupConfig {
            repo_salt: None,
            small_file_threshold: SMALL_FILE_THRESHOLD,
            global_dedup_policy: Default::default(),
        }),
        repo_info: Some(RepoInfo {
            repo_paths: vec!["".into()],
        }),
    };

    translator_config.validate()?;

    Ok(translator_config)
}
