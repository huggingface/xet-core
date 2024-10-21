use data::configurations::*;
use data::CacheConfig;
use data::errors;
use std::env::current_dir;
use std::fs;
use std::sync::Arc;
use utils::auth::{AuthConfig, TokenRefresher};

pub const SMALL_FILE_THRESHOLD: usize = 1;

pub fn default_config(
    endpoint: String,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
) -> errors::Result<TranslatorConfig> {
    let path = current_dir()?.join(".xet");
    fs::create_dir_all(&path)?;

    let (token, token_expiration) = token_info.unzip();
    let auth_cfg = AuthConfig::maybe_new(token, token_expiration, token_refresher);

    let translator_config = TranslatorConfig {
        file_query_policy: FileQueryPolicy::ServerOnly,
        cas_storage_config: StorageConfig {
            endpoint: Endpoint::Server(endpoint.clone()),
            auth: auth_cfg.clone(),
            prefix: "default".into(),
            cache_config: Some(CacheConfig {
                cache_directory: path.join("cache"),
                cache_size: 10 * 1024 * 1024 * 1024, // 10 GiB
            }),
            staging_directory: None,
        },
        shard_storage_config: StorageConfig {
            endpoint: Endpoint::Server(endpoint),
            auth: auth_cfg,
            prefix: "default-merkledb".into(),
            cache_config: Some(CacheConfig {
                cache_directory: path.join("shard-cache"),
                cache_size: 0,      // ignored
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
