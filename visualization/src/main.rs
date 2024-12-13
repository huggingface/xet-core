use std::fs::File;
use std::io::{self, BufRead, BufReader, Lines};
use std::path::Path;
use std::sync::Arc;

use data::configurations::{default_config, TranslatorConfig};
use data::errors::DataProcessingError;

use merklehash::DataHash;
use utils::ThreadPool;

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

pub fn main() -> data::errors::Result<()> {
    /*
    let config = StorageConfig {
        endpoint: Endpoint::FileSystem(PathBuf::from("/Users/zach/hf/data")),
        auth: None,
        prefix: "default-merkledb".into(),
        cache_config: Some(CacheConfig {
            cache_directory: PathBuf::from("/tmp/xet_shard_cache"),
            cache_size: 1024 * 1024 * 1024,
        }),
        staging_directory: Some(PathBuf::from("/tmp/xet_shard_staging")),
    };
    let shard_manager_result = create_shard_manager(&config).await;
    let shard_manager = shard_manager_result?;
    //println!("Shard manager contains: {:#?}", shard_manager.to_vis_json().await);
    */

    let endpoint = "https://cas-server.us.dev.moon.huggingface.tech".to_string();
    let token_info = None;
    let token_refresher = None;
    let config = default_config(endpoint, token_info, token_refresher)?;
    let threadpool = Arc::new(ThreadPool::new());
    println!("Zach was here, about to read_lines");
    let lines = read_lines("./file_ids.txt")?;
    threadpool.block_on(query_for_file_ids(lines, config, threadpool.clone()))?;

    Ok(())
}

async fn query_for_file_ids(file_ids: Lines<BufReader<File>>, config: TranslatorConfig, threadpool: Arc<ThreadPool>) -> Result<(), DataProcessingError> {
    let remote_shards = {
        data::remote_shard_interface::RemoteShardInterface::new_query_only(
            data::configurations::FileQueryPolicy::ServerOnly,
            &config.cas_storage_config,
            threadpool.clone(),
        )
    }.await?;

    for maybe_file_id in file_ids {
        match maybe_file_id {
            Ok(file_id) => {
                let hash = DataHash::from_hex(&file_id)?;
                println!("Zach was here, hash is {:#?}", hash);
                let info = remote_shards.get_file_reconstruction_info(&hash).await?;
                println!("Zach was here, got reconstruction info {:#?}", info);
            },
            Err(_) => return Err(DataProcessingError::HashNotFound),
        }
    }

    Ok(())
}
