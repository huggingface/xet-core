use std::fs::File;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};

use data::configurations::{Endpoint, StorageConfig};
use data::shard_interface::create_shard_manager;
use data::CacheConfig;

use mdb_shard::shard_file_reconstructor::FileReconstructor;
use merklehash::DataHash;

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}


#[tokio::main]
async fn main() -> data::errors::Result<()> {
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

    let lines = read_lines("./file_ids.txt")?;
    // Consumes the iterator, returns an (Optional) String
    for line in lines.flatten() {
        let hash = DataHash::from_hex(&line)?;

        println!("Zach was here, hash is {:#?}", hash);
        let info = shard_manager.get_file_reconstruction_info(&hash).await?;
        println!("Zach was here, got reconstruction info {:#?}", info);
    }
    
    Ok(())
}
