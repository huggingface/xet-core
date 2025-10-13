use hub_client::{BearerCredentialHelper, HFRepoType, HubClient, HubRepositoryTrait, RepoInfo};

#[tokio::main]
async fn main() {
    let token = std::env::var("HF_TOKEN").unwrap();

    let client = HubClient::new(
        "https://huggingface.co",
        RepoInfo::new(HFRepoType::Model, "assafvayner/unsafetensors_new".to_string()),
        None,
        "",
        "",
        BearerCredentialHelper::new(token.to_string(), "assafvayner"),
    )
    .unwrap();

    let paths = client.list_files("").await.unwrap();
    println!("{:?}", paths);

    println!();
    for entry in &paths {
        let Some(dir) = entry.as_directory() else {
            continue;
        };
        let sub_paths = client.list_files(dir.path.as_str()).await.unwrap();
        println!("sub paths of {}", dir.path);
        println!("{:?}", sub_paths);
        println!();
    }
}
