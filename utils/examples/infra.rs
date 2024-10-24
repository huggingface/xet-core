use clap::Parser;
use http::Uri;
use tonic::transport::Channel;
use utils::common::Empty;
use utils::consistenthash::ConsistentHash;
use utils::infra::infra_utils_client::InfraUtilsClient;

pub type InfraUtilsClientType = InfraUtilsClient<Channel>;
pub async fn get_infra_client(server_name: &str) -> anyhow::Result<InfraUtilsClientType> {
    let mut server_uri: Uri = server_name.parse()?;

    // supports an absolute URI (above) or just the host:port (below)
    if server_uri.scheme().is_none() {
        server_uri = format!("https://{}", server_name).parse().unwrap();
    }

    let channel = Channel::builder(server_uri).connect().await?;
    Ok(InfraUtilsClient::new(channel))
}

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser)]
    server_name: String,

    #[clap(short, long, value_parser)]
    key: Option<String>,
}
#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut client = get_infra_client(&args.server_name).await.unwrap();
    let request = Empty {};
    let response = client.endpoint_load(request).await.unwrap();
    if let Some(key) = args.key {
        let ch = ConsistentHash::new(response.into_inner().responses).unwrap();

        println!("Key {} gets hashed to server {:?}", &key, ch.server(&key).unwrap());
        return;
    }
    // when no key is specified, print out the entire response
    for load_status in response.into_inner().responses.into_iter() {
        if let Some(load_stat) = load_status.status {
            println!("Host: {}", &load_status.address);
            println!("Load Stats: {:?}", load_stat);
        }
    }
}
