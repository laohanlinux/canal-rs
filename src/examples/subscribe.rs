#[macro_use]
extern crate log;

use canal_rs::Client;
use canal_rs::DbConfig;
use canal_rs::protobuf::EntryProtocol::Entry;
use canal_rs::protobuf::CanalProtocol::ClientAuth_oneof_net_read_timeout_present;
use canal_rs::protobuf::CanalProtocol::ClientAuth_oneof_net_write_timeout_present;
use protobuf::parse_from_bytes;

use tokio::prelude::*;
use tokio::task;
use tokio::time::{self, delay_for};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), String> {
    pretty_env_logger::init();

    let conf = DbConfig::new("".to_string(), "".to_string(), ClientAuth_oneof_net_read_timeout_present::net_read_timeout(0), ClientAuth_oneof_net_write_timeout_present::net_write_timeout(0));

    let mut client: Client = Client::new("127.0.0.1:11111".parse().unwrap(), conf);
    client.connect().await.unwrap();
    client.subscribe(&".*".to_string()).await.unwrap();
    let join = task::spawn(async move {
        while let message = client.get(100, Some(10), Some(10)).await.unwrap() {
            if message.batch_id == -1 {
                delay_for(Duration::from_secs(1)).await;
                continue;
            }
            debug!("batch_id: {:?}", message.batch_id);
            for buf in &message.messages {
                let entry: Entry = parse_from_bytes(&buf).unwrap();
                println!("{:?}", entry);
            }
            delay_for(Duration::from_secs(1)).await;
        }
    });

    let ret = join.await;
    Ok(())
}
