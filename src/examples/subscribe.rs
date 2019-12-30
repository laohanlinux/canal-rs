#[macro_use]
extern crate log;

use canal_rs::Client;
use canal_rs::Config;
use canal_rs::protobuf::EntryProtocol::{Entry, RowChange};
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

    let conf = Config::new("".to_string(), "".to_string(), "128".to_string(), "example".to_string(), ClientAuth_oneof_net_read_timeout_present::net_read_timeout(0), ClientAuth_oneof_net_write_timeout_present::net_write_timeout(0));

    let mut client: Client = Client::new("111.229.233.174:11111".parse().unwrap(), conf);
    client.connect().await.unwrap();
    client.subscribe(".*".to_string()).await.unwrap();
    let join = task::spawn(async move {
        while let message = client.get(100, Some(10), Some(10)).await.unwrap() {
            if message.batch_id == -1 {
                debug!("Empty data");
                delay_for(Duration::from_secs(3)).await;
                continue;
            }
            debug!("batch_id: {:?}", message.batch_id);
            for buf in &message.messages {
                let entry: Entry = parse_from_bytes(&buf).unwrap();
                match parse_from_bytes::<RowChange>(entry.get_storeValue()) {
                    Ok(row_change) => {
                        debug!("row_change: {:?}", row_change);
                    }
                    _ => {}
                }
            }
            delay_for(Duration::from_secs(1)).await;
        }
    });

    let ret = join.await;
    Ok(())
}
