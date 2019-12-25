#[macro_use] extern crate log;

use canal_rs::Client;
use canal_rs::DbConfig;
use canal_rs::protobuf::CanalProtocol::ClientAuth_oneof_net_read_timeout_present;
use canal_rs::protobuf::CanalProtocol::ClientAuth_oneof_net_write_timeout_present;


use tokio::prelude::*;
use tokio::task;
use tokio::time::{self, Delay, timeout, timeout_at, Instant};
use std::time::{Duration};

#[tokio::main]
async fn main () -> Result<(), String>{
    pretty_env_logger::init();

    let conf = DbConfig::new("root".to_string(), "".to_string(), ClientAuth_oneof_net_read_timeout_present::net_read_timeout(10), ClientAuth_oneof_net_write_timeout_present::net_write_timeout(10));

    let mut client: Client = Client::new("127.0.0.1:11111".parse().unwrap(), conf);

    let join = task::spawn(async move {
        client.connect().await.unwrap();
        client.subscribe(&".*".to_string()).await.unwrap();
        while let message = client.get(100, Some(10), Some(10)).await.unwrap() {
            // if message.batch_id == -1 {
            //     time::advance(Duration::from_secs(3)).await;
            // }
            println!("{:?}, {:?}", message.batch_id, message.messages);
            break;
        }
    });

    let ret = join.await;
    Ok(())
}
