#[macro_use]
extern crate log;

use canal_rs::Client;
use canal_rs::Config;
use canal_rs::Cluster;
use canal_rs::protobuf::EntryProtocol::{Entry, RowChange};
use canal_rs::protobuf::CanalProtocol::ClientAuth_oneof_net_read_timeout_present;
use canal_rs::protobuf::CanalProtocol::ClientAuth_oneof_net_write_timeout_present;
use canal_rs::protobuf::CanalProtocol::Messages;
use protobuf::parse_from_bytes;

use tokio::prelude::*;
use tokio::task;
use tokio::time::{self, delay_for};
use std::time::Duration;
use std::io;

use failure::Error as FailureError;
use zookeeper::ZkError;

#[tokio::main]
async fn main() -> Result<(), FailureError> {
    pretty_env_logger::init();

    let conf = Config::new("".to_string(), "".to_string(), "128".to_string(), "example".to_string(), ClientAuth_oneof_net_read_timeout_present::net_read_timeout(0), ClientAuth_oneof_net_write_timeout_present::net_write_timeout(0));
    let zk_addr = "remote.dev.com:2181".to_string();
    let timeout = Duration::from_secs(10);
    match Cluster::new(conf.clone(), zk_addr.clone(), timeout.clone()).await {
        Ok(_) => {}
        Err(e) => {
            debug!("{:?}", e.downcast::<ZkError>().unwrap());
        }
    }
    let mut cluster = Cluster::new(conf, zk_addr, timeout).await?;
    cluster.subscribe(".*".to_string()).await.unwrap();
    let join = task::spawn(async move {
        while let message = cluster.get(100, Some(10), Some(10)).await {
            match message {
                Ok(message) => {
                    output(message);
                    delay_for(Duration::from_secs(3));
                }
                Err(err) => {
                    if err.downcast_ref::<ZkError>().is_some() {
                        debug!("not found available node");
                        delay_for(Duration::from_secs(3));
                        continue;
                    } else if err.downcast_ref::<io::Error>().is_some() {
                        debug!("not found available node");
                        delay_for(Duration::from_secs(3));
                        continue;
                    }
                    break;
                }
            }
        }
    });

    let ret = join.await;
    Ok(())
}

fn output(message: Messages) {
    if message.batch_id == -1 {
        debug!("Empty data");
        return;
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
}