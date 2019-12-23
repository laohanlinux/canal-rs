use canal_rs::Client;
use canal_rs::DbConfig;
use canal_rs::protobuf::CanalProtocol::ClientAuth_oneof_net_read_timeout_present;
use canal_rs::protobuf::CanalProtocol::ClientAuth_oneof_net_write_timeout_present;


use tokio::prelude::*;
use tokio::task;

#[tokio::main]
async fn main () -> Result<(), String>{
    pretty_env_logger::init();

    let conf = DbConfig::new("root".to_string(), "".to_string(), ClientAuth_oneof_net_read_timeout_present::net_read_timeout(10), ClientAuth_oneof_net_write_timeout_present::net_write_timeout(10));

    let mut client = Client::new("127.0.0.1:11111".parse().unwrap(), conf);

    let join = task::spawn(async move {
        let ret = client.connect().await;
        println!("{:?}", ret);
    });

    let ret = join.await;
    Ok(())
}