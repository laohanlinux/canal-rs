use zookeeper::{ZkResult, ZooKeeper, Watch, WatchedEvent, Watcher, ZkError};
use futures::{future, Sink, SinkExt, Stream, StreamExt, TryFutureExt};
use failure::{Error as FailureError, Fail};
use tokio::sync::oneshot::{self, Receiver, Sender};
use tokio::task;
use tokio::runtime::Runtime;
use serde::{Serialize, Deserialize};
use serde_json::{Serializer, Deserializer};

use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::str::FromStr;
use std::net::SocketAddr;
use crate::{Config, Client};
use crate::protobuf::CanalProtocol::Messages;

pub struct Cluster {
    config: Config,
    node: ClusterNode,
    client: Client,
}

impl Cluster {
    pub async fn new(config: Config, zk_addr: String, zk_timeout: Duration) -> Result<Cluster, FailureError> {
        let node = ClusterNode::new(zk_addr.as_str(), config.destinations.clone(), zk_timeout).await?;
        let running_node: RunningNode = node.get_active_canal_config()?;
        let mut client = crate::Client::new(running_node.address.parse().unwrap(), config.clone());
        client.connect().await?;
        Ok(Cluster {
            node,
            client,
            config,
        })
    }

    pub async fn get_without_ack(&mut self, batch_size: i32, timeout: Option<i64>, uints: Option<i32>) -> Result<Messages, FailureError> {
        let ret = self.client.get_without_ack(batch_size, timeout, uints).await?;
        self.fix_connect().await?;
        Ok(ret)
    }

    pub async fn get(&mut self, batch_size: i32, timeout: Option<i64>, uints: Option<i32>) -> Result<Messages, FailureError> {
        let ret = self.client.get(batch_size, timeout, uints).await?;
        self.fix_connect().await?;
        Ok(ret)
    }
    pub async fn subscribe(&mut self, filter: String) -> Result<(), FailureError> {
        let ret = self.client.subscribe(filter).await?;
        self.fix_connect().await?;
        Ok(ret)
    }

    pub async fn unsubscribe(&mut self, filter: String) -> Result<(), FailureError> {
        let ret = self.client.unsubscribe(filter).await?;
        self.fix_connect().await?;
        Ok(ret)
    }

    pub async fn ack(&mut self, batch_id: i64) -> Result<(), FailureError> {
        let ret = self.client.ack(batch_id).await?;
        self.fix_connect().await?;
        Ok(ret)
    }

    pub async fn disconnect(&mut self) -> Result<(), FailureError> {
        self.client.disconnect().await
    }

    async fn fix_connect(&mut self) -> Result<(), FailureError> {
        let running_node = self.node.get_active_canal_config()?;
        let socket = running_node.address.parse()?;
        let mut client = crate::Client::new(socket, self.config.clone());
        client.connect().await?;
        self.client = client;
        Ok(())
    }
}

#[derive(Clone)]
pub struct ClusterNode {
    destinations: String,
    zk: Arc<Mutex<ZooKeeper>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RunningNode {
    #[serde(alias = "active")]
    active: bool,
    #[serde(alias = "address")]
    address: String,
}

struct IgnoreLogWatch;

impl Watcher for IgnoreLogWatch {
    fn handle(&self, _: WatchedEvent) {}
}

impl Watcher for ClusterNode {
    fn handle(&self, e: WatchedEvent) {
        debug!("receive a event: {:?}", e);
    }
}

impl ClusterNode {
    pub async fn new(addr: &str, destinations: String, timeout: Duration) -> Result<ClusterNode, FailureError> {
        match ZooKeeper::connect(addr, timeout, IgnoreLogWatch) {
            Ok(zk) => {
                let zk = Arc::new(Mutex::new(zk));
                let active_path = format!("/otter/canal/destinations/{}/running", destinations.clone());
                let cluster = ClusterNode {
                    destinations: destinations.clone(),
                    zk: zk.clone(),
                };
                ClusterNode::get_active_canal_config_watch(zk.clone(), &destinations, cluster.clone()).await?;
                Ok(cluster)
            }
            Err(e) => {
                bail!("{:?}", e)
            }
        }
    }

    pub(crate) fn get_active_canal_config(&self) -> Result<RunningNode, FailureError> {
        let zk = self.zk.lock().unwrap();
        let cluster_path = format!("/otter/canal/destinations/{}/cluster", self.destinations);
        let active_path = format!("/otter/canal/destinations/{}/running", self.destinations);
        zk.get_children(cluster_path.as_str(), false)?.iter().for_each(|node| { debug!("canal node: {:?}", node) });
        let (data, _) = zk.get_data(active_path.as_str(), false)?;
        serde_json::from_slice(&data).map_err(|err| err.into())
    }

    async fn get_active_canal_config_watch<W: Watcher + 'static>(zk: Arc<Mutex<ZooKeeper>>, destinations: &String, watcher: W) -> Result<RunningNode, FailureError> {
        let zk = zk.lock().unwrap();
        let cluster_path = format!("/otter/canal/destinations/{}/cluster", destinations);
        let active_path = format!("/otter/canal/destinations/{}/running", destinations);
        zk.get_children(cluster_path.as_str(), false)?.iter().for_each(|node| { debug!("canal node: {:?}", node); });
        let (data, _) = zk.get_data_w(active_path.as_str(), watcher)?;
        serde_json::from_slice(&data).map_err(|err| err.into())
    }
}