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

#[derive(Clone)]
pub struct Cluster {
    destinations: String,
    zk: Arc<Mutex<ZooKeeper>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct RunningNode {
    #[serde(alias = "active")]
    Active: bool,
    #[serde(alias = "address")]
    Address: String,
}

struct ignoreLogWatch;

impl Watcher for ignoreLogWatch {
    fn handle(&self, e: WatchedEvent) {}
}

impl Watcher for Cluster {
    fn handle(&self, e: WatchedEvent) {}
}

impl Cluster {
    pub async fn new(addr: &str, destinations: String, timeout: Duration) -> Result<Cluster, FailureError> {
        match ZooKeeper::connect(addr, timeout, ignoreLogWatch) {
            Ok(zk) => {
                let zk = Arc::new(Mutex::new(zk));
                let active_path = format!("/otter/canal/destinations/{}/running", destinations.clone());
                let mut cluster = Cluster {
                    destinations: destinations.clone(),
                    zk: zk.clone(),
                };
                let node = Cluster::get_active_canal_config_watch(zk.clone(), &destinations, cluster.clone()).await?;
                Ok(cluster)
            }
            Err(e) => {
                bail!("{:?}", e)
            }
        }
    }

    fn get_active_canal_config_by_zk(&self) -> Result<RunningNode, FailureError> {
        let zk = self.zk.lock().unwrap();
        let cluster_path = format!("/otter/canal/destinations/{}/cluster", self.destinations);
        let active_path = format!("/otter/canal/destinations/{}/running", self.destinations);
        zk.get_children(cluster_path.as_str(), false).map(|nodes| {
            nodes.iter().for_each(|node| { debug!("canal node: {:?}", node) });
        }).map_err::<FailureError, _>(|err| err.into())?;
        zk.get_data(active_path.as_str(), false).map(|(value, _)| {
            serde_json::from_slice(&value).unwrap()
        }).map_err(|err| err.into())
    }

    async fn get_active_canal_config_watch<W: Watcher + 'static> (zk: Arc<Mutex<ZooKeeper>>, destinations: &String, watcher: W) -> Result<RunningNode, FailureError> {
        let zk = zk.lock().unwrap();
        let cluster_path = format!("/otter/canal/destinations/{}/cluster", destinations);
        let active_path = format!("/otter/canal/destinations/{}/running", destinations);
        zk.get_children(cluster_path.as_str(), false).map(|nodes| {
            nodes.iter().for_each(|node| { debug!("canal node: {:?}", node); });
        }).map_err::<FailureError, _>(|err| {
            err.into()
        })?;
        zk.get_data_w(active_path.as_str(), watcher).map(|(value, _)| {
            serde_json::from_slice(&value).unwrap()
        }).map_err(|err| err.into())
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;
    use tokio::prelude::*;
    use tokio::task;
    use tokio::runtime::Runtime;

    #[test]
    fn it_works() {
        // Create the runtime
        let mut rt = Runtime::new().unwrap();
        let mut cluster = super::Cluster::new("remote.dev.com:2181", "example".to_string(), Duration::from_secs(3));
        rt.block_on(async move {
            let mut cluster = super::Cluster::new("remote.dev.com:2181", "example".to_string(), Duration::from_secs(3));
        });
    }
}