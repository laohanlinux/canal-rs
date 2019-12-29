use zookeeper::{ZkResult, ZooKeeper, Watch, WatchedEvent, Watcher};

use futures::{future, Sink, SinkExt, Stream, StreamExt};
use failure::{Error as FailureError, Fail};

use std::time::Duration;
use serde::{Serialize, Deserialize};
use serde_json::{Serializer, Deserializer};
use std::sync::Arc;

use tokio::sync::oneshot::{self, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task;
use tokio::runtime::Runtime;

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

struct LogWatcher(Arc<Mutex<ZooKeeper>>, String);

impl Watcher for LogWatcher {
    fn handle(&self, e: WatchedEvent) {
        let mut rt = Runtime::new().unwrap();
        let zk = self.0.clone();
        rt.block_on(async move {
            let zk = zk.lock().await;
        });
    }
}

struct ignoreLogWatch;

impl Watcher for ignoreLogWatch {
    fn handle(&self, e: WatchedEvent) {
    }
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
                let watcher = LogWatcher(zk.clone(), active_path);
                let mut cluster = Cluster {
                    destinations: destinations.clone(),
                    zk: zk.clone(),
                };
                match Cluster::get_active_cancal_config(zk.clone(), &destinations, cluster).await {
                    Ok(node) => {
                        bail!("")
                    }
                    Err(e) => bail!("{:?}", e)
                }
            }
            Err(e) => {
                bail!("{:?}", e)
            }
        }
    }

    async fn get_active_cancal_config<W: Watcher + 'static>(zk: Arc<Mutex<ZooKeeper>>, destinations: &String, watcher: W) -> Result<RunningNode, FailureError> {
        let zk = zk.lock().await;
        let cluster_path = format!("/otter/canal/destinations/{}/cluster", destinations);
        let active_path = format!("/otter/canal/destinations/{}/running", destinations);
        match zk.get_children(cluster_path.as_str(), false) {
            Ok(nodes) => {
                nodes.iter().for_each(|node| {
                    debug!("{:?}", node);
                })
            }
            Err(e) => bail!("{:?}", e),
        }

        match zk.get_data_w(active_path.as_str(), watcher)
            {
                Ok(ret) => {
                    let value: RunningNode = serde_json::from_slice(&ret.0).unwrap();
                    Ok(value)
                }
                Err(e) => bail!("{:?}", e)
            }
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