use seahash::hash;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Clone, Deserialize)]
pub struct Node {
    pub id: String,
    pub addr: String, // "host:port"
}

#[derive(Debug, Clone)]
pub struct Cluster {
    pub me: Node,
    pub nodes: Arc<Vec<Node>>,
}

impl Cluster {
    /// env:
    /// QBUS_NODE_ID="node-a"
    /// QBUS_NODES='[{"id":"node-a","addr":"127.0.0.1:7001"},{"id":"node-b","addr":"127.0.0.1:7002"}]'
    pub fn from_env() -> anyhow::Result<Self> {
        let me_id = std::env::var("QBUS_NODE_ID")?;
        let nodes_json = std::env::var("QBUS_NODES")?;
        let nodes: Vec<Node> = serde_json::from_str(&nodes_json)?;
        let me = nodes
            .iter()
            .find(|n| n.id == me_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("me id not in QBUS_NODES"))?;
        Ok(Self {
            me,
            nodes: Arc::new(nodes),
        })
    }

    /// Rendezvous hashing: 가장 큰 hash(node, topic, partition)
    pub fn leader_of(&self, topic: &str, partition: u32) -> Node {
        let mut best: Option<(&Node, u64)> = None;
        for n in self.nodes.iter() {
            let key = format!("{}:{}:{}", n.id, topic, partition);
            let score = hash(key.as_bytes());
            if best.map(|(_, s)| score > s).unwrap_or(true) {
                best = Some((n, score));
            }
        }
        best.unwrap().0.clone()
    }

    pub fn is_leader(&self, topic: &str, partition: u32) -> bool {
        self.leader_of(topic, partition).id == self.me.id
    }
}
