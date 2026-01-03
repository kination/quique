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
    pub fn new(me_id: String, nodes: Vec<Node>) -> anyhow::Result<Self> {
        let me = nodes
            .iter()
            .find(|n| n.id == me_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("me id not in nodes"))?;
        Ok(Self {
            me,
            nodes: Arc::new(nodes),
        })
    }

    /// env:
    /// TOQ_NODE_ID="node-a"
    /// TOQ_NODES='[{"id":"node-a","addr":"127.0.0.1:7001"},{"id":"node-b","addr":"127.0.0.1:7002"}]'
    pub fn from_env() -> anyhow::Result<Self> {
        let me_id = std::env::var("TOQ_NODE_ID").unwrap_or_else(|_| "node-a".to_string());
        let nodes_json = std::env::var("TOQ_NODES").unwrap_or_else(|_| {
            r#"[{"id":"node-a","addr":"127.0.0.1:7001"},{"id":"node-b","addr":"127.0.0.1:7002"}]"#
                .to_string()
        });
        let nodes: Vec<Node> = serde_json::from_str(&nodes_json)?;
        let me = nodes
            .iter()
            .find(|n| n.id == me_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("me id not in TOQ_NODES"))?;
        Ok(Self {
            me,
            nodes: Arc::new(nodes),
        })
    }

    /// Rendezvous hashing: 가장 큰 hash(node, topic)
    pub fn leader_of(&self, topic: &str) -> Node {
        let mut best: Option<(&Node, u64)> = None;
        for n in self.nodes.iter() {
            let key = format!("{}:{}", n.id, topic);
            let score = hash(key.as_bytes());
            if best.map(|(_, s)| score > s).unwrap_or(true) {
                best = Some((n, score));
            }
        }
        best.unwrap().0.clone()
    }

    pub fn is_leader(&self, topic: &str) -> bool {
        self.leader_of(topic).id == self.me.id
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_nodes() -> Vec<Node> {
        vec![
            Node {
                id: "node-a".to_string(),
                addr: "127.0.0.1:7001".to_string(),
            },
            Node {
                id: "node-b".to_string(),
                addr: "127.0.0.1:7002".to_string(),
            },
            Node {
                id: "node-c".to_string(),
                addr: "127.0.0.1:7003".to_string(),
            },
        ]
    }

    #[test]
    fn test_leader_of_deterministic() {
        let nodes = create_test_nodes();
        let cluster_a = Cluster::new("node-a".to_string(), nodes.clone()).unwrap();
        let cluster_b = Cluster::new("node-b".to_string(), nodes.clone()).unwrap();
        let cluster_c = Cluster::new("node-c".to_string(), nodes.clone()).unwrap();

        let topic = "test-topic";
        let leader_a = cluster_a.leader_of(topic);
        let leader_b = cluster_b.leader_of(topic);
        let leader_c = cluster_c.leader_of(topic);

        assert_eq!(leader_a.id, leader_b.id);
        assert_eq!(leader_b.id, leader_c.id);
    }

    #[test]
    fn test_leader_of_distribution() {
        let nodes = create_test_nodes();
        let cluster = Cluster::new("node-a".to_string(), nodes).unwrap();

        let mut leaders = std::collections::HashMap::new();
        for i in 0..100 {
            let topic = format!("topic-{}", i);
            let leader = cluster.leader_of(&topic);
            *leaders.entry(leader.id).or_insert(0) += 1;
        }

        assert!(leaders.len() > 1, "Leaders should be distributed across nodes");
    }

    #[test]
    fn test_is_leader() {
        let nodes = create_test_nodes();
        let cluster = Cluster::new("node-a".to_string(), nodes).unwrap();

        let topic_a = "topic-for-a";
        let leader = cluster.leader_of(topic_a);

        if leader.id == "node-a" {
            assert!(cluster.is_leader(topic_a));
        } else {
            assert!(!cluster.is_leader(topic_a));
        }
    }

    #[test]
    fn test_from_env_default() {
        unsafe {
            std::env::remove_var("TOQ_NODE_ID");
            std::env::remove_var("TOQ_NODES");
        }

        let cluster = Cluster::from_env().unwrap();
        assert_eq!(cluster.me.id, "node-a");
        assert!(cluster.nodes.len() >= 1);
    }

    #[test]
    fn test_from_env_custom() {
        unsafe {
            std::env::set_var("TOQ_NODE_ID", "custom-node");
            std::env::set_var("TOQ_NODES", r#"[{"id":"custom-node","addr":"192.168.1.1:8000"}]"#);
        }

        let cluster = Cluster::from_env().unwrap();
        assert_eq!(cluster.me.id, "custom-node");
        assert_eq!(cluster.me.addr, "192.168.1.1:8000");

        unsafe {
            std::env::remove_var("TOQ_NODE_ID");
            std::env::remove_var("TOQ_NODES");
        }
    }

    #[test]
    fn test_from_env_node_not_found() {
        unsafe {
            std::env::set_var("TOQ_NODE_ID", "nonexistent");
            std::env::set_var("TOQ_NODES", r#"[{"id":"node-a","addr":"127.0.0.1:7001"}]"#);
        }

        let result = Cluster::from_env();
        assert!(result.is_err());

        unsafe {
            std::env::remove_var("TOQ_NODE_ID");
            std::env::remove_var("TOQ_NODES");
        }
    }
}
