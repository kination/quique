use crate::storage::disk_log::DiskLog;
use anyhow::Result;
use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;
use seahash::hash;
use std::sync::Arc;

pub struct Partition {
    mem: Arc<ArrayQueue<(u64, Vec<u8>)>>,
    wal: Arc<DiskLog>,
}
impl Partition {
    pub fn open(data_dir: &str, topic: &str, part: u32, cap: usize) -> Result<Self> {
        let wal = Arc::new(DiskLog::open(data_dir, topic, part)?);
        let mem = Arc::new(ArrayQueue::new(cap));
        // replay
        let mut entries = wal.replay_unacked()?;
        entries.sort_by_key(|(s, _)| *s);
        for (seq, payload) in entries {
            if mem.push((seq, payload)).is_err() {
                break;
            }
        }
        Ok(Self { mem, wal })
    }
    pub fn enqueue(&self, val: Vec<u8>) -> Result<u64> {
        let seq = self.wal.append(&val)?; // durable
        self.mem
            .push((seq, val))
            .map_err(|_| anyhow::anyhow!("Queue full"))?;
        Ok(seq)
    }
    pub fn dequeue(&self) -> Result<Option<Vec<u8>>> {
        if let Some((seq, v)) = self.mem.pop() {
            self.wal.write_acked(seq)?;
            return Ok(Some(v));
        }
        Ok(None)
    }
    pub fn read_last_n(&self, n: usize) -> Result<Vec<Vec<u8>>> {
        let messages = self.wal.read_last_n(n)?;
        Ok(messages)
    }
    pub fn len(&self) -> usize {
        self.mem.len()
    }
    pub fn capacity(&self) -> usize {
        self.mem.capacity()
    }
}

pub struct Topic {
    pub name: String,
    pub partitions: Vec<Arc<Partition>>,
}
impl Topic {
    pub fn open(
        data_dir: &str,
        name: &str,
        num_partitions: u32,
        cap: usize,
        is_leader_fn: impl Fn(u32) -> bool,
    ) -> Result<Self> {
        let mut parts = Vec::new();
        for p in 0..num_partitions {
            if is_leader_fn(p) {
                parts.push(Arc::new(Partition::open(data_dir, name, p, cap)?));
            }
        }
        Ok(Self {
            name: name.to_string(),
            partitions: parts,
        })
    }
    pub fn pick_partition_by_key(&self, key: &str) -> usize {
        (hash(key.as_bytes()) as usize) % self.partitions.len()
    }
}

pub struct TopicRegistry(pub DashMap<String, Arc<Topic>>);
impl TopicRegistry {
    pub fn new() -> Self {
        Self(DashMap::new())
    }
    pub fn get(&self, t: &str) -> Option<Arc<Topic>> {
        self.0.get(t).map(|v| v.value().clone())
    }
    pub fn insert(&self, t: Arc<Topic>) {
        self.0.insert(t.name.clone(), t);
    }
}
