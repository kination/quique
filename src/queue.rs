use crate::storage::disk_log::DiskLog;
use crate::storage::metadata::{BrokerMetadata, LocalMetadataStorage, MetadataStorage, QueueMeta, TopicMeta};
use anyhow::Result;
use crossbeam_queue::ArrayQueue;
use dashmap::{DashMap, DashSet};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Notify;

/// A Queue which holds messages in memory with a write-ahead log (WAL) for persistence.
pub struct Queue {
    pub name: String,
    mem: ArrayQueue<Vec<u8>>,
    notify: Notify,
    wal: Arc<DiskLog>,
}

impl Queue {
    pub fn new(name: String, cap: usize) -> Self {
        let wal = Arc::new(DiskLog::open("./data", &name).expect("Failed to open WAL"));
        Self {
            name,
            mem: ArrayQueue::new(cap),
            notify: Notify::new(),
            wal,
        }
    }

    /// Open existing queue and replay WAL
    pub fn open(data_dir: &str, name: String, cap: usize) -> Result<Self> {
        let wal = Arc::new(DiskLog::open(data_dir, &name)?);
        let mem = ArrayQueue::new(cap);

        // Replay unacked messages
        let mut entries = wal.replay_unacked()?;
        entries.sort_by_key(|(s, _)| *s);
        for (_seq, payload) in entries {
            let _ = mem.push(payload); // Best effort
        }

        Ok(Self {
            name,
            mem,
            notify: Notify::new(),
            wal,
        })
    }

    pub fn push(&self, val: Vec<u8>) -> Result<(), Vec<u8>> {
        // Write WAL first
        if let Ok(seq) = self.wal.append(&val) {
            let res = self.mem.push(val);
            if res.is_ok() {
                self.notify.notify_one();
            }
            return res;
        }
        Err(val)
    }

    pub fn pop(&self) -> Option<Vec<u8>> {
        if let Some(v) = self.mem.pop() {
            // Acknowledge in WAL
            // TODO: Proper seq tracking
            return Some(v);
        }
        None
    }

    pub async fn pop_wait(&self) -> Vec<u8> {
        loop {
            if let Some(v) = self.mem.pop() {
                return v;
            }
            self.notify.notified().await;
        }
    }

    pub fn len(&self) -> usize {
        self.mem.len()
    }

    pub fn capacity(&self) -> usize {
        self.mem.capacity()
    }
}

/// Topic: Routing key that distributes messages to bound Queues.
pub struct Topic {
    pub name: String,
    pub bound_queues: DashSet<String>,
}

impl Topic {
    pub fn new(name: String) -> Self {
        Self {
            name,
            bound_queues: DashSet::new(),
        }
    }

    pub fn bind(&self, queue_name: String) {
        self.bound_queues.insert(queue_name);
    }

    pub fn unbind(&self, queue_name: &str) {
        self.bound_queues.remove(queue_name);
    }
}

/// Global registry for topic/queue with persistence.
pub struct Registry {
    pub topics: DashMap<String, Arc<Topic>>,
    pub queues: DashMap<String, Arc<Queue>>,
    data_dir: String,
    metadata_store: Box<dyn MetadataStorage>,
}

impl Registry {
    pub fn new(data_dir: String) -> Self {
        let metadata_path = format!("{}/metadata.json", data_dir);
        let metadata_store = Box::new(LocalMetadataStorage::new(metadata_path));
        
        Self {
            topics: DashMap::new(),
            queues: DashMap::new(),
            data_dir,
            metadata_store,
        }
    }

    /// Load metadata and replay WAL
    pub fn load(&self) -> Result<()> {
        let metadata = self.metadata_store.load()?;
        
        // Restore queues from WAL
        for (name, qmeta) in metadata.queues {
            let queue = Queue::open(&self.data_dir, name.clone(), qmeta.capacity)?;
            self.queues.insert(name, Arc::new(queue));
        }
        
        // Restore topics and bindings
        for (name, tmeta) in metadata.topics {
            let topic = Topic::new(name.clone());
            for q in tmeta.bound_queues {
                topic.bind(q);
            }
            self.topics.insert(name, Arc::new(topic));
        }
        
        Ok(())
    }

    /// Save metadata snapshot
    pub fn save(&self) -> Result<()> {
        let mut metadata = BrokerMetadata {
            topics: HashMap::new(),
            queues: HashMap::new(),
        };
        
        for entry in self.topics.iter() {
            let topic = entry.value();
            let bound_queues: HashSet<String> = topic.bound_queues.iter()
                .map(|r| r.key().clone())
                .collect();
            
            metadata.topics.insert(
                entry.key().clone(),
                TopicMeta {
                    name: topic.name.clone(),
                    bound_queues,
                },
            );
        }
        
        for entry in self.queues.iter() {
            let queue = entry.value();
            metadata.queues.insert(
                entry.key().clone(),
                QueueMeta {
                    name: queue.name.clone(),
                    capacity: queue.capacity(),
                },
            );
        }
        
        self.metadata_store.save(&metadata)?;
        Ok(())
    }

    pub fn get_topic(&self, name: &str) -> Option<Arc<Topic>> {
        self.topics.get(name).map(|v| v.value().clone())
    }

    pub fn get_queue(&self, name: &str) -> Option<Arc<Queue>> {
        self.queues.get(name).map(|v| v.value().clone())
    }

    pub fn create_topic(&self, name: String) -> Arc<Topic> {
        self.topics.entry(name.clone()).or_insert_with(|| Arc::new(Topic::new(name))).value().clone()
    }

    pub fn create_queue(&self, name: String, cap: usize) -> Arc<Queue> {
        self.queues.entry(name.clone()).or_insert_with(|| Arc::new(Queue::new(name, cap))).value().clone()
    }
}
