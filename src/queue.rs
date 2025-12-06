use anyhow::Result;
use crossbeam_queue::ArrayQueue;
use dashmap::{DashMap, DashSet};
use std::sync::Arc;
use tokio::sync::Notify;

/// A Queue holds messages in memory.
pub struct Queue {
    pub name: String,
    mem: ArrayQueue<Vec<u8>>,
    notify: Notify,
}

impl Queue {
    pub fn new(name: String, cap: usize) -> Self {
        Self {
            name,
            mem: ArrayQueue::new(cap),
            notify: Notify::new(),
        }
    }

    pub fn push(&self, val: Vec<u8>) -> Result<(), Vec<u8>> {
        let res = self.mem.push(val);
        if res.is_ok() {
            self.notify.notify_one();
        }
        res
    }

    pub fn pop(&self) -> Option<Vec<u8>> {
        self.mem.pop()
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

/// A Topic is a routing key that distributes messages to bound Queues.
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

/// Global registry for Topics and Queues.
pub struct Registry {
    pub topics: DashMap<String, Arc<Topic>>,
    pub queues: DashMap<String, Arc<Queue>>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            topics: DashMap::new(),
            queues: DashMap::new(),
        }
    }

    pub fn get_topic(&self, name: &str) -> Option<Arc<Topic>> {
        self.topics.get(name).map(|v| v.value().clone())
    }

    pub fn get_queue(&self, name: &str) -> Option<Arc<Queue>> {
        self.queues.get(name).map(|v| v.value().clone())
    }

    pub fn create_topic(&self, name: String) -> Arc<Topic> {
        // If exists, return existing (get_or_insert logic)
        // DashMap entry API or just check-then-insert (race condition possible but acceptable for now)
        // Let's use entry to be safe-ish or just simplistic check.
        // DashMap::entry is good.
        self.topics.entry(name.clone()).or_insert_with(|| Arc::new(Topic::new(name))).value().clone()
    }

    pub fn create_queue(&self, name: String, cap: usize) -> Arc<Queue> {
        self.queues.entry(name.clone()).or_insert_with(|| Arc::new(Queue::new(name, cap))).value().clone()
    }
}
