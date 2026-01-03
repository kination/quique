use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

/// Metadata snapshot of the broker state
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BrokerMetadata {
    pub topics: HashMap<String, TopicMeta>,
    pub queues: HashMap<String, QueueMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMeta {
    pub name: String,
    pub bound_queues: HashSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMeta {
    pub name: String,
    pub capacity: usize,
}

/// Storage abstraction for future S3 support
pub trait MetadataStorage: Send + Sync {
    fn save(&self, metadata: &BrokerMetadata) -> Result<()>;
    fn load(&self) -> Result<BrokerMetadata>;
}

/// Local file-based metadata storage
pub struct LocalMetadataStorage {
    path: String,
}

impl LocalMetadataStorage {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}

impl MetadataStorage for LocalMetadataStorage {
    fn save(&self, metadata: &BrokerMetadata) -> Result<()> {
        let json = serde_json::to_string_pretty(metadata)?;
        
        // Ensure parent directory exists
        if let Some(parent) = Path::new(&self.path).parent() {
            fs::create_dir_all(parent)?;
        }
        
        // Atomic write: write to temp file, then rename
        let temp_path = format!("{}.tmp", self.path);
        fs::write(&temp_path, json)?;
        fs::rename(&temp_path, &self.path)?;
        
        Ok(())
    }

    fn load(&self) -> Result<BrokerMetadata> {
        if !Path::new(&self.path).exists() {
            return Ok(BrokerMetadata::default());
        }
        
        let json = fs::read_to_string(&self.path)?;
        let metadata = serde_json::from_str(&json)?;
        Ok(metadata)
    }
}

// TODO: Future S3 storage implementation
// pub struct S3MetadataStorage {
//     bucket: String,
//     key: String,
// }
