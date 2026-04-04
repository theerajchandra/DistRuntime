use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use proto_gen::distruntime::ShardRange;

#[derive(Debug, Clone)]
pub struct Dataset {
    pub dataset_id: String,
    pub job_id: String,
    pub uri: String,
    pub format: String,
    pub num_shards: u64,
    /// Shard assignments computed at registration time: worker_id -> ranges.
    pub assignments: HashMap<String, Vec<ShardRange>>,
}

/// Thread-safe registry of datasets and their shard assignments.
#[derive(Clone)]
pub struct DatasetRegistry {
    inner: Arc<Mutex<RegistryState>>,
}

struct RegistryState {
    datasets: HashMap<String, Dataset>,
    counter: u64,
}

impl DatasetRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(RegistryState {
                datasets: HashMap::new(),
                counter: 0,
            })),
        }
    }

    pub async fn register(
        &self,
        job_id: String,
        uri: String,
        format: String,
        num_shards: u64,
        assignments: HashMap<String, Vec<ShardRange>>,
    ) -> String {
        let mut state = self.inner.lock().await;
        state.counter += 1;
        let dataset_id = format!("ds-{:08x}", state.counter);

        state.datasets.insert(
            dataset_id.clone(),
            Dataset {
                dataset_id: dataset_id.clone(),
                job_id,
                uri,
                format,
                num_shards,
                assignments,
            },
        );

        dataset_id
    }

    pub async fn get(&self, dataset_id: &str) -> Option<Dataset> {
        let state = self.inner.lock().await;
        state.datasets.get(dataset_id).cloned()
    }

    pub async fn list_for_job(&self, job_id: &str) -> Vec<Dataset> {
        let state = self.inner.lock().await;
        state
            .datasets
            .values()
            .filter(|d| d.job_id == job_id)
            .cloned()
            .collect()
    }
}

impl Default for DatasetRegistry {
    fn default() -> Self {
        Self::new()
    }
}
