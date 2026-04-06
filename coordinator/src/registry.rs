use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

use proto_gen::distruntime::ShardRange;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedShardRange {
    start: u64,
    end: u64,
}

impl From<&ShardRange> for PersistedShardRange {
    fn from(r: &ShardRange) -> Self {
        Self {
            start: r.start,
            end: r.end,
        }
    }
}

impl From<&PersistedShardRange> for ShardRange {
    fn from(r: &PersistedShardRange) -> Self {
        Self {
            start: r.start,
            end: r.end,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedDataset {
    dataset_id: String,
    job_id: String,
    uri: String,
    format: String,
    num_shards: u64,
    assignments: HashMap<String, Vec<PersistedShardRange>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedState {
    datasets: HashMap<String, PersistedDataset>,
    counter: u64,
}

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
///
/// When a `persist_path` is set, the registry writes its full state to a JSON
/// file after every mutation and loads it back on construction, surviving
/// coordinator restarts.
#[derive(Clone)]
pub struct DatasetRegistry {
    inner: Arc<Mutex<RegistryState>>,
    persist_path: Option<PathBuf>,
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
            persist_path: None,
        }
    }

    /// Create a registry that persists state to the given JSON file.
    /// If the file exists, state is loaded from it.
    pub fn with_persistence(path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let path = path.into();
        let (datasets, counter) = if path.exists() {
            Self::load_from_file(&path)?
        } else {
            (HashMap::new(), 0)
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(RegistryState { datasets, counter })),
            persist_path: Some(path),
        })
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

        if let Some(ref path) = self.persist_path {
            if let Err(e) = Self::save_to_file(path, &state) {
                tracing::error!(error = %e, "failed to persist dataset registry");
            }
        }

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

    fn save_to_file(path: &Path, state: &RegistryState) -> anyhow::Result<()> {
        let persisted = PersistedState {
            datasets: state
                .datasets
                .iter()
                .map(|(k, d)| {
                    let pd = PersistedDataset {
                        dataset_id: d.dataset_id.clone(),
                        job_id: d.job_id.clone(),
                        uri: d.uri.clone(),
                        format: d.format.clone(),
                        num_shards: d.num_shards,
                        assignments: d
                            .assignments
                            .iter()
                            .map(|(wid, ranges)| {
                                (
                                    wid.clone(),
                                    ranges.iter().map(PersistedShardRange::from).collect(),
                                )
                            })
                            .collect(),
                    };
                    (k.clone(), pd)
                })
                .collect(),
            counter: state.counter,
        };
        let json = serde_json::to_string_pretty(&persisted)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    fn load_from_file(path: &Path) -> anyhow::Result<(HashMap<String, Dataset>, u64)> {
        let json = std::fs::read_to_string(path)?;
        let persisted: PersistedState = serde_json::from_str(&json)?;

        let datasets = persisted
            .datasets
            .into_iter()
            .map(|(k, pd)| {
                let d = Dataset {
                    dataset_id: pd.dataset_id,
                    job_id: pd.job_id,
                    uri: pd.uri,
                    format: pd.format,
                    num_shards: pd.num_shards,
                    assignments: pd
                        .assignments
                        .into_iter()
                        .map(|(wid, ranges)| (wid, ranges.iter().map(ShardRange::from).collect()))
                        .collect(),
                };
                (k, d)
            })
            .collect();

        Ok((datasets, persisted.counter))
    }
}

impl Default for DatasetRegistry {
    fn default() -> Self {
        Self::new()
    }
}
