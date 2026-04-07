use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;

use crate::registry::DatasetRegistry;

#[derive(Debug, Clone)]
pub struct WorkerEntry {
    pub worker_id: String,
    pub address: String,
    pub port: u32,
    pub last_seen: Instant,
    pub alive: bool,
}

/// Tracks worker liveness based on heartbeat timestamps.
///
/// A background reaper task scans at `check_interval` and marks any worker
/// whose last heartbeat is older than `dead_threshold` as dead, emitting a
/// `WorkerFailed` tracing event.
#[derive(Clone)]
pub struct LivenessTracker {
    inner: Arc<Mutex<TrackerState>>,
    dead_threshold: Duration,
    check_interval: Duration,
    shutdown: Arc<Notify>,
}

struct TrackerState {
    workers: HashMap<String, WorkerEntry>,
    failed_workers: Vec<(String, Instant)>,
}

impl LivenessTracker {
    pub fn new(heartbeat_interval: Duration) -> Self {
        let dead_threshold = heartbeat_interval * 3;
        let check_interval = heartbeat_interval;
        Self {
            inner: Arc::new(Mutex::new(TrackerState {
                workers: HashMap::new(),
                failed_workers: Vec::new(),
            })),
            dead_threshold,
            check_interval,
            shutdown: Arc::new(Notify::new()),
        }
    }

    pub async fn register_worker(&self, worker_id: String, address: String, port: u32) {
        let mut state = self.inner.lock().await;
        state.workers.insert(
            worker_id.clone(),
            WorkerEntry {
                worker_id,
                address,
                port,
                last_seen: Instant::now(),
                alive: true,
            },
        );
    }

    pub async fn record_heartbeat(&self, worker_id: &str) -> bool {
        let mut state = self.inner.lock().await;
        if let Some(entry) = state.workers.get_mut(worker_id) {
            entry.last_seen = Instant::now();
            entry.alive = true;
            true
        } else {
            false
        }
    }

    /// Returns list of (worker_id, last_seen) for workers that have been marked dead.
    pub async fn failed_workers(&self) -> Vec<(String, Instant)> {
        let state = self.inner.lock().await;
        state.failed_workers.clone()
    }

    /// Spawn the background reaper. Returns a handle that can be used to await completion.
    pub fn spawn_reaper(&self) -> JoinHandle<()> {
        self.spawn_reaper_inner(None)
    }

    /// Like [`Self::spawn_reaper`], but recomputes dataset shard assignments in the registry
    /// whenever workers are marked dead (survivors pick up shards).
    pub fn spawn_reaper_with_registry(&self, registry: DatasetRegistry) -> JoinHandle<()> {
        self.spawn_reaper_inner(Some(registry))
    }

    fn spawn_reaper_inner(&self, registry: Option<DatasetRegistry>) -> JoinHandle<()> {
        let inner = Arc::clone(&self.inner);
        let threshold = self.dead_threshold;
        let interval = self.check_interval;
        let shutdown = Arc::clone(&self.shutdown);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = shutdown.notified() => break,
                }

                let now = Instant::now();
                let newly_failed: Vec<(String, Instant)> = {
                    let mut state = inner.lock().await;
                    let newly_failed: Vec<(String, Instant)> = state
                        .workers
                        .values_mut()
                        .filter(|e| e.alive && now.duration_since(e.last_seen) > threshold)
                        .map(|e| {
                            e.alive = false;
                            (e.worker_id.clone(), e.last_seen)
                        })
                        .collect();

                    for (ref id, last_seen) in &newly_failed {
                        tracing::warn!(
                            worker_id = %id,
                            last_seen_ms_ago = now.duration_since(*last_seen).as_millis() as u64,
                            "WorkerFailed: worker missed heartbeat deadline"
                        );
                    }
                    state.failed_workers.extend(newly_failed.iter().cloned());
                    newly_failed
                };

                if !newly_failed.is_empty() {
                    if let Some(reg) = registry.clone() {
                        let alive = {
                            let state = inner.lock().await;
                            let mut ids: Vec<String> = state
                                .workers
                                .values()
                                .filter(|e| e.alive)
                                .map(|e| e.worker_id.clone())
                                .collect();
                            ids.sort();
                            ids
                        };
                        if alive.is_empty() {
                            tracing::warn!("no alive workers after failure; skipping rebalance");
                        } else {
                            let start = Instant::now();
                            match reg.rebalance_all(&alive).await {
                                Ok(()) => {
                                    tracing::info!(
                                        elapsed_ms = start.elapsed().as_millis() as u64,
                                        failed = ?newly_failed.iter().map(|(id, _)| id).collect::<Vec<_>>(),
                                        "rebalanced dataset shard assignments after worker failure"
                                    );
                                }
                                Err(e) => {
                                    tracing::error!(error = %e, "rebalance after worker failure failed");
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    /// Returns the IDs of all workers currently marked alive (sorted for stable shard maps).
    pub async fn alive_worker_ids(&self) -> Vec<String> {
        let state = self.inner.lock().await;
        let mut ids: Vec<String> = state
            .workers
            .values()
            .filter(|e| e.alive)
            .map(|e| e.worker_id.clone())
            .collect();
        ids.sort();
        ids
    }

    /// Count of workers marked alive vs total registered (alive or dead).
    pub async fn worker_counts(&self) -> (u32, u32) {
        let state = self.inner.lock().await;
        let total = state.workers.len() as u32;
        let alive = state.workers.values().filter(|e| e.alive).count() as u32;
        (alive, total)
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }
}
