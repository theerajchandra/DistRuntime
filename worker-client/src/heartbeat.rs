use anyhow::{Context, Result};
use proto_gen::distruntime::coordinator_service_client::CoordinatorServiceClient;
use proto_gen::distruntime::{
    DatasetShardAssignment, HeartbeatRequest, RecoverWorkerRequest, RegisterDatasetRequest,
    ShardRange, WorkerInfo, WorkerReadyRequest,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tonic::metadata::MetadataValue;

use crate::metrics;

const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, Default)]
struct ThroughputSnapshot {
    epoch: u64,
    step: u64,
    records_per_sec: f32,
    bytes_per_sec: f32,
}

pub struct WorkerClient {
    client: CoordinatorServiceClient<Channel>,
    worker_id: String,
    heartbeat_interval: Duration,
    assignment_tx: watch::Sender<(u64, Vec<DatasetShardAssignment>)>,
    throughput: Arc<Mutex<ThroughputSnapshot>>,
}

impl WorkerClient {
    pub async fn connect(coordinator_addr: &str) -> Result<Self> {
        Self::connect_with_interval(coordinator_addr, DEFAULT_HEARTBEAT_INTERVAL).await
    }

    pub async fn connect_with_interval(
        coordinator_addr: &str,
        heartbeat_interval: Duration,
    ) -> Result<Self> {
        let client = CoordinatorServiceClient::connect(coordinator_addr.to_string())
            .await
            .with_context(|| format!("failed to connect to coordinator at {coordinator_addr}"))?;

        let (assignment_tx, _) = watch::channel((0u64, vec![]));

        Ok(Self {
            client,
            worker_id: String::new(),
            heartbeat_interval,
            assignment_tx,
            throughput: Arc::new(Mutex::new(ThroughputSnapshot::default())),
        })
    }

    /// Subscribe to shard assignment snapshots pushed on every successful heartbeat.
    /// Hot-swap readers when [`DatasetShardAssignment`] or generation changes.
    pub fn assignment_watch(&self) -> watch::Receiver<(u64, Vec<DatasetShardAssignment>)> {
        self.assignment_tx.subscribe()
    }

    /// Register this worker with the coordinator. Returns the assigned worker ID.
    pub async fn register(&mut self, address: &str, port: u32) -> Result<String> {
        let correlation_id = fresh_correlation_id(None);
        let mut request = tonic::Request::new(WorkerReadyRequest {
            info: Some(WorkerInfo {
                worker_id: String::new(),
                address: address.to_string(),
                port,
            }),
            capabilities: vec![],
        });
        let cid = MetadataValue::try_from(correlation_id.as_str())
            .context("invalid correlation id metadata")?;
        request.metadata_mut().insert("x-correlation-id", cid);
        let resp = self
            .client
            .worker_ready(request)
            .await
            .context("WorkerReady RPC failed")?
            .into_inner();

        if !resp.accepted {
            anyhow::bail!("coordinator rejected worker registration");
        }

        self.worker_id = resp.assigned_worker_id.clone();
        tracing::info!(worker_id = %self.worker_id, correlation_id = %correlation_id, "registered with coordinator");
        Ok(resp.assigned_worker_id)
    }

    /// Update the next heartbeat's telemetry payload.
    pub fn update_throughput(
        &self,
        epoch: u64,
        step: u64,
        records_per_sec: f32,
        bytes_per_sec: f32,
    ) {
        if let Ok(mut snapshot) = self.throughput.lock() {
            snapshot.epoch = epoch;
            snapshot.step = step;
            snapshot.records_per_sec = records_per_sec.max(0.0);
            snapshot.bytes_per_sec = bytes_per_sec.max(0.0);
        }
        metrics::observe_throughput(bytes_per_sec as f64, records_per_sec as f64);
    }

    /// Spawn a background task that sends heartbeats at the configured interval.
    /// Returns a handle; dropping the returned `AbortHandle` stops the loop.
    pub fn start_heartbeat(&self) -> (JoinHandle<()>, tokio::task::AbortHandle) {
        let mut client = self.client.clone();
        let worker_id = self.worker_id.clone();
        let interval = self.heartbeat_interval;
        let assignment_tx = self.assignment_tx.clone();
        let throughput = Arc::clone(&self.throughput);

        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let snapshot = throughput.lock().map(|s| *s).unwrap_or_default();
                let req = HeartbeatRequest {
                    worker_id: worker_id.clone(),
                    epoch: snapshot.epoch,
                    step: snapshot.step,
                    throughput_samples_per_sec: snapshot.records_per_sec,
                    throughput_bytes_per_sec: snapshot.bytes_per_sec,
                };
                metrics::inc_heartbeat_sent();
                match client.heartbeat(req).await {
                    Ok(resp) => {
                        let inner = resp.into_inner();
                        let _ = assignment_tx.send((inner.rebalance_generation, inner.assignments));
                        tracing::trace!(worker_id = %worker_id, "heartbeat acknowledged");
                    }
                    Err(e) => {
                        metrics::inc_heartbeat_failed();
                        tracing::warn!(worker_id = %worker_id, error = %e, "heartbeat failed");
                    }
                }
            }
        });

        let abort = handle.abort_handle();
        (handle, abort)
    }

    /// Ask the coordinator to resume from the latest checkpoint for `job_id`.
    ///
    /// Returns `Some((checkpoint_path, assigned_shards))` when recovery is
    /// possible, or `None` when no checkpoint exists for the job.
    pub async fn recover(
        &mut self,
        job_id: &str,
    ) -> anyhow::Result<Option<(String, Vec<ShardRange>)>> {
        let correlation_id = fresh_correlation_id(Some(job_id));
        let mut request = tonic::Request::new(RecoverWorkerRequest {
            worker_id: self.worker_id.clone(),
            last_checkpoint_id: job_id.to_string(),
        });
        let cid = MetadataValue::try_from(correlation_id.as_str())
            .context("invalid correlation id metadata")?;
        request.metadata_mut().insert("x-correlation-id", cid);
        let resp = self
            .client
            .recover_worker(request)
            .await
            .context("RecoverWorker RPC failed")?
            .into_inner();

        if resp.can_recover {
            tracing::info!(
                worker_id = %self.worker_id,
                job_id,
                correlation_id = %correlation_id,
                checkpoint_path = %resp.checkpoint_path,
                shard_count = resp.assigned_shards.len(),
                "resuming from checkpoint"
            );
            Ok(Some((resp.checkpoint_path, resp.assigned_shards)))
        } else {
            Ok(None)
        }
    }

    /// Register a dataset with the coordinator and return the assigned dataset ID.
    ///
    /// The coordinator distributes shards across all alive workers. Call
    /// [`heartbeat_once`] afterwards to learn this worker's shard assignments.
    pub async fn register_dataset(
        &mut self,
        job_id: &str,
        uri: &str,
        num_shards: u64,
        format: &str,
    ) -> anyhow::Result<String> {
        let correlation_id = fresh_correlation_id(Some(job_id));
        let mut request = tonic::Request::new(RegisterDatasetRequest {
            job_id: job_id.to_string(),
            uri: uri.to_string(),
            num_shards,
            format: format.to_string(),
        });
        let cid = MetadataValue::try_from(correlation_id.as_str())
            .context("invalid correlation id metadata")?;
        request.metadata_mut().insert("x-correlation-id", cid);
        let resp = self
            .client
            .register_dataset(request)
            .await
            .context("RegisterDataset RPC failed")?
            .into_inner();

        if !resp.accepted {
            anyhow::bail!("coordinator rejected dataset registration");
        }

        tracing::info!(
            dataset_id = %resp.dataset_id,
            job_id,
            correlation_id = %correlation_id,
            uri,
            num_shards,
            format,
            "dataset registered"
        );
        Ok(resp.dataset_id)
    }

    /// Perform a single heartbeat and return the current shard assignments.
    ///
    /// Useful after registering a dataset to immediately learn which shards
    /// this worker is responsible for.
    pub async fn heartbeat_once(&mut self) -> anyhow::Result<(u64, Vec<DatasetShardAssignment>)> {
        let snapshot = self.throughput.lock().map(|s| *s).unwrap_or_default();
        metrics::inc_heartbeat_sent();
        let resp = self
            .client
            .heartbeat(HeartbeatRequest {
                worker_id: self.worker_id.clone(),
                epoch: snapshot.epoch,
                step: snapshot.step,
                throughput_samples_per_sec: snapshot.records_per_sec,
                throughput_bytes_per_sec: snapshot.bytes_per_sec,
            })
            .await
            .map_err(|e| {
                metrics::inc_heartbeat_failed();
                e
            })
            .context("Heartbeat RPC failed")?
            .into_inner();

        if !resp.acknowledged {
            anyhow::bail!("heartbeat not acknowledged");
        }

        Ok((resp.rebalance_generation, resp.assignments))
    }
}

fn fresh_correlation_id(job_id: Option<&str>) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let nonce = COUNTER.fetch_add(1, Ordering::Relaxed);
    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    match job_id {
        Some(job_id) if !job_id.is_empty() => {
            format!("job-{job_id}-{now_nanos:x}-{nonce:x}")
        }
        _ => format!("corr-{now_nanos:x}-{nonce:x}"),
    }
}
