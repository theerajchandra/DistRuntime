use anyhow::{Context, Result};
use proto_gen::distruntime::coordinator_service_client::CoordinatorServiceClient;
use proto_gen::distruntime::{
    DatasetShardAssignment, HeartbeatRequest, RecoverWorkerRequest, ShardRange, WorkerInfo,
    WorkerReadyRequest,
};
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

pub struct WorkerClient {
    client: CoordinatorServiceClient<Channel>,
    worker_id: String,
    heartbeat_interval: Duration,
    assignment_tx: watch::Sender<(u64, Vec<DatasetShardAssignment>)>,
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
        })
    }

    /// Subscribe to shard assignment snapshots pushed on every successful heartbeat.
    /// Hot-swap readers when [`DatasetShardAssignment`] or generation changes.
    pub fn assignment_watch(&self) -> watch::Receiver<(u64, Vec<DatasetShardAssignment>)> {
        self.assignment_tx.subscribe()
    }

    /// Register this worker with the coordinator. Returns the assigned worker ID.
    pub async fn register(&mut self, address: &str, port: u32) -> Result<String> {
        let resp = self
            .client
            .worker_ready(WorkerReadyRequest {
                info: Some(WorkerInfo {
                    worker_id: String::new(),
                    address: address.to_string(),
                    port,
                }),
                capabilities: vec![],
            })
            .await
            .context("WorkerReady RPC failed")?
            .into_inner();

        if !resp.accepted {
            anyhow::bail!("coordinator rejected worker registration");
        }

        self.worker_id = resp.assigned_worker_id.clone();
        tracing::info!(worker_id = %self.worker_id, "registered with coordinator");
        Ok(resp.assigned_worker_id)
    }

    /// Spawn a background task that sends heartbeats at the configured interval.
    /// Returns a handle; dropping the returned `AbortHandle` stops the loop.
    pub fn start_heartbeat(&self) -> (JoinHandle<()>, tokio::task::AbortHandle) {
        let mut client = self.client.clone();
        let worker_id = self.worker_id.clone();
        let interval = self.heartbeat_interval;
        let assignment_tx = self.assignment_tx.clone();

        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let req = HeartbeatRequest {
                    worker_id: worker_id.clone(),
                    epoch: 0,
                    step: 0,
                    throughput_samples_per_sec: 0.0,
                };
                match client.heartbeat(req).await {
                    Ok(resp) => {
                        let inner = resp.into_inner();
                        let _ = assignment_tx.send((inner.rebalance_generation, inner.assignments));
                        tracing::trace!(worker_id = %worker_id, "heartbeat acknowledged");
                    }
                    Err(e) => {
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
        let resp = self
            .client
            .recover_worker(RecoverWorkerRequest {
                worker_id: self.worker_id.clone(),
                last_checkpoint_id: job_id.to_string(),
            })
            .await
            .context("RecoverWorker RPC failed")?
            .into_inner();

        if resp.can_recover {
            tracing::info!(
                worker_id = %self.worker_id,
                job_id,
                checkpoint_path = %resp.checkpoint_path,
                shard_count = resp.assigned_shards.len(),
                "resuming from checkpoint"
            );
            Ok(Some((resp.checkpoint_path, resp.assigned_shards)))
        } else {
            Ok(None)
        }
    }
}
