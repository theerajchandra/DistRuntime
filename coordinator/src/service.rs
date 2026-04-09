use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use proto_gen::distruntime::coordinator_service_server::CoordinatorService;
use proto_gen::distruntime::{
    CheckpointAbortRequest, CheckpointAbortResponse, CheckpointBeginRequest,
    CheckpointBeginResponse, CheckpointCommitRequest, CheckpointCommitResponse, CheckpointEntry,
    DatasetShardAssignment, DatasetSummary, GetCheckpointRestoreInfoRequest,
    GetCheckpointRestoreInfoResponse, GetJobStatusRequest, GetJobStatusResponse, HeartbeatRequest,
    HeartbeatResponse, ListCheckpointsRequest, ListCheckpointsResponse, RecoverWorkerRequest,
    RecoverWorkerResponse, RegisterDatasetRequest, RegisterDatasetResponse, WorkerReadyRequest,
    WorkerReadyResponse,
};
use tonic::{Request, Response, Status};

use checkpoint_engine::{CheckpointEngine, CheckpointError};

use crate::metrics;
use crate::registry::DatasetRegistry;
use crate::shard_map;
use crate::tracker::LivenessTracker;

pub struct CoordinatorServiceImpl {
    tracker: LivenessTracker,
    registry: DatasetRegistry,
    checkpoint_engine: CheckpointEngine,
    checkpoint_started: Arc<tokio::sync::Mutex<HashMap<String, Instant>>>,
}

impl CoordinatorServiceImpl {
    pub fn new(
        tracker: LivenessTracker,
        registry: DatasetRegistry,
        checkpoint_engine: CheckpointEngine,
    ) -> Self {
        Self {
            tracker,
            registry,
            checkpoint_engine,
            checkpoint_started: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }
}

#[tonic::async_trait]
impl CoordinatorService for CoordinatorServiceImpl {
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let correlation_id = correlation_id_for_request(&request, None);
        let _span = tracing::info_span!(
            "coordinator.heartbeat",
            correlation_id = %correlation_id
        )
        .entered();
        let req = request.into_inner();
        let known = self.tracker.record_heartbeat(&req.worker_id).await;
        if !known {
            return Err(Status::not_found(format!(
                "unknown worker: {}",
                req.worker_id
            )));
        }

        let (generation, rows) = self.registry.assignments_for_worker(&req.worker_id).await;
        let assignments: Vec<DatasetShardAssignment> = rows
            .into_iter()
            .map(|(dataset_id, shards)| DatasetShardAssignment { dataset_id, shards })
            .collect();
        metrics::observe_worker_throughput(
            &req.worker_id,
            req.throughput_bytes_per_sec as f64,
            req.throughput_samples_per_sec as f64,
        );

        Ok(Response::new(HeartbeatResponse {
            acknowledged: true,
            directive: String::new(),
            assignments,
            rebalance_generation: generation,
        }))
    }

    async fn worker_ready(
        &self,
        request: Request<WorkerReadyRequest>,
    ) -> Result<Response<WorkerReadyResponse>, Status> {
        let correlation_id = correlation_id_for_request(&request, None);
        let _span = tracing::info_span!(
            "coordinator.worker_ready",
            correlation_id = %correlation_id
        )
        .entered();
        let req = request.into_inner();
        let info = req
            .info
            .ok_or_else(|| Status::invalid_argument("missing worker info"))?;

        let worker_id = if info.worker_id.is_empty() {
            format!("worker-{}", uuid_v4_simple())
        } else {
            info.worker_id.clone()
        };

        self.tracker
            .register_worker(worker_id.clone(), info.address, info.port)
            .await;

        tracing::info!(worker_id = %worker_id, "worker registered");

        Ok(Response::new(WorkerReadyResponse {
            accepted: true,
            assigned_worker_id: worker_id,
        }))
    }

    async fn register_dataset(
        &self,
        request: Request<RegisterDatasetRequest>,
    ) -> Result<Response<RegisterDatasetResponse>, Status> {
        let incoming_correlation_id = correlation_id_from_metadata(&request);
        let req = request.into_inner();
        let correlation_id =
            incoming_correlation_id.unwrap_or_else(|| fresh_correlation_id(Some(&req.job_id)));
        let _span = tracing::info_span!(
            "coordinator.register_dataset",
            correlation_id = %correlation_id,
            job_id = %req.job_id
        )
        .entered();

        if req.uri.is_empty() {
            return Err(Status::invalid_argument("dataset uri is required"));
        }

        let alive = self.tracker.alive_worker_ids().await;
        if alive.is_empty() {
            return Err(Status::failed_precondition(
                "no alive workers to assign shards to",
            ));
        }

        let assignments = shard_map::compute_shard_map(req.num_shards, &alive)
            .map_err(|e| Status::internal(e.to_string()))?;

        let dataset_id = self
            .registry
            .register(req.job_id, req.uri, req.format, req.num_shards, assignments)
            .await;

        tracing::info!(dataset_id = %dataset_id, num_shards = req.num_shards, workers = alive.len(), "dataset registered");

        Ok(Response::new(RegisterDatasetResponse {
            dataset_id,
            accepted: true,
        }))
    }

    async fn checkpoint_begin(
        &self,
        request: Request<CheckpointBeginRequest>,
    ) -> Result<Response<CheckpointBeginResponse>, Status> {
        let incoming_correlation_id = correlation_id_from_metadata(&request);
        let req = request.into_inner();
        let correlation_id =
            incoming_correlation_id.unwrap_or_else(|| fresh_correlation_id(Some(&req.job_id)));
        let _span = tracing::info_span!(
            "coordinator.checkpoint_begin",
            correlation_id = %correlation_id,
            job_id = %req.job_id
        )
        .entered();
        let alive = self.tracker.alive_worker_ids().await;
        if alive.is_empty() {
            return Err(Status::failed_precondition(
                "no alive workers for checkpoint",
            ));
        }
        let (checkpoint_id, storage_path) = self
            .checkpoint_engine
            .begin(&req.job_id, req.epoch, req.step, alive)
            .await;
        self.checkpoint_started
            .lock()
            .await
            .insert(checkpoint_id.clone(), Instant::now());
        tracing::info!(checkpoint_id = %checkpoint_id, job_id = %req.job_id, epoch = req.epoch, step = req.step, "checkpoint begun");
        Ok(Response::new(CheckpointBeginResponse {
            checkpoint_id,
            storage_path,
        }))
    }

    async fn checkpoint_commit(
        &self,
        request: Request<CheckpointCommitRequest>,
    ) -> Result<Response<CheckpointCommitResponse>, Status> {
        let correlation_id = correlation_id_for_request(&request, None);
        let req = request.into_inner();
        let _span = tracing::info_span!(
            "coordinator.checkpoint_commit",
            correlation_id = %correlation_id,
            checkpoint_id = %req.checkpoint_id
        )
        .entered();
        let all_done = self
            .checkpoint_engine
            .worker_commit(&req.checkpoint_id, &req.worker_id, req.bytes_written)
            .await
            .map_err(|e| match e {
                CheckpointError::NotFound(_) => Status::not_found(e.to_string()),
                CheckpointError::InvalidState(_) => Status::failed_precondition(e.to_string()),
                CheckpointError::AlreadyCommitted(_) => Status::already_exists(e.to_string()),
            })?;
        if all_done {
            if let Some(started_at) = self.checkpoint_started.lock().await.remove(&req.checkpoint_id) {
                metrics::observe_checkpoint_write_latency(started_at.elapsed().as_secs_f64());
            }
        }
        Ok(Response::new(CheckpointCommitResponse {
            success: all_done,
        }))
    }

    async fn checkpoint_abort(
        &self,
        request: Request<CheckpointAbortRequest>,
    ) -> Result<Response<CheckpointAbortResponse>, Status> {
        let correlation_id = correlation_id_for_request(&request, None);
        let req = request.into_inner();
        let _span = tracing::info_span!(
            "coordinator.checkpoint_abort",
            correlation_id = %correlation_id,
            checkpoint_id = %req.checkpoint_id
        )
        .entered();
        let acknowledged = self
            .checkpoint_engine
            .abort(&req.checkpoint_id, &req.reason)
            .await;
        self.checkpoint_started.lock().await.remove(&req.checkpoint_id);
        Ok(Response::new(CheckpointAbortResponse { acknowledged }))
    }

    async fn recover_worker(
        &self,
        request: Request<RecoverWorkerRequest>,
    ) -> Result<Response<RecoverWorkerResponse>, Status> {
        let incoming_correlation_id = correlation_id_from_metadata(&request);
        let req = request.into_inner();
        let correlation_id = incoming_correlation_id
            .unwrap_or_else(|| fresh_correlation_id(Some(&req.last_checkpoint_id)));
        let _span = tracing::info_span!(
            "coordinator.recover_worker",
            correlation_id = %correlation_id,
            job_id = %req.last_checkpoint_id
        )
        .entered();
        // last_checkpoint_id carries the job_id on restart
        let job_id = &req.last_checkpoint_id;

        // Look up the latest committed checkpoint for this job
        let meta = match self.checkpoint_engine.checkpoint_registry() {
            None => {
                return Ok(Response::new(RecoverWorkerResponse {
                    can_recover: false,
                    checkpoint_path: String::new(),
                    assigned_shards: vec![],
                }))
            }
            Some(reg) => {
                let reg = reg.lock().unwrap();
                match reg.latest_for_job(job_id) {
                    None => {
                        return Ok(Response::new(RecoverWorkerResponse {
                            can_recover: false,
                            checkpoint_path: String::new(),
                            assigned_shards: vec![],
                        }))
                    }
                    Some(m) => m,
                }
            }
        };

        // Recompute shard assignments for the current alive worker pool.
        // This handles mismatched worker counts (e.g. saved with 8, resuming with 4).
        let alive = self.tracker.alive_worker_ids().await;
        let datasets = self.registry.list_for_job(job_id).await;
        let num_shards = datasets.first().map(|d| d.num_shards).unwrap_or(0);

        let assigned_shards = if num_shards > 0 && !alive.is_empty() {
            shard_map::compute_shard_map(num_shards, &alive)
                .unwrap_or_default()
                .remove(&req.worker_id)
                .unwrap_or_default()
        } else {
            vec![]
        };

        let checkpoint_path = format!("checkpoints/{}/committed/", meta.checkpoint_id);

        tracing::info!(
            worker_id = %req.worker_id,
            job_id = %job_id,
            step = meta.step,
            epoch = meta.epoch,
            checkpoint_id = %meta.checkpoint_id,
            "worker resuming from checkpoint"
        );

        Ok(Response::new(RecoverWorkerResponse {
            can_recover: true,
            checkpoint_path,
            assigned_shards,
        }))
    }

    async fn get_job_status(
        &self,
        request: Request<GetJobStatusRequest>,
    ) -> Result<Response<GetJobStatusResponse>, Status> {
        let incoming_correlation_id = correlation_id_from_metadata(&request);
        let req = request.into_inner();
        let job_id = req.job_id;
        let correlation_id =
            incoming_correlation_id.unwrap_or_else(|| fresh_correlation_id(Some(&job_id)));
        let _span = tracing::info_span!(
            "coordinator.get_job_status",
            correlation_id = %correlation_id,
            job_id = %job_id
        )
        .entered();
        let datasets: Vec<DatasetSummary> = self
            .registry
            .list_for_job(&job_id)
            .await
            .into_iter()
            .map(|d| DatasetSummary {
                dataset_id: d.dataset_id,
                uri: d.uri,
                format: d.format,
                num_shards: d.num_shards,
            })
            .collect();
        let (alive_workers, total_workers) = self.tracker.worker_counts().await;
        let rebalance_generation = self.registry.rebalance_generation().await;
        Ok(Response::new(GetJobStatusResponse {
            job_id,
            datasets,
            alive_workers,
            total_workers,
            rebalance_generation,
        }))
    }

    async fn list_checkpoints(
        &self,
        request: Request<ListCheckpointsRequest>,
    ) -> Result<Response<ListCheckpointsResponse>, Status> {
        let incoming_correlation_id = correlation_id_from_metadata(&request);
        let req = request.into_inner();
        let job_id = req.job_id;
        let correlation_id =
            incoming_correlation_id.unwrap_or_else(|| fresh_correlation_id(Some(&job_id)));
        let _span = tracing::info_span!(
            "coordinator.list_checkpoints",
            correlation_id = %correlation_id,
            job_id = %job_id
        )
        .entered();
        let checkpoints: Vec<CheckpointEntry> = match self.checkpoint_engine.checkpoint_registry() {
            None => vec![],
            Some(reg) => {
                let reg = reg.lock().unwrap();
                reg.list(&job_id)
                    .into_iter()
                    .map(|m| CheckpointEntry {
                        version: m.version,
                        checkpoint_id: m.checkpoint_id,
                        job_id: m.job_id,
                        epoch: m.epoch,
                        step: m.step,
                        committed_at_secs: m.committed_at_secs,
                        total_bytes: m.total_bytes,
                    })
                    .collect()
            }
        };
        Ok(Response::new(ListCheckpointsResponse { checkpoints }))
    }

    async fn get_checkpoint_restore_info(
        &self,
        request: Request<GetCheckpointRestoreInfoRequest>,
    ) -> Result<Response<GetCheckpointRestoreInfoResponse>, Status> {
        let correlation_id = correlation_id_for_request(&request, None);
        let req = request.into_inner();
        let version = req.version;
        let _span = tracing::info_span!(
            "coordinator.get_checkpoint_restore_info",
            correlation_id = %correlation_id,
            version
        )
        .entered();
        let meta = match self.checkpoint_engine.checkpoint_registry() {
            None => None,
            Some(reg) => {
                let reg = reg.lock().unwrap();
                reg.get_by_version(version)
            }
        };
        let response = match meta {
            None => GetCheckpointRestoreInfoResponse {
                found: false,
                version: 0,
                checkpoint_id: String::new(),
                job_id: String::new(),
                epoch: 0,
                step: 0,
                committed_path: String::new(),
            },
            Some(m) => {
                let committed_path =
                    checkpoint_engine::ResumeInfo::from_metadata(&m).committed_path;
                GetCheckpointRestoreInfoResponse {
                    found: true,
                    version: m.version,
                    checkpoint_id: m.checkpoint_id,
                    job_id: m.job_id,
                    epoch: m.epoch,
                    step: m.step,
                    committed_path,
                }
            }
        };
        Ok(Response::new(response))
    }
}

fn uuid_v4_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{t:x}")
}

fn correlation_id_for_request<T>(request: &Request<T>, job_id: Option<&str>) -> String {
    correlation_id_from_metadata(request).unwrap_or_else(|| fresh_correlation_id(job_id))
}

fn correlation_id_from_metadata<T>(request: &Request<T>) -> Option<String> {
    request
        .metadata()
        .get("x-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn fresh_correlation_id(job_id: Option<&str>) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let nonce = COUNTER.fetch_add(1, Ordering::Relaxed);
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    match job_id {
        Some(job_id) if !job_id.is_empty() => {
            format!("job-{job_id}-{now_nanos:x}-{nonce:x}")
        }
        _ => format!("corr-{now_nanos:x}-{nonce:x}"),
    }
}
