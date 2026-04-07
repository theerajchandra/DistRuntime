use proto_gen::distruntime::coordinator_service_server::CoordinatorService;
use proto_gen::distruntime::{
    CheckpointAbortRequest, CheckpointAbortResponse, CheckpointBeginRequest,
    CheckpointBeginResponse, CheckpointCommitRequest, CheckpointCommitResponse,
    DatasetShardAssignment, HeartbeatRequest, HeartbeatResponse, RecoverWorkerRequest,
    RecoverWorkerResponse, RegisterDatasetRequest, RegisterDatasetResponse, WorkerReadyRequest,
    WorkerReadyResponse,
};
use tonic::{Request, Response, Status};

use checkpoint_engine::{CheckpointEngine, CheckpointError};

use crate::registry::DatasetRegistry;
use crate::shard_map;
use crate::tracker::LivenessTracker;

pub struct CoordinatorServiceImpl {
    tracker: LivenessTracker,
    registry: DatasetRegistry,
    checkpoint_engine: CheckpointEngine,
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
        }
    }
}

#[tonic::async_trait]
impl CoordinatorService for CoordinatorServiceImpl {
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
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
        let req = request.into_inner();

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
        let req = request.into_inner();
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
        let req = request.into_inner();
        let all_done = self
            .checkpoint_engine
            .worker_commit(&req.checkpoint_id, &req.worker_id, req.bytes_written)
            .await
            .map_err(|e| match e {
                CheckpointError::NotFound(_) => Status::not_found(e.to_string()),
                CheckpointError::InvalidState(_) => Status::failed_precondition(e.to_string()),
                CheckpointError::AlreadyCommitted(_) => Status::already_exists(e.to_string()),
            })?;
        Ok(Response::new(CheckpointCommitResponse {
            success: all_done,
        }))
    }

    async fn checkpoint_abort(
        &self,
        request: Request<CheckpointAbortRequest>,
    ) -> Result<Response<CheckpointAbortResponse>, Status> {
        let req = request.into_inner();
        let acknowledged = self
            .checkpoint_engine
            .abort(&req.checkpoint_id, &req.reason)
            .await;
        Ok(Response::new(CheckpointAbortResponse { acknowledged }))
    }

    async fn recover_worker(
        &self,
        request: Request<RecoverWorkerRequest>,
    ) -> Result<Response<RecoverWorkerResponse>, Status> {
        let req = request.into_inner();
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
}

fn uuid_v4_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{t:x}")
}
