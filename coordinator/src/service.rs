use proto_gen::distruntime::coordinator_service_server::CoordinatorService;
use proto_gen::distruntime::{
    CheckpointAbortRequest, CheckpointAbortResponse, CheckpointBeginRequest,
    CheckpointBeginResponse, CheckpointCommitRequest, CheckpointCommitResponse,
    DatasetShardAssignment, HeartbeatRequest, HeartbeatResponse, RecoverWorkerRequest,
    RecoverWorkerResponse, RegisterDatasetRequest, RegisterDatasetResponse, WorkerReadyRequest,
    WorkerReadyResponse,
};
use tonic::{Request, Response, Status};

use crate::registry::DatasetRegistry;
use crate::shard_map;
use crate::tracker::LivenessTracker;

pub struct CoordinatorServiceImpl {
    tracker: LivenessTracker,
    registry: DatasetRegistry,
}

impl CoordinatorServiceImpl {
    pub fn new(tracker: LivenessTracker, registry: DatasetRegistry) -> Self {
        Self { tracker, registry }
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
        _request: Request<CheckpointBeginRequest>,
    ) -> Result<Response<CheckpointBeginResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn checkpoint_commit(
        &self,
        _request: Request<CheckpointCommitRequest>,
    ) -> Result<Response<CheckpointCommitResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn checkpoint_abort(
        &self,
        _request: Request<CheckpointAbortRequest>,
    ) -> Result<Response<CheckpointAbortResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn recover_worker(
        &self,
        _request: Request<RecoverWorkerRequest>,
    ) -> Result<Response<RecoverWorkerResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
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
