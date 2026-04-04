use proto_gen::distruntime::coordinator_service_server::CoordinatorService;
use proto_gen::distruntime::{
    CheckpointAbortRequest, CheckpointAbortResponse, CheckpointBeginRequest,
    CheckpointBeginResponse, CheckpointCommitRequest, CheckpointCommitResponse, HeartbeatRequest,
    HeartbeatResponse, RecoverWorkerRequest, RecoverWorkerResponse, WorkerReadyRequest,
    WorkerReadyResponse,
};
use tonic::{Request, Response, Status};

use crate::tracker::LivenessTracker;

pub struct CoordinatorServiceImpl {
    tracker: LivenessTracker,
}

impl CoordinatorServiceImpl {
    pub fn new(tracker: LivenessTracker) -> Self {
        Self { tracker }
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
        Ok(Response::new(HeartbeatResponse {
            acknowledged: true,
            directive: String::new(),
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
