use checkpoint_engine::{CheckpointEngine, CheckpointRegistry};
use coordinator::{CoordinatorServiceImpl, DatasetRegistry, LivenessTracker};
use proto_gen::distruntime::coordinator_service_server::CoordinatorServiceServer;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Server;
use worker_client::WorkerClient;

const HB: Duration = Duration::from_millis(100);

async fn start_coordinator(
    tracker: LivenessTracker,
    registry: DatasetRegistry,
    engine: CheckpointEngine,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}");
    let svc = CoordinatorServiceImpl::new(tracker, registry, engine);
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(CoordinatorServiceServer::new(svc))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (url, handle)
}

#[tokio::test]
async fn recover_worker_returns_checkpoint_at_correct_step() {
    let tracker = LivenessTracker::new(HB);
    let dataset_registry = DatasetRegistry::new();

    // Build engine with a registry that already has a checkpoint at step 500
    let mut ckpt_reg = CheckpointRegistry::new(0);
    ckpt_reg.record(
        "ckpt-job-recover-0-500",
        "job-recover",
        0,
        500,
        1024,
        None,
        None,
    );
    let engine = CheckpointEngine::new().with_registry(ckpt_reg);

    let (url, server_handle) = start_coordinator(tracker.clone(), dataset_registry, engine).await;

    let mut worker = WorkerClient::connect_with_interval(&url, HB).await.unwrap();
    let worker_id = worker.register("127.0.0.1", 9001).await.unwrap();
    assert!(!worker_id.is_empty());

    let result = worker.recover("job-recover").await.unwrap();
    assert!(result.is_some(), "should be able to recover");

    let (checkpoint_path, _shards) = result.unwrap();
    assert!(
        checkpoint_path.contains("ckpt-job-recover-0-500"),
        "checkpoint path should reference step-500 checkpoint, got: {checkpoint_path}"
    );
    assert!(
        checkpoint_path.ends_with("/committed/"),
        "checkpoint path should point to committed prefix"
    );

    server_handle.abort();
}

#[tokio::test]
async fn recover_worker_returns_none_when_no_checkpoint() {
    let tracker = LivenessTracker::new(HB);
    let engine = CheckpointEngine::new().with_registry(CheckpointRegistry::new(0));

    let (url, server_handle) =
        start_coordinator(tracker.clone(), DatasetRegistry::new(), engine).await;

    let mut worker = WorkerClient::connect_with_interval(&url, HB).await.unwrap();
    worker.register("127.0.0.1", 9001).await.unwrap();

    let result = worker.recover("job-nonexistent").await.unwrap();
    assert!(
        result.is_none(),
        "should return None when no checkpoint exists"
    );

    server_handle.abort();
}

#[tokio::test]
async fn recover_worker_handles_mismatched_worker_count() {
    // Checkpoint was saved with a specific shard layout; we recover with a different
    // number of workers. The coordinator must not crash and must return valid shards.
    let tracker = LivenessTracker::new(HB);
    let dataset_registry = DatasetRegistry::new();

    let mut ckpt_reg = CheckpointRegistry::new(0);
    ckpt_reg.record(
        "ckpt-job-mismatch-0-100",
        "job-mismatch",
        0,
        100,
        512,
        None,
        None,
    );
    let engine = CheckpointEngine::new().with_registry(ckpt_reg);

    let (url, server_handle) = start_coordinator(tracker.clone(), dataset_registry, engine).await;

    // Register only 1 worker (checkpoint was "saved with 8" conceptually, but
    // we're resuming with 1 — coordinator must not crash).
    let mut worker = WorkerClient::connect_with_interval(&url, HB).await.unwrap();
    worker.register("127.0.0.1", 9001).await.unwrap();

    let result = worker.recover("job-mismatch").await.unwrap();
    // can_recover=true; assigned_shards may be empty (no dataset registered) but no crash.
    assert!(result.is_some());

    server_handle.abort();
}
