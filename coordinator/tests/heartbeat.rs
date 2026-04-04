use coordinator::{CoordinatorServiceImpl, DatasetRegistry, LivenessTracker};
use proto_gen::distruntime::coordinator_service_server::CoordinatorServiceServer;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Server;
use worker_client::WorkerClient;

/// Use a fast heartbeat interval so the test completes quickly.
const TEST_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

async fn start_coordinator(tracker: LivenessTracker) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}");

    let svc = CoordinatorServiceImpl::new(tracker, DatasetRegistry::new());

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(CoordinatorServiceServer::new(svc))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Give the server a moment to start accepting connections.
    tokio::time::sleep(Duration::from_millis(50)).await;

    (url, handle)
}

#[tokio::test]
async fn worker_failure_detected_after_missed_heartbeats() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("coordinator=trace,worker_client=trace")
        .with_test_writer()
        .try_init();

    let tracker = LivenessTracker::new(TEST_HEARTBEAT_INTERVAL);
    let reaper = tracker.spawn_reaper();

    let (url, server_handle) = start_coordinator(tracker.clone()).await;

    // Connect a worker and register it.
    let mut worker = WorkerClient::connect_with_interval(&url, TEST_HEARTBEAT_INTERVAL)
        .await
        .unwrap();
    let worker_id = worker.register("127.0.0.1", 9999).await.unwrap();
    assert!(!worker_id.is_empty());

    // Start heartbeat loop -- worker is alive.
    let (_hb_handle, abort) = worker.start_heartbeat();

    // Let a few heartbeats go through.
    tokio::time::sleep(TEST_HEARTBEAT_INTERVAL * 5).await;

    // No failures yet.
    assert!(
        tracker.failed_workers().await.is_empty(),
        "no workers should have failed yet"
    );

    // Silence the worker by aborting its heartbeat loop.
    abort.abort();

    // Wait for 4x the heartbeat interval (> 3x threshold) so the reaper fires.
    tokio::time::sleep(TEST_HEARTBEAT_INTERVAL * 5).await;

    let failed = tracker.failed_workers().await;
    assert_eq!(failed.len(), 1, "exactly one worker should have failed");
    assert_eq!(failed[0].0, worker_id);

    // Cleanup.
    tracker.shutdown();
    let _ = reaper.await;
    server_handle.abort();
}
