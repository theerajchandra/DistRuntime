use coordinator::{CoordinatorServiceImpl, DatasetRegistry, LivenessTracker};
use proto_gen::distruntime::coordinator_service_client::CoordinatorServiceClient;
use proto_gen::distruntime::coordinator_service_server::CoordinatorServiceServer;
use proto_gen::distruntime::{RegisterDatasetRequest, WorkerInfo, WorkerReadyRequest};
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Server;

const TEST_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

async fn start_coordinator(
    tracker: LivenessTracker,
    registry: DatasetRegistry,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}");

    let svc = CoordinatorServiceImpl::new(tracker, registry);

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
async fn register_dataset_via_grpc() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("coordinator=trace")
        .with_test_writer()
        .try_init();

    let tracker = LivenessTracker::new(TEST_HEARTBEAT_INTERVAL);
    let registry = DatasetRegistry::new();

    let (url, server_handle) = start_coordinator(tracker.clone(), registry.clone()).await;

    let mut client = CoordinatorServiceClient::connect(url).await.unwrap();

    // Register two workers first.
    for port in [9001u32, 9002] {
        client
            .worker_ready(WorkerReadyRequest {
                info: Some(WorkerInfo {
                    worker_id: String::new(),
                    address: "127.0.0.1".into(),
                    port,
                }),
                capabilities: vec![],
            })
            .await
            .unwrap();
    }

    // Register a dataset.
    let resp = client
        .register_dataset(RegisterDatasetRequest {
            job_id: "job-1".into(),
            uri: "s3://bucket/data/".into(),
            format: "parquet".into(),
            num_shards: 10,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(resp.accepted);
    assert!(!resp.dataset_id.is_empty());

    // Verify the registry contains it with correct assignments.
    let ds = registry.get(&resp.dataset_id).await.unwrap();
    assert_eq!(ds.num_shards, 10);
    assert_eq!(ds.assignments.len(), 2);

    let total_assigned: u64 = ds
        .assignments
        .values()
        .flat_map(|ranges| ranges.iter())
        .map(|r| r.end - r.start)
        .sum();
    assert_eq!(total_assigned, 10);

    // Verify list_for_job returns the dataset.
    let datasets = registry.list_for_job("job-1").await;
    assert_eq!(datasets.len(), 1);
    assert_eq!(datasets[0].dataset_id, resp.dataset_id);

    server_handle.abort();
}

#[tokio::test]
async fn register_dataset_fails_without_workers() {
    let tracker = LivenessTracker::new(TEST_HEARTBEAT_INTERVAL);
    let registry = DatasetRegistry::new();

    let (url, server_handle) = start_coordinator(tracker, registry).await;
    let mut client = CoordinatorServiceClient::connect(url).await.unwrap();

    let result = client
        .register_dataset(RegisterDatasetRequest {
            job_id: "job-1".into(),
            uri: "s3://bucket/data/".into(),
            format: "parquet".into(),
            num_shards: 10,
        })
        .await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);

    server_handle.abort();
}
