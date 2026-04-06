use coordinator::{CoordinatorServiceImpl, DatasetRegistry, LivenessTracker};
use proto_gen::distruntime::coordinator_service_client::CoordinatorServiceClient;
use proto_gen::distruntime::coordinator_service_server::CoordinatorServiceServer;
use proto_gen::distruntime::{RegisterDatasetRequest, ShardRange};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tonic::transport::Server;
use worker_client::WorkerClient;

const TEST_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

/// Each shard index in `[0, num_shards)` appears in exactly one range across all workers.
fn assert_exact_partition(ranges_by_worker: &[Vec<ShardRange>], num_shards: u64) {
    let mut counts = vec![0u8; num_shards as usize];
    for ranges in ranges_by_worker {
        for r in ranges {
            assert!(r.start < r.end, "invalid range {:?}", r);
            for i in r.start..r.end {
                assert!(
                    (i as usize) < counts.len(),
                    "range {:?} exceeds num_shards {}",
                    r,
                    num_shards
                );
                counts[i as usize] += 1;
            }
        }
    }
    assert!(
        counts.iter().all(|&c| c == 1),
        "expected each shard index covered exactly once, got {:?}",
        counts
    );
}

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
async fn rebalance_after_worker_failure_no_gap_no_overlap() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("coordinator=info,worker_client=info")
        .with_test_writer()
        .try_init();

    let tracker = LivenessTracker::new(TEST_HEARTBEAT_INTERVAL);
    let registry = DatasetRegistry::new();
    let reaper = tracker.spawn_reaper_with_registry(registry.clone());

    let (url, server_handle) = start_coordinator(tracker.clone(), registry.clone()).await;

    let mut w1 = WorkerClient::connect_with_interval(&url, TEST_HEARTBEAT_INTERVAL)
        .await
        .unwrap();
    let id1 = w1.register("127.0.0.1", 9001).await.unwrap();

    let mut w2 = WorkerClient::connect_with_interval(&url, TEST_HEARTBEAT_INTERVAL)
        .await
        .unwrap();
    let id2 = w2.register("127.0.0.1", 9002).await.unwrap();

    let mut w3 = WorkerClient::connect_with_interval(&url, TEST_HEARTBEAT_INTERVAL)
        .await
        .unwrap();
    let id3 = w3.register("127.0.0.1", 9003).await.unwrap();

    let (_h1, _a1) = w1.start_heartbeat();
    let (_h2, _a2) = w2.start_heartbeat();
    let (_h3, a3) = w3.start_heartbeat();

    tokio::time::sleep(TEST_HEARTBEAT_INTERVAL * 3).await;

    let mut grpc = CoordinatorServiceClient::connect(url.clone())
        .await
        .unwrap();
    let reg_resp = grpc
        .register_dataset(RegisterDatasetRequest {
            job_id: "job-rb".into(),
            uri: "s3://bucket/shards/".into(),
            format: "parquet".into(),
            num_shards: 9,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(reg_resp.accepted);
    let ds_id = reg_resp.dataset_id;

    let ds = registry.get(&ds_id).await.unwrap();
    assert_exact_partition(
        &[
            ds.assignments.get(&id1).cloned().unwrap_or_default(),
            ds.assignments.get(&id2).cloned().unwrap_or_default(),
            ds.assignments.get(&id3).cloned().unwrap_or_default(),
        ],
        9,
    );

    assert_eq!(registry.rebalance_generation().await, 0);

    // Kill worker 3 mid-run (stop heartbeats).
    a3.abort();
    let fail_start = Instant::now();

    loop {
        if registry.rebalance_generation().await >= 1 {
            break;
        }
        assert!(
            fail_start.elapsed() < Duration::from_secs(10),
            "rebalance did not complete within 10s of failure detection"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    assert!(
        fail_start.elapsed() < Duration::from_secs(10),
        "rebalance completed within 10s"
    );

    let ds = registry.get(&ds_id).await.unwrap();
    assert_exact_partition(
        &[
            ds.assignments.get(&id1).cloned().unwrap_or_default(),
            ds.assignments.get(&id2).cloned().unwrap_or_default(),
        ],
        9,
    );
    assert!(!ds.assignments.contains_key(&id3));

    tracker.shutdown();
    server_handle.abort();
    let _ = reaper.await;
}
