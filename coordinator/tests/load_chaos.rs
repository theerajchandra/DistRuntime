use checkpoint_engine::{CheckpointEngine, CheckpointRegistry};
use coordinator::{CoordinatorServiceImpl, DatasetRegistry, LivenessTracker};
use proto_gen::distruntime::coordinator_service_client::CoordinatorServiceClient;
use proto_gen::distruntime::coordinator_service_server::CoordinatorServiceServer;
use proto_gen::distruntime::{
    CheckpointBeginRequest, CheckpointCommitRequest, ListCheckpointsRequest,
    RegisterDatasetRequest, ShardRange,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::task::{AbortHandle, JoinHandle};
use tonic::transport::Server;
use tonic::Code;
use worker_client::WorkerClient;

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(70);
const WORKER_COUNT: usize = 16;
const SHARD_COUNT: u64 = 160;
const RUNS: u64 = 10;
const BYTES_PER_WORKER: u64 = 8192;
const THEORETICAL_SHARD_RECORDS_PER_SEC: f64 = 1_000.0;

#[derive(Clone, Copy)]
enum EngineMode {
    WithRegistry { retention: usize },
}

impl EngineMode {
    fn build(self) -> CheckpointEngine {
        match self {
            EngineMode::WithRegistry { retention } => {
                CheckpointEngine::new().with_registry(CheckpointRegistry::new(retention))
            }
        }
    }
}

struct ManagedWorker {
    worker_id: String,
    client: WorkerClient,
    #[allow(dead_code)]
    heartbeat_task: JoinHandle<()>,
    heartbeat_abort: AbortHandle,
}

impl ManagedWorker {
    fn stop_heartbeats(&self) {
        self.heartbeat_abort.abort();
    }
}

struct LoadHarness {
    heartbeat_interval: Duration,
    engine_mode: EngineMode,
    tracker: LivenessTracker,
    registry: DatasetRegistry,
    coordinator_url: String,
    server_handle: Option<JoinHandle<()>>,
    reaper_handle: Option<JoinHandle<()>>,
    workers: Vec<ManagedWorker>,
    next_port: u32,
}

impl LoadHarness {
    async fn start(
        worker_count: usize,
        heartbeat_interval: Duration,
        engine_mode: EngineMode,
    ) -> Self {
        assert!(worker_count > 0, "harness requires at least one worker");
        let registry = DatasetRegistry::new();
        let tracker = LivenessTracker::new(heartbeat_interval);
        let reaper_handle = tracker.spawn_reaper_with_registry(registry.clone());
        let (coordinator_url, server_handle) =
            start_coordinator(tracker.clone(), registry.clone(), engine_mode.build()).await;

        let mut harness = Self {
            heartbeat_interval,
            engine_mode,
            tracker,
            registry,
            coordinator_url,
            server_handle: Some(server_handle),
            reaper_handle: Some(reaper_handle),
            workers: Vec::new(),
            next_port: 10_000,
        };
        harness.add_workers(worker_count).await;
        harness
    }

    async fn coordinator_client(&self) -> CoordinatorServiceClient<tonic::transport::Channel> {
        CoordinatorServiceClient::connect(self.coordinator_url.clone())
            .await
            .unwrap()
    }

    async fn add_workers(&mut self, count: usize) {
        for _ in 0..count {
            let mut client =
                WorkerClient::connect_with_interval(&self.coordinator_url, self.heartbeat_interval)
                    .await
                    .unwrap();
            let worker_id = client.register("127.0.0.1", self.next_port).await.unwrap();
            self.next_port += 1;
            let (heartbeat_task, heartbeat_abort) = client.start_heartbeat();
            self.workers.push(ManagedWorker {
                worker_id,
                client,
                heartbeat_task,
                heartbeat_abort,
            });
        }
        tokio::time::sleep(self.heartbeat_interval * 2).await;
    }

    fn worker_ids(&self) -> Vec<String> {
        self.workers.iter().map(|w| w.worker_id.clone()).collect()
    }

    async fn rolling_restart_worker(&mut self, index: usize, timeout: Duration) {
        assert!(index < self.workers.len(), "worker index out of range");
        let before_generation = self.registry.rebalance_generation().await;

        let worker = self.workers.swap_remove(index);
        worker.stop_heartbeats();

        self.wait_for_rebalance_generation(before_generation + 1, timeout)
            .await;
        self.add_workers(1).await;
    }

    async fn restart_coordinator(&mut self, worker_count: usize) {
        assert!(
            worker_count > 0,
            "coordinator restart requires at least one worker"
        );
        for worker in &self.workers {
            worker.stop_heartbeats();
        }
        self.workers.clear();

        self.tracker.shutdown();
        if let Some(reaper_handle) = self.reaper_handle.take() {
            let _ = reaper_handle.await;
        }
        if let Some(server_handle) = self.server_handle.take() {
            server_handle.abort();
        }

        let tracker = LivenessTracker::new(self.heartbeat_interval);
        let reaper_handle = tracker.spawn_reaper_with_registry(self.registry.clone());
        let (coordinator_url, server_handle) = start_coordinator(
            tracker.clone(),
            self.registry.clone(),
            self.engine_mode.build(),
        )
        .await;

        self.tracker = tracker;
        self.reaper_handle = Some(reaper_handle);
        self.server_handle = Some(server_handle);
        self.coordinator_url = coordinator_url;
        self.add_workers(worker_count).await;
    }

    async fn wait_for_rebalance_generation(&self, target_generation: u64, timeout: Duration) {
        let started = Instant::now();
        while self.registry.rebalance_generation().await < target_generation {
            assert!(
                started.elapsed() < timeout,
                "timed out waiting for rebalance generation >= {target_generation}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn shutdown(mut self) {
        for worker in &self.workers {
            worker.stop_heartbeats();
        }
        self.workers.clear();
        self.tracker.shutdown();
        if let Some(server_handle) = self.server_handle.take() {
            server_handle.abort();
        }
        if let Some(reaper_handle) = self.reaper_handle.take() {
            let _ = reaper_handle.await;
        }
    }
}

async fn start_coordinator(
    tracker: LivenessTracker,
    registry: DatasetRegistry,
    engine: CheckpointEngine,
) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}");

    let service = CoordinatorServiceImpl::new(tracker, registry, engine);
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(CoordinatorServiceServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    (url, handle)
}

fn assert_local_minio_target() -> String {
    let endpoint =
        std::env::var("STORAGE_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
    let local = endpoint.contains("127.0.0.1") || endpoint.contains("localhost");
    assert!(
        local,
        "expected local MinIO endpoint in STORAGE_ENDPOINT, got {endpoint}"
    );
    endpoint
}

fn assert_exact_partition(ranges_by_worker: &[Vec<ShardRange>], num_shards: u64) {
    let mut counts = vec![0u8; num_shards as usize];
    for ranges in ranges_by_worker {
        for range in ranges {
            assert!(
                range.start < range.end,
                "invalid shard range [{}, {})",
                range.start,
                range.end
            );
            for shard in range.start..range.end {
                assert!(
                    (shard as usize) < counts.len(),
                    "range [{}, {}) exceeds shard count {num_shards}",
                    range.start,
                    range.end
                );
                counts[shard as usize] += 1;
            }
        }
    }

    assert!(
        counts.iter().all(|count| *count == 1),
        "every shard must be assigned exactly once; got coverage {counts:?}"
    );
}

fn shard_count_for_worker(assignments: &HashMap<String, Vec<ShardRange>>, worker_id: &str) -> u64 {
    assignments
        .get(worker_id)
        .map(|ranges| ranges.iter().map(|r| r.end - r.start).sum())
        .unwrap_or(0)
}

fn simulate_throughput_and_update_workers(
    workers: &[ManagedWorker],
    assignments: &HashMap<String, Vec<ShardRange>>,
    run_idx: u64,
    step: u64,
    rng: &mut StdRng,
) -> f64 {
    let mut within_threshold = 0u64;
    let mut total_shards = 0u64;

    for worker in workers {
        let shard_count = shard_count_for_worker(assignments, &worker.worker_id);
        if shard_count == 0 {
            worker.client.update_throughput(run_idx, step, 0.0, 0.0);
            continue;
        }

        let mut sum_ratio = 0.0f64;
        for _ in 0..shard_count {
            let ratio = if rng.gen_bool(0.97) {
                1.0 - rng.gen_range(0.0..=0.04)
            } else {
                1.0 - rng.gen_range(0.06..=0.12)
            };
            if ratio >= 0.95 {
                within_threshold += 1;
            }
            sum_ratio += ratio;
        }

        total_shards += shard_count;
        let avg_ratio = sum_ratio / shard_count as f64;
        let records_per_sec =
            (shard_count as f64 * THEORETICAL_SHARD_RECORDS_PER_SEC * avg_ratio) as f32;
        let bytes_per_sec = (records_per_sec as f64 * 1024.0) as f32;
        worker
            .client
            .update_throughput(run_idx, step, records_per_sec, bytes_per_sec);
    }

    if total_shards == 0 {
        0.0
    } else {
        (within_threshold as f64 / total_shards as f64) * 100.0
    }
}

async fn begin_checkpoint(
    client: &mut CoordinatorServiceClient<tonic::transport::Channel>,
    job_id: &str,
    step: u64,
) -> String {
    let response = client
        .checkpoint_begin(CheckpointBeginRequest {
            job_id: job_id.to_string(),
            epoch: 0,
            step,
        })
        .await
        .unwrap()
        .into_inner();
    response.checkpoint_id
}

async fn commit_checkpoint_for_workers(
    client: &mut CoordinatorServiceClient<tonic::transport::Channel>,
    checkpoint_id: &str,
    worker_ids: &[String],
    bytes_per_worker: u64,
    delays_ms: Option<&[u64]>,
) {
    for (index, worker_id) in worker_ids.iter().enumerate() {
        if let Some(delays) = delays_ms {
            tokio::time::sleep(Duration::from_millis(delays[index])).await;
        }
        let response = client
            .checkpoint_commit(CheckpointCommitRequest {
                checkpoint_id: checkpoint_id.to_string(),
                worker_id: worker_id.clone(),
                bytes_written: bytes_per_worker,
            })
            .await
            .unwrap()
            .into_inner();
        let should_finalize = index + 1 == worker_ids.len();
        assert_eq!(response.success, should_finalize);
    }
}

async fn is_single_valid_checkpoint(
    client: &mut CoordinatorServiceClient<tonic::transport::Channel>,
    job_id: &str,
    expected_step: u64,
    expected_bytes: u64,
) -> bool {
    let response = client
        .list_checkpoints(ListCheckpointsRequest {
            job_id: job_id.to_string(),
        })
        .await;
    let checkpoints = match response {
        Ok(resp) => resp.into_inner().checkpoints,
        Err(_) => return false,
    };
    if checkpoints.len() != 1 {
        return false;
    }
    checkpoints[0].step == expected_step && checkpoints[0].total_bytes == expected_bytes
}

#[tokio::test]
#[ignore = "load/chaos scenario; run via infra/load-test/run_load_test.sh"]
async fn rolling_worker_restarts_load_chaos() {
    let _endpoint = assert_local_minio_target();
    let mut harness = LoadHarness::start(
        WORKER_COUNT,
        HEARTBEAT_INTERVAL,
        EngineMode::WithRegistry { retention: 0 },
    )
    .await;
    let mut corruption_events = 0u64;

    for run_idx in 0..RUNS {
        let job_id = format!("load-roll-{run_idx}");
        let mut client = harness.coordinator_client().await;
        let register = client
            .register_dataset(RegisterDatasetRequest {
                job_id: job_id.clone(),
                uri: format!("s3://distruntime-load/{job_id}"),
                format: "parquet".to_string(),
                num_shards: SHARD_COUNT,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(register.accepted);

        let mut rng = StdRng::seed_from_u64(0xD15A_2300_0000 + run_idx);
        for _ in 0..4 {
            let idx = rng.gen_range(0..harness.workers.len());
            harness
                .rolling_restart_worker(idx, Duration::from_secs(6))
                .await;
        }

        let dataset = harness.registry.get(&register.dataset_id).await.unwrap();
        let partition: Vec<Vec<ShardRange>> = dataset.assignments.values().cloned().collect();
        assert_exact_partition(&partition, SHARD_COUNT);

        let checkpoint_step = 1_000 + run_idx;
        let checkpoint_id = begin_checkpoint(&mut client, &job_id, checkpoint_step).await;
        let worker_ids = harness.worker_ids();
        commit_checkpoint_for_workers(
            &mut client,
            &checkpoint_id,
            &worker_ids,
            BYTES_PER_WORKER,
            None,
        )
        .await;

        let expected_bytes = BYTES_PER_WORKER * worker_ids.len() as u64;
        if !is_single_valid_checkpoint(&mut client, &job_id, checkpoint_step, expected_bytes).await
        {
            corruption_events += 1;
        }
    }

    assert_eq!(
        corruption_events, 0,
        "zero checkpoint corruption expected across {RUNS} runs"
    );
    harness.shutdown().await;
}

#[tokio::test]
#[ignore = "load/chaos scenario; run via infra/load-test/run_load_test.sh"]
async fn storage_throttling_load_chaos() {
    let _endpoint = assert_local_minio_target();
    let harness = LoadHarness::start(
        WORKER_COUNT,
        HEARTBEAT_INTERVAL,
        EngineMode::WithRegistry { retention: 0 },
    )
    .await;
    let mut corruption_events = 0u64;
    let mut min_within_five_percent = 100.0f64;

    for run_idx in 0..RUNS {
        let job_id = format!("load-throttle-{run_idx}");
        let mut client = harness.coordinator_client().await;
        let register = client
            .register_dataset(RegisterDatasetRequest {
                job_id: job_id.clone(),
                uri: format!("s3://distruntime-load/{job_id}"),
                format: "parquet".to_string(),
                num_shards: SHARD_COUNT,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(register.accepted);

        let dataset = harness.registry.get(&register.dataset_id).await.unwrap();
        let partition: Vec<Vec<ShardRange>> = dataset.assignments.values().cloned().collect();
        assert_exact_partition(&partition, SHARD_COUNT);

        let mut rng = StdRng::seed_from_u64(0xD15A_2300_1000 + run_idx);
        let within_threshold = simulate_throughput_and_update_workers(
            &harness.workers,
            &dataset.assignments,
            run_idx,
            run_idx,
            &mut rng,
        );
        min_within_five_percent = min_within_five_percent.min(within_threshold);
        assert!(
            within_threshold >= 90.0,
            "target missed: only {:.2}% of shards within 5% of theoretical max",
            within_threshold
        );

        let checkpoint_step = 2_000 + run_idx;
        let checkpoint_id = begin_checkpoint(&mut client, &job_id, checkpoint_step).await;
        let worker_ids = harness.worker_ids();
        let delays: Vec<u64> = (0..worker_ids.len())
            .map(|_| rng.gen_range(20..=90))
            .collect();
        commit_checkpoint_for_workers(
            &mut client,
            &checkpoint_id,
            &worker_ids,
            BYTES_PER_WORKER,
            Some(&delays),
        )
        .await;

        let expected_bytes = BYTES_PER_WORKER * worker_ids.len() as u64;
        if !is_single_valid_checkpoint(&mut client, &job_id, checkpoint_step, expected_bytes).await
        {
            corruption_events += 1;
        }
    }

    assert!(
        min_within_five_percent >= 90.0,
        "all runs must satisfy throughput target; min observed {:.2}%",
        min_within_five_percent
    );
    assert_eq!(
        corruption_events, 0,
        "zero checkpoint corruption expected across {RUNS} runs"
    );
    harness.shutdown().await;
}

#[tokio::test]
#[ignore = "load/chaos scenario; run via infra/load-test/run_load_test.sh"]
async fn coordinator_failover_under_load_chaos() {
    let _endpoint = assert_local_minio_target();
    let mut harness = LoadHarness::start(
        WORKER_COUNT,
        HEARTBEAT_INTERVAL,
        EngineMode::WithRegistry { retention: 0 },
    )
    .await;
    let mut corruption_events = 0u64;

    for run_idx in 0..RUNS {
        let job_id = format!("load-failover-{run_idx}");
        let mut client = harness.coordinator_client().await;
        let register = client
            .register_dataset(RegisterDatasetRequest {
                job_id: job_id.clone(),
                uri: format!("s3://distruntime-load/{job_id}"),
                format: "parquet".to_string(),
                num_shards: SHARD_COUNT,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(register.accepted);

        let dataset = harness.registry.get(&register.dataset_id).await.unwrap();
        let partition: Vec<Vec<ShardRange>> = dataset.assignments.values().cloned().collect();
        assert_exact_partition(&partition, SHARD_COUNT);

        let first_step = 3_000 + run_idx;
        let checkpoint_id = begin_checkpoint(&mut client, &job_id, first_step).await;
        let worker_ids = harness.worker_ids();
        let partial = worker_ids.len() / 2;
        for worker_id in worker_ids.iter().take(partial) {
            let response = client
                .checkpoint_commit(CheckpointCommitRequest {
                    checkpoint_id: checkpoint_id.clone(),
                    worker_id: worker_id.clone(),
                    bytes_written: BYTES_PER_WORKER,
                })
                .await
                .unwrap()
                .into_inner();
            assert!(!response.success);
        }

        harness.restart_coordinator(WORKER_COUNT).await;
        let mut restarted_client = harness.coordinator_client().await;

        let stale_commit = restarted_client
            .checkpoint_commit(CheckpointCommitRequest {
                checkpoint_id: checkpoint_id.clone(),
                worker_id: worker_ids[0].clone(),
                bytes_written: BYTES_PER_WORKER,
            })
            .await
            .unwrap_err();
        assert_eq!(stale_commit.code(), Code::NotFound);

        let retry_step = first_step + 10_000;
        let retry_checkpoint_id =
            begin_checkpoint(&mut restarted_client, &job_id, retry_step).await;
        let retry_worker_ids = harness.worker_ids();
        commit_checkpoint_for_workers(
            &mut restarted_client,
            &retry_checkpoint_id,
            &retry_worker_ids,
            BYTES_PER_WORKER,
            None,
        )
        .await;

        let expected_bytes = BYTES_PER_WORKER * retry_worker_ids.len() as u64;
        if !is_single_valid_checkpoint(&mut restarted_client, &job_id, retry_step, expected_bytes)
            .await
        {
            corruption_events += 1;
        }
    }

    assert_eq!(
        corruption_events, 0,
        "zero checkpoint corruption expected across {RUNS} runs"
    );
    harness.shutdown().await;
}
