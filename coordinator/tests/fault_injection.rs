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
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::task::{AbortHandle, JoinHandle};
use tonic::transport::Server;
use tonic::Code;
use worker_client::WorkerClient;

const HB: Duration = Duration::from_millis(80);

#[derive(Clone, Copy)]
enum EngineMode {
    Plain,
    WithRegistry { retention: usize },
}

impl EngineMode {
    fn build(self) -> CheckpointEngine {
        match self {
            EngineMode::Plain => CheckpointEngine::new(),
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

struct IntegrationHarness {
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

impl IntegrationHarness {
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
            next_port: 9000,
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

        // Let a couple heartbeats pass so all workers are marked alive.
        tokio::time::sleep(self.heartbeat_interval * 2).await;
    }

    fn kill_worker(&self, index: usize) -> String {
        let worker = self.workers.get(index).expect("worker index out of range");
        worker.stop_heartbeats();
        worker.worker_id.clone()
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct FaultPlan {
    kill_step: u64,
    kill_worker_index: usize,
    restart_after_commits: usize,
    checkpoint_step: u64,
    io_delays_ms: Vec<u64>,
}

impl FaultPlan {
    fn from_seed(seed: u64, worker_count: usize) -> Self {
        assert!(worker_count > 0, "fault plan requires workers");
        let mut rng = StdRng::seed_from_u64(seed);
        let kill_worker_index = rng.gen_range(0..worker_count);
        let kill_step = rng.gen_range(2..=6);
        let restart_after_commits = if worker_count == 1 {
            1
        } else {
            rng.gen_range(1..worker_count)
        };
        let checkpoint_step = rng.gen_range(100..=1000);
        let io_delays_ms = (0..worker_count).map(|_| rng.gen_range(15..=45)).collect();

        Self {
            kill_step,
            kill_worker_index,
            restart_after_commits,
            checkpoint_step,
            io_delays_ms,
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

#[test]
fn fault_plan_is_repeatable_for_same_seed() {
    let plan_a = FaultPlan::from_seed(0xD157_2200, 3);
    let plan_b = FaultPlan::from_seed(0xD157_2200, 3);
    assert_eq!(plan_a, plan_b);
}

#[tokio::test]
async fn kill_worker_at_step_rebalances_without_gaps_or_overlap() {
    let harness = IntegrationHarness::start(3, HB, EngineMode::Plain).await;
    let plan = FaultPlan::from_seed(0xD157_2201, harness.workers.len());
    let mut client = harness.coordinator_client().await;

    let register = client
        .register_dataset(RegisterDatasetRequest {
            job_id: "job-fault-kill".into(),
            uri: "s3://bucket/shards/".into(),
            format: "parquet".into(),
            num_shards: 12,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(register.accepted);

    let victim_id = harness.workers[plan.kill_worker_index].worker_id.clone();

    for step in 0..=(plan.kill_step + 1) {
        for worker in &harness.workers {
            worker
                .client
                .update_throughput(0, step, 1000.0 + step as f32, 2048.0 + step as f32);
        }
        if step == plan.kill_step {
            let killed = harness.kill_worker(plan.kill_worker_index);
            assert_eq!(killed, victim_id);
        }
        tokio::time::sleep(harness.heartbeat_interval).await;
    }

    harness
        .wait_for_rebalance_generation(1, Duration::from_secs(5))
        .await;

    let dataset = harness.registry.get(&register.dataset_id).await.unwrap();
    let survivor_ranges: Vec<Vec<ShardRange>> = dataset
        .assignments
        .iter()
        .filter(|(worker_id, _)| *worker_id != &victim_id)
        .map(|(_, ranges)| ranges.clone())
        .collect();
    assert_exact_partition(&survivor_ranges, dataset.num_shards);
    assert!(
        !dataset.assignments.contains_key(&victim_id),
        "dead worker should be removed from shard assignments"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn slow_io_delay_still_commits_checkpoint_deterministically() {
    let harness =
        IntegrationHarness::start(2, HB, EngineMode::WithRegistry { retention: 10 }).await;
    let plan = FaultPlan::from_seed(0xD157_2202, harness.workers.len());
    let mut client = harness.coordinator_client().await;

    let begin = client
        .checkpoint_begin(CheckpointBeginRequest {
            job_id: "job-fault-slow-io".into(),
            epoch: 3,
            step: plan.checkpoint_step,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!begin.checkpoint_id.is_empty());

    let bytes_per_worker = 4096u64;
    let total_delay_ms: u64 = plan.io_delays_ms.iter().sum();
    let started = Instant::now();

    for (idx, worker) in harness.workers.iter().enumerate() {
        tokio::time::sleep(Duration::from_millis(plan.io_delays_ms[idx])).await;
        let commit = client
            .checkpoint_commit(CheckpointCommitRequest {
                checkpoint_id: begin.checkpoint_id.clone(),
                worker_id: worker.worker_id.clone(),
                bytes_written: bytes_per_worker,
            })
            .await
            .unwrap()
            .into_inner();

        let should_be_final_commit = idx + 1 == harness.workers.len();
        assert_eq!(commit.success, should_be_final_commit);
    }

    assert!(
        started.elapsed() >= Duration::from_millis(total_delay_ms),
        "commit path should include injected delays"
    );

    let checkpoints = client
        .list_checkpoints(ListCheckpointsRequest {
            job_id: "job-fault-slow-io".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .checkpoints;
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0].step, plan.checkpoint_step);
    assert_eq!(
        checkpoints[0].total_bytes,
        bytes_per_worker * harness.workers.len() as u64
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn coordinator_restart_mid_checkpoint_requires_retry() {
    let mut harness = IntegrationHarness::start(3, HB, EngineMode::Plain).await;
    let plan = FaultPlan::from_seed(0xD157_2203, harness.workers.len());
    let mut client = harness.coordinator_client().await;

    let begin = client
        .checkpoint_begin(CheckpointBeginRequest {
            job_id: "job-fault-restart".into(),
            epoch: 7,
            step: plan.checkpoint_step,
        })
        .await
        .unwrap()
        .into_inner();

    let mut committed_worker_ids = Vec::new();
    for worker in harness.workers.iter().take(plan.restart_after_commits) {
        committed_worker_ids.push(worker.worker_id.clone());
        let commit = client
            .checkpoint_commit(CheckpointCommitRequest {
                checkpoint_id: begin.checkpoint_id.clone(),
                worker_id: worker.worker_id.clone(),
                bytes_written: 1024,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(
            !commit.success,
            "checkpoint should still be pending before restart"
        );
    }

    harness.restart_coordinator(3).await;
    let mut restarted_client = harness.coordinator_client().await;

    let err = restarted_client
        .checkpoint_commit(CheckpointCommitRequest {
            checkpoint_id: begin.checkpoint_id.clone(),
            worker_id: committed_worker_ids[0].clone(),
            bytes_written: 1024,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    let retry = restarted_client
        .checkpoint_begin(CheckpointBeginRequest {
            job_id: "job-fault-restart".into(),
            epoch: 7,
            step: plan.checkpoint_step + 1,
        })
        .await
        .unwrap()
        .into_inner();

    for (idx, worker) in harness.workers.iter().enumerate() {
        let commit = restarted_client
            .checkpoint_commit(CheckpointCommitRequest {
                checkpoint_id: retry.checkpoint_id.clone(),
                worker_id: worker.worker_id.clone(),
                bytes_written: 2048,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(commit.success, idx + 1 == harness.workers.len());
    }

    harness.shutdown().await;
}
