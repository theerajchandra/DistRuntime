//! Repeatable, end-to-end scenario used by the README terminal recording.
//!
//! Start a coordinator first, then run this binary with its gRPC URL as the
//! optional first argument.  It intentionally uses the public coordinator API
//! so the README demonstration stays coupled to real runtime behavior.

use std::time::Duration;

use anyhow::{Context, Result};
use proto_gen::distruntime::coordinator_service_client::CoordinatorServiceClient;
use proto_gen::distruntime::{
    CheckpointBeginRequest, CheckpointCommitRequest, GetJobStatusRequest, HeartbeatRequest,
    RecoverWorkerRequest, RegisterDatasetRequest, WorkerInfo, WorkerReadyRequest,
};

const JOB_ID: &str = "training-run";
const DATASET_NAME: &str = "training-data";
const WORKERS: [&str; 3] = ["worker-a", "worker-b", "worker-c"];

#[tokio::main]
async fn main() -> Result<()> {
    let coordinator = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:8787".to_string());
    let mut client = CoordinatorServiceClient::connect(coordinator.clone())
        .await
        .with_context(|| format!("failed to connect to coordinator at {coordinator}"))?;

    println!("DistRuntime • distributed training recovery demo\n");
    println!("1/5  Registering three workers with the coordinator");
    for (index, worker_id) in WORKERS.iter().enumerate() {
        client
            .worker_ready(WorkerReadyRequest {
                info: Some(WorkerInfo {
                    worker_id: (*worker_id).to_string(),
                    address: "127.0.0.1".to_string(),
                    port: 9100 + index as u32,
                }),
                capabilities: vec!["parquet".to_string(), "checkpoint-v1".to_string()],
            })
            .await?
            .into_inner();
    }
    heartbeat_all(&mut client).await?;
    let all_heartbeats = start_heartbeats(client.clone(), &WORKERS);
    println!("     workers: 3 alive / 3 total\n");
    pause().await;

    println!("2/5  Registering {DATASET_NAME} (16 parquet shards)");
    let dataset = client
        .register_dataset(RegisterDatasetRequest {
            job_id: JOB_ID.to_string(),
            uri: "s3://ml-artifacts/training-data/".to_string(),
            format: "parquet".to_string(),
            num_shards: 16,
        })
        .await?
        .into_inner();
    println!(
        "     dataset: {DATASET_NAME} (16 shards) • id={}",
        dataset.dataset_id
    );
    println!("     shard assignments (no overlap):");
    for worker_id in WORKERS {
        let assignments = heartbeat(&mut client, worker_id).await?;
        println!("       {worker_id:<8} {}", format_assignments(&assignments));
    }
    println!();
    pause().await;

    println!("3/5  Committing checkpoint at step 1000");
    let checkpoint = client
        .checkpoint_begin(CheckpointBeginRequest {
            job_id: JOB_ID.to_string(),
            epoch: 4,
            step: 1000,
        })
        .await?
        .into_inner();
    for (index, worker_id) in WORKERS.iter().enumerate() {
        let committed = client
            .checkpoint_commit(CheckpointCommitRequest {
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                worker_id: (*worker_id).to_string(),
                bytes_written: 256 * 1024 * 1024 + index as u64 * 1024,
            })
            .await?
            .into_inner()
            .success;
        println!(
            "     {worker_id:<8} checkpoint shard written{}",
            if committed {
                " • commit complete"
            } else {
                ""
            }
        );
    }
    println!("     checkpoint: step-1000 [committed]\n");
    pause().await;

    println!("4/5  Simulating worker-c failure; heartbeats stop");
    println!("     coordinator detects the missed heartbeat and rebalances");
    all_heartbeats.abort();
    let survivor_heartbeats = start_heartbeats(client.clone(), &["worker-a", "worker-b"]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        heartbeat(&mut client, "worker-a").await?;
        heartbeat(&mut client, "worker-b").await?;
        let status = client
            .get_job_status(GetJobStatusRequest {
                job_id: JOB_ID.to_string(),
            })
            .await?
            .into_inner();
        if status.rebalance_generation >= 1 {
            println!(
                "     workers: {} alive / {} total • rebalance generation {}",
                status.alive_workers, status.total_workers, status.rebalance_generation
            );
            break;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "worker failure was not rebalanced in time"
        );
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
    for worker_id in ["worker-a", "worker-b"] {
        let assignments = heartbeat(&mut client, worker_id).await?;
        println!("       {worker_id:<8} {}", format_assignments(&assignments));
    }
    println!();
    pause().await;

    println!("5/5  Restarting worker-c and recovering from the committed checkpoint");
    client
        .worker_ready(WorkerReadyRequest {
            info: Some(WorkerInfo {
                worker_id: "worker-c".to_string(),
                address: "127.0.0.1".to_string(),
                port: 9102,
            }),
            capabilities: vec!["parquet".to_string(), "checkpoint-v1".to_string()],
        })
        .await?;
    let recovery = client
        .recover_worker(RecoverWorkerRequest {
            worker_id: "worker-c".to_string(),
            last_checkpoint_id: JOB_ID.to_string(),
        })
        .await?
        .into_inner();
    let status = client
        .get_job_status(GetJobStatusRequest {
            job_id: JOB_ID.to_string(),
        })
        .await?
        .into_inner();
    anyhow::ensure!(
        recovery.can_recover,
        "expected a committed checkpoint to recover"
    );
    println!("     restored: {}", recovery.checkpoint_path);
    println!(
        "     worker-c  {}",
        format_ranges(&recovery.assigned_shards)
    );
    println!("\n     final status");
    println!(
        "     workers: {} alive / {} total",
        status.alive_workers, status.total_workers
    );
    println!("     dataset: {DATASET_NAME} (16 shards)");
    println!("     checkpoint: step-1000 [committed]");
    println!("     recovery: ready");

    survivor_heartbeats.abort();

    Ok(())
}

async fn heartbeat_all(
    client: &mut CoordinatorServiceClient<tonic::transport::Channel>,
) -> Result<()> {
    for worker_id in WORKERS {
        heartbeat(client, worker_id).await?;
    }
    Ok(())
}

fn start_heartbeats(
    mut client: CoordinatorServiceClient<tonic::transport::Channel>,
    workers: &[&str],
) -> tokio::task::JoinHandle<()> {
    let workers = workers
        .iter()
        .map(|id| (*id).to_string())
        .collect::<Vec<_>>();
    tokio::spawn(async move {
        loop {
            for worker_id in &workers {
                let _ = heartbeat(&mut client, worker_id).await;
            }
            tokio::time::sleep(Duration::from_millis(75)).await;
        }
    })
}

async fn heartbeat(
    client: &mut CoordinatorServiceClient<tonic::transport::Channel>,
    worker_id: &str,
) -> Result<Vec<proto_gen::distruntime::DatasetShardAssignment>> {
    Ok(client
        .heartbeat(HeartbeatRequest {
            worker_id: worker_id.to_string(),
            epoch: 4,
            step: 1000,
            throughput_samples_per_sec: 12_000.0,
            throughput_bytes_per_sec: 48_000_000.0,
        })
        .await?
        .into_inner()
        .assignments)
}

fn format_assignments(assignments: &[proto_gen::distruntime::DatasetShardAssignment]) -> String {
    let ranges = assignments
        .first()
        .map(|assignment| assignment.shards.as_slice())
        .unwrap_or_default();
    format_ranges(ranges)
}

fn format_ranges(ranges: &[proto_gen::distruntime::ShardRange]) -> String {
    ranges
        .iter()
        .map(|range| format!("shards {}–{}", range.start, range.end.saturating_sub(1)))
        .collect::<Vec<_>>()
        .join(", ")
}

async fn pause() {
    tokio::time::sleep(Duration::from_millis(900)).await;
}
