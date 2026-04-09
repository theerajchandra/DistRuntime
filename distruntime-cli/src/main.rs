//! DistRuntime coordinator CLI — client commands and optional local server.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use clap::{Parser, Subcommand};
use proto_gen::distruntime::coordinator_service_client::CoordinatorServiceClient;
use proto_gen::distruntime::{
    GetCheckpointRestoreInfoRequest, GetJobStatusRequest, ListCheckpointsRequest,
};
use tonic::transport::{Channel, Server};
use tonic::Request;

use checkpoint_engine::{CheckpointEngine, CheckpointRegistry};
use coordinator::{CoordinatorServiceImpl, DatasetRegistry, LivenessTracker};
use proto_gen::distruntime::coordinator_service_server::CoordinatorServiceServer;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

const DEFAULT_COORDINATOR: &str = "http://127.0.0.1:8787";

#[derive(Parser)]
#[command(name = "distruntime", version, about = "DistRuntime coordinator CLI")]
struct Cli {
    /// Coordinator gRPC base URL (client commands only).
    #[arg(
        long,
        env = "DISTRUNTIME_COORDINATOR",
        default_value = DEFAULT_COORDINATOR,
        global = true
    )]
    coordinator: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the gRPC coordinator server on this machine.
    StartCoordinator {
        /// Address and port to listen on (e.g. 127.0.0.1:8787).
        #[arg(long, default_value = "127.0.0.1:8787")]
        listen: String,
        /// Optional JSON file to persist the dataset registry.
        #[arg(long)]
        registry_path: Option<PathBuf>,
        /// Heartbeat interval used for worker liveness (also scales dead threshold).
        #[arg(long, default_value_t = 1000)]
        heartbeat_interval_ms: u64,
        /// Optional Prometheus metrics HTTP address (serves /metrics).
        #[arg(long, default_value = "127.0.0.1:9090")]
        metrics_listen: String,
    },
    Job {
        #[command(subcommand)]
        job: JobCmd,
    },
    Checkpoint {
        #[command(subcommand)]
        checkpoint: CheckpointCmd,
    },
}

#[derive(Subcommand)]
enum JobCmd {
    /// Show registered datasets and worker counts for a job.
    Status { job_id: String },
}

#[derive(Subcommand)]
enum CheckpointCmd {
    /// List committed checkpoints for a job (ascending global registry version).
    List { job_id: String },
    /// Look up committed checkpoint path and training step by global registry version.
    Restore {
        /// Global checkpoint registry version (not training step or epoch).
        #[arg(value_name = "VERSION")]
        version: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(true)
        .with_span_list(true)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::StartCoordinator {
            listen,
            registry_path,
            heartbeat_interval_ms,
            metrics_listen,
        } => {
            run_coordinator_server(listen, registry_path, heartbeat_interval_ms, metrics_listen)
                .await?;
        }
        Commands::Job { job } => {
            let mut client = connect_client(&cli.coordinator).await?;
            match job {
                JobCmd::Status { job_id } => {
                    let resp = client
                        .get_job_status(Request::new(GetJobStatusRequest { job_id }))
                        .await
                        .context("GetJobStatus RPC failed")?
                        .into_inner();
                    println!("job_id: {}", resp.job_id);
                    println!(
                        "workers: {} alive / {} total",
                        resp.alive_workers, resp.total_workers
                    );
                    println!("rebalance_generation: {}", resp.rebalance_generation);
                    if resp.datasets.is_empty() {
                        println!("datasets: (none)");
                    } else {
                        println!("datasets:");
                        for d in resp.datasets {
                            println!(
                                "  - {}  uri={}  format={}  num_shards={}",
                                d.dataset_id, d.uri, d.format, d.num_shards
                            );
                        }
                    }
                }
            }
        }
        Commands::Checkpoint { checkpoint } => {
            let mut client = connect_client(&cli.coordinator).await?;
            match checkpoint {
                CheckpointCmd::List { job_id } => {
                    let resp = client
                        .list_checkpoints(Request::new(ListCheckpointsRequest { job_id }))
                        .await
                        .context("ListCheckpoints RPC failed")?
                        .into_inner();
                    for c in resp.checkpoints {
                        println!(
                            "version={} checkpoint_id={} job_id={} epoch={} step={} committed_at_secs={} total_bytes={}",
                            c.version,
                            c.checkpoint_id,
                            c.job_id,
                            c.epoch,
                            c.step,
                            c.committed_at_secs,
                            c.total_bytes
                        );
                    }
                }
                CheckpointCmd::Restore { version } => {
                    let resp = client
                        .get_checkpoint_restore_info(Request::new(
                            GetCheckpointRestoreInfoRequest { version },
                        ))
                        .await
                        .context("GetCheckpointRestoreInfo RPC failed")?
                        .into_inner();
                    if !resp.found {
                        println!("not found: version {}", version);
                    } else {
                        println!("version: {}", resp.version);
                        println!("checkpoint_id: {}", resp.checkpoint_id);
                        println!("job_id: {}", resp.job_id);
                        println!("epoch: {}", resp.epoch);
                        println!("step: {}", resp.step);
                        println!("committed_path: {}", resp.committed_path);
                    }
                }
            }
        }
    }

    Ok(())
}

async fn connect_client(coordinator: &str) -> Result<CoordinatorServiceClient<Channel>> {
    let url = coordinator.trim().to_string();
    CoordinatorServiceClient::connect(url)
        .await
        .context("failed to connect to coordinator")
}

async fn run_coordinator_server(
    listen: String,
    registry_path: Option<PathBuf>,
    heartbeat_interval_ms: u64,
    metrics_listen: String,
) -> Result<()> {
    let hb = Duration::from_millis(heartbeat_interval_ms.max(1));
    let tracker = LivenessTracker::new(hb);
    let registry = match registry_path {
        Some(p) => DatasetRegistry::with_persistence(&p).context("load dataset registry")?,
        None => DatasetRegistry::new(),
    };
    let engine = CheckpointEngine::new().with_registry(CheckpointRegistry::new(0));
    let _reaper = tracker.spawn_reaper_with_registry(registry.clone());

    let listener = TcpListener::bind(&listen)
        .await
        .with_context(|| format!("bind {listen}"))?;
    let addr = listener.local_addr()?;
    tracing::info!(%addr, "coordinator listening");
    spawn_metrics_server(metrics_listen);

    let svc = CoordinatorServiceImpl::new(tracker, registry, engine);
    Server::builder()
        .add_service(CoordinatorServiceServer::new(svc))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .context("coordinator server error")?;
    Ok(())
}

fn spawn_metrics_server(metrics_listen: String) {
    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/metrics",
            get(|| async {
                let payload = coordinator::render_prometheus_metrics();
                (
                    StatusCode::OK,
                    [(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; version=0.0.4"),
                    )],
                    payload,
                )
                    .into_response()
            }),
        );

        match tokio::net::TcpListener::bind(&metrics_listen).await {
            Ok(listener) => {
                tracing::info!(metrics_addr = %metrics_listen, "prometheus metrics endpoint ready");
                if let Err(err) = axum::serve(listener, app).await {
                    tracing::error!(error = %err, metrics_addr = %metrics_listen, "metrics server exited");
                }
            }
            Err(err) => {
                tracing::error!(error = %err, metrics_addr = %metrics_listen, "failed to bind metrics endpoint");
            }
        }
    });
}
