use std::net::SocketAddr;

use axum::http::{header, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use once_cell::sync::Lazy;
use prometheus::{register_gauge, register_int_counter, Encoder, Gauge, IntCounter, TextEncoder};
use tokio::task::JoinHandle;

struct WorkerMetrics {
    shard_read_bytes_per_sec: Gauge,
    shard_read_records_per_sec: Gauge,
    heartbeat_failures_total: IntCounter,
    heartbeats_sent_total: IntCounter,
}

impl WorkerMetrics {
    fn new() -> Self {
        Self {
            shard_read_bytes_per_sec: register_gauge!(
                "distruntime_worker_shard_read_bytes_per_sec",
                "Latest shard read throughput in bytes/s from this worker."
            )
            .expect("register distruntime_worker_shard_read_bytes_per_sec"),
            shard_read_records_per_sec: register_gauge!(
                "distruntime_worker_shard_read_records_per_sec",
                "Latest shard read throughput in records/s from this worker."
            )
            .expect("register distruntime_worker_shard_read_records_per_sec"),
            heartbeat_failures_total: register_int_counter!(
                "distruntime_worker_heartbeat_failures_total",
                "Total failed worker heartbeat RPC attempts."
            )
            .expect("register distruntime_worker_heartbeat_failures_total"),
            heartbeats_sent_total: register_int_counter!(
                "distruntime_worker_heartbeats_sent_total",
                "Total worker heartbeat RPC attempts."
            )
            .expect("register distruntime_worker_heartbeats_sent_total"),
        }
    }
}

static METRICS: Lazy<WorkerMetrics> = Lazy::new(WorkerMetrics::new);

pub fn observe_throughput(bytes_per_sec: f64, records_per_sec: f64) {
    METRICS
        .shard_read_bytes_per_sec
        .set(bytes_per_sec.max(0.0));
    METRICS
        .shard_read_records_per_sec
        .set(records_per_sec.max(0.0));
}

pub fn inc_heartbeat_sent() {
    METRICS.heartbeats_sent_total.inc();
}

pub fn inc_heartbeat_failed() {
    METRICS.heartbeat_failures_total.inc();
}

pub fn render() -> String {
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    encoder
        .encode(&metric_families, &mut buffer)
        .expect("encode prometheus metrics");
    String::from_utf8(buffer).expect("prometheus metrics must be utf8")
}

/// Serve Prometheus metrics on `listen_addr` under `/metrics`.
pub fn spawn_metrics_server(listen_addr: SocketAddr) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/metrics",
            get(|| async move {
                let payload = render();
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

        let listener = tokio::net::TcpListener::bind(listen_addr).await?;
        axum::serve(listener, app).await?;
        Ok(())
    })
}
