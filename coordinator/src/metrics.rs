use once_cell::sync::Lazy;
use prometheus::{
    register_gauge, register_gauge_vec, register_histogram, register_int_counter, Encoder, Gauge,
    GaugeVec, Histogram, IntCounter, TextEncoder,
};

struct CoordinatorMetrics {
    shard_read_bytes_per_sec: GaugeVec,
    shard_read_records_per_sec: GaugeVec,
    checkpoint_write_latency_seconds: Histogram,
    worker_count: Gauge,
    heartbeat_misses_total: IntCounter,
    heartbeats_total: IntCounter,
    heartbeat_miss_rate: Gauge,
}

impl CoordinatorMetrics {
    fn new() -> Self {
        Self {
            shard_read_bytes_per_sec: register_gauge_vec!(
                "distruntime_shard_read_bytes_per_sec",
                "Latest shard read throughput in bytes/s reported by each worker.",
                &["worker_id"]
            )
            .expect("register distruntime_shard_read_bytes_per_sec"),
            shard_read_records_per_sec: register_gauge_vec!(
                "distruntime_shard_read_records_per_sec",
                "Latest shard read throughput in records/s reported by each worker.",
                &["worker_id"]
            )
            .expect("register distruntime_shard_read_records_per_sec"),
            checkpoint_write_latency_seconds: register_histogram!(
                "distruntime_checkpoint_write_latency_seconds",
                "Latency from checkpoint begin to fully committed.",
                vec![0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0]
            )
            .expect("register distruntime_checkpoint_write_latency_seconds"),
            worker_count: register_gauge!(
                "distruntime_worker_count",
                "Number of workers currently marked alive by the coordinator."
            )
            .expect("register distruntime_worker_count"),
            heartbeat_misses_total: register_int_counter!(
                "distruntime_heartbeat_misses_total",
                "Total number of workers marked dead due to heartbeat misses."
            )
            .expect("register distruntime_heartbeat_misses_total"),
            heartbeats_total: register_int_counter!(
                "distruntime_heartbeats_total",
                "Total number of successful worker heartbeat updates."
            )
            .expect("register distruntime_heartbeats_total"),
            heartbeat_miss_rate: register_gauge!(
                "distruntime_heartbeat_miss_rate",
                "Missed heartbeat rate = misses / (heartbeats + misses)."
            )
            .expect("register distruntime_heartbeat_miss_rate"),
        }
    }

    fn refresh_heartbeat_miss_rate(&self) {
        let misses = self.heartbeat_misses_total.get() as f64;
        let heartbeats = self.heartbeats_total.get() as f64;
        let denom = misses + heartbeats;
        let rate = if denom > 0.0 { misses / denom } else { 0.0 };
        self.heartbeat_miss_rate.set(rate);
    }
}

static METRICS: Lazy<CoordinatorMetrics> = Lazy::new(CoordinatorMetrics::new);

pub fn observe_worker_throughput(
    worker_id: &str,
    bytes_per_sec: f64,
    records_per_sec: f64,
) {
    METRICS
        .shard_read_bytes_per_sec
        .with_label_values(&[worker_id])
        .set(bytes_per_sec.max(0.0));
    METRICS
        .shard_read_records_per_sec
        .with_label_values(&[worker_id])
        .set(records_per_sec.max(0.0));
}

pub fn observe_checkpoint_write_latency(seconds: f64) {
    METRICS
        .checkpoint_write_latency_seconds
        .observe(seconds.max(0.0));
}

pub fn set_worker_count(alive_workers: u32) {
    METRICS.worker_count.set(alive_workers as f64);
}

pub fn inc_heartbeat() {
    METRICS.heartbeats_total.inc();
    METRICS.refresh_heartbeat_miss_rate();
}

pub fn inc_heartbeat_misses(count: u64) {
    METRICS.heartbeat_misses_total.inc_by(count);
    METRICS.refresh_heartbeat_miss_rate();
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
