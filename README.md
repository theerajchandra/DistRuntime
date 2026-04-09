# DistRuntime

A high-performance distributed runtime for coordinating data loading, checkpointing, and state persistence across large-scale ML training jobs.

Built with a Rust core for all I/O-critical paths and a Python surface for ML engineer ergonomics.

---

## What it does

Training a large model across hundreds of GPU workers introduces problems that naive implementations ignore: thundering-herd I/O on data reads, partial checkpoint corruption on node failure, no coordinated recovery when a worker dies mid-epoch. DistRuntime solves all of this.

- **Sharded data loading** — coordinator assigns non-overlapping data shards to each worker, no duplicates, no gaps
- **Fault-tolerant checkpointing** — two-phase commit protocol ensures checkpoints are either fully written or cleanly aborted, never partial
- **Worker coordination** — heartbeat-based liveness detection with automatic shard rebalancing on worker failure
- **Recovery** — jobs resume from the last committed checkpoint, even across worker count changes

---

## Architecture

```
┌──────────────────────────────────────────────┐
│            Python API + CLI surface          │
│ Runtime · Dataset · Checkpoint · distruntime │
└────────────────────┬─────────────────────────┘
                     │ gRPC / HTTP2
┌────────────────────▼─────────────────────────┐
│         Coordinator service (Rust)           │
│ Worker registration · heartbeats · shard map │
│ checkpoint RPCs · recovery · Prometheus      │
└───────────────┬────────────────────┬─────────┘
                │                    │
┌───────────────▼──────────────┐ ┌───▼──────────────────┐
│ Worker process / Python app  │ │ CLI + observability  │
│ worker-client + data-loader  │ │ job status · ckpt ls │
│ dataset iteration · ckpt I/O │ │ restore info         │
└───────────────┬──────────────┘ └──────────────────────┘
                │
┌───────────────▼────────────────────────────────────────┐
│                Storage and local state                 │
│ local shard files · S3-compatible object storage       │
│ optional JSON-backed dataset registry persistence      │
└────────────────────────────────────────────────────────┘
```

The current implementation is a single Rust coordinator plus worker-side clients. There is no `etcd`, `openraft`, or Postgres metadata/control plane in the repo today.

---

## Tech stack

| Layer | Technology |
|---|---|
| Core runtime | Rust 2021, Tokio, Tonic gRPC, Prost |
| Coordinator CLI | `clap` + `axum` |
| Python bindings | PyO3 + maturin |
| Data layer | Apache Arrow, Parquet, bytes, Tokio async file I/O |
| Storage backends | Local filesystem and S3-compatible object storage via the AWS SDK for Rust |
| State persistence | In-memory coordinator state with optional JSON-backed dataset registry persistence and Rust checkpoint registries |
| Observability | `tracing` / `tracing-subscriber` JSON logs, Prometheus, Grafana |
| Profiling and benchmarks | Criterion, `pprof`, flamegraphs |
| CI | GitHub Actions + maturin wheel builds |

---

## Project structure

```
DistRuntime/
├── coordinator/          # Coordinator service, shard mapping, liveness, metrics
├── worker-client/        # Worker registration, heartbeats, shard watches
├── checkpoint-engine/    # Two-phase checkpoint engine and checkpoint registry
├── data-loader/          # File/S3 readers, format plugins, benches, tests
├── distruntime-cli/      # Coordinator runner and admin CLI
├── py-runtime/           # Python package, type hints, maturin config
├── proto/                # Protobuf schema
├── proto-gen/            # Generated Rust protobuf / gRPC bindings
├── infra/grafana/        # Grafana dashboard
├── infra/load-test/      # Load / chaos test runner and reports
├── infra/profiling/      # Profiling runner, flamegraphs, benchmark report
├── .github/workflows/    # CI, wheel builds, regression benchmarks
├── Makefile
├── Cargo.toml
├── clippy.toml
└── rustfmt.toml
```

---

## Getting started

### Prerequisites

- Rust stable toolchain
- Python 3.10+
- `protoc` (Protocol Buffers compiler) — `brew install protobuf`
- `maturin` for the Python extension — `python -m pip install maturin`
- Optional S3-compatible local testing: MinIO — `brew install minio/stable/minio`

### Build and install

```bash
git clone https://github.com/theerajchandra/DistRuntime
cd DistRuntime

# Build and verify the Rust workspace
cargo build --workspace
make check

# Install the Python package into the current environment
maturin develop -m py-runtime/Cargo.toml
```

### Run the coordinator

```bash
cargo run -p distruntime-cli -- start-coordinator \
  --listen 127.0.0.1:8787 \
  --metrics-listen 127.0.0.1:9090 \
  --registry-path ./registry.json

export DISTRUNTIME_COORDINATOR=http://127.0.0.1:8787
```

### Optional local S3-compatible storage

```bash
minio server ~/minio-data &
export STORAGE_ENDPOINT=http://127.0.0.1:9000
```

### Example shard layout

The high-level Python `Dataset` API expects shards laid out under a local directory as:

```text
sample-data/
  shard-0.jsonl
  shard-1.jsonl
  shard-2.jsonl
```

---

## Python API

```python
from distruntime import Runtime

rt = Runtime("http://127.0.0.1:8787", "run-01")

# Register a dataset rooted at a local shard directory.
# Files are expected to look like ./sample-data/shard-0.jsonl, shard-1.jsonl, ...
ds = rt.register_dataset("./sample-data", num_shards=3, format="jsonl")

# Iterate individual records
for record in ds:
    train_step(record)

# Or iterate batches
for batch in ds.batches(batch_size=32):
    train_batch(batch)

# Local async checkpoint save/load with retention
ckpt = rt.register_checkpoint("./checkpoints", keep_last=5)
ckpt.save({"step": 100, "model": model_state}, step=100)
ckpt.wait()
state = ckpt.load()

# Coordinator-side recovery metadata, if available for this job
recovery = rt.recover()

rt.shutdown()
```

The Python package also exposes `ShardIterator`, `CheckpointManager`, `DatasetIterator`, and `BatchIterator`.

---

## Development status

| Epic | Status |
|---|---|
| Epic 0 — Infrastructure & environment | Done |
| Epic 1 — Core Rust runtime | Done |
| Epic 2 — Sharded data loading | Done |
| Epic 3 — Fault-tolerant checkpointing | Done |
| Epic 4 — Python API and bindings | Done |
| Epic 5 — Observability and load testing | Done |

Tracked on [Jira](https://theeraj.atlassian.net/browse/DIST-1).

---

## Make targets

| Command | Description |
|---|---|
| `make build` | Build all workspace crates |
| `make test` | Run all tests |
| `make lint` | Run clippy with `-D warnings` |
| `make fmt` | Auto-format all code |
| `make check` | Format check + clippy + tests (CI equivalent) |
| `make proto` | Rebuild protobuf codegen |
| `make clean` | Remove build artifacts |

---

## Profiling and Benchmarking

Hot-path profiling and regression benchmarking for `data-loader`:

- Run profiling + flamegraph artifacts:
  - `infra/profiling/run_hot_path_profiling.sh`
- Criterion benchmark (same command used in CI):
  - `cargo bench -p data-loader --bench hot_path -- --noplot`
- Artifacts:
  - `infra/profiling/hot-path-before.svg`
  - `infra/profiling/hot-path-after.svg`
  - `infra/profiling/hot-path-report.md`

---

## Observability metrics

Prometheus metrics are exposed from the coordinator at `/metrics` (default bind `127.0.0.1:9090`).
Worker-side metrics are available through `worker_client::spawn_metrics_server(...)`.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `distruntime_shard_read_bytes_per_sec` | gauge | `worker_id` | Latest shard read throughput in bytes/s reported by each worker heartbeat. |
| `distruntime_shard_read_records_per_sec` | gauge | `worker_id` | Latest shard read throughput in records/s reported by each worker heartbeat. |
| `distruntime_checkpoint_write_latency_seconds` | histogram | none | End-to-end checkpoint write latency from begin to fully committed. |
| `distruntime_worker_count` | gauge | none | Alive worker count tracked by coordinator liveness state. |
| `distruntime_heartbeat_miss_rate` | gauge | none | Heartbeat miss ratio, computed as misses / (heartbeats + misses). |
| `distruntime_heartbeat_misses_total` | counter | none | Total workers marked failed for missed heartbeat deadlines. |
| `distruntime_heartbeats_total` | counter | none | Total successful coordinator heartbeat updates. |
| `distruntime_worker_shard_read_bytes_per_sec` | gauge | none | Latest local worker shard read throughput in bytes/s. |
| `distruntime_worker_shard_read_records_per_sec` | gauge | none | Latest local worker shard read throughput in records/s. |
| `distruntime_worker_heartbeats_sent_total` | counter | none | Total worker heartbeat RPC attempts. |
| `distruntime_worker_heartbeat_failures_total` | counter | none | Total worker heartbeat RPC failures. |

For percentile queries, use `histogram_quantile` over `distruntime_checkpoint_write_latency_seconds_bucket`:
- p50: `histogram_quantile(0.50, sum(rate(distruntime_checkpoint_write_latency_seconds_bucket[5m])) by (le))`
- p95: `histogram_quantile(0.95, sum(rate(distruntime_checkpoint_write_latency_seconds_bucket[5m])) by (le))`
- p99: `histogram_quantile(0.99, sum(rate(distruntime_checkpoint_write_latency_seconds_bucket[5m])) by (le))`

---

## License

MIT
