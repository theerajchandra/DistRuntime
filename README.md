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
┌─────────────────────────────────────────────┐
│              Python API layer               │
│  register_dataset() · save() · load()       │
└────────────────────┬────────────────────────┘
                     │ gRPC / HTTP2
┌────────────────────▼────────────────────────┐
│         Coordinator service (Rust)          │
│   Shard assignment · heartbeats · election  │
└──────┬─────────────┬──────────────┬─────────┘
       │             │              │
┌──────▼──────┐ ┌────▼─────┐ ┌─────▼──────┐
│  Worker 0   │ │ Worker 1 │ │ Worker N   │
│ Data reader │ │ Ckpt I/O │ │  Recovery  │
└──────┬──────┘ └────┬─────┘ └─────┬──────┘
       │             │              │
┌──────▼─────────────▼──────────────▼───────┐
│              Storage layer                 │
│   Object storage (S3) · etcd · Postgres   │
└────────────────────────────────────────────┘
```

---

## Tech stack

| Layer | Technology |
|---|---|
| Core runtime | Rust (tokio async, tonic gRPC) |
| Python bindings | PyO3 + maturin |
| Coordination | etcd (via openraft) |
| Object storage | S3-compatible (MinIO locally, DO Spaces in prod) |
| Metadata | Postgres |
| Observability | Datadog (metrics, APM, logs) |
| CI | GitHub Actions |

---

## Project structure

```
DistRuntime/
├── coordinator/          # Coordinator gRPC server
├── worker-client/        # Worker-side runtime client
├── checkpoint-engine/    # Two-phase checkpoint protocol
├── data-loader/          # Async sharded data reader
├── proto-gen/            # Protobuf definitions and codegen
├── py-runtime/           # PyO3 Python bindings
├── proto/
│   └── distruntime.proto
├── .github/
│   └── workflows/ci.yml
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
- etcd, MinIO, Postgres — `brew install etcd minio/stable/minio postgresql`

### Build

```bash
# Clone
git clone https://github.com/theerajchandra/DistRuntime
cd DistRuntime

# Build all crates
make build

# Run tests
make test

# Lint
make lint
```

### Local dev services

```bash
# Start etcd
etcd &

# Start MinIO
minio server ~/minio-data &

# Start Postgres
brew services start postgresql
```

Set `STORAGE_ENDPOINT=http://localhost:9000` to point the data-loader at local MinIO.

---

## Python API (in progress)

```python
from distruntime import Runtime

rt = Runtime(coordinator_addr="grpc://localhost:50051", job_id="run-01")

# Register a dataset — coordinator assigns shards across workers
ds = rt.register_dataset("s3://bucket/data/", num_shards=512, format="parquet")

# Iterate batches
for batch in ds.batches(batch_size=256):
    loss = train_step(batch)

# Save a checkpoint
ckpt = rt.register_checkpoint("s3://bucket/checkpoints/", keep_last=5)
ckpt.save(model.state_dict(), step=global_step)

# Resume
state = ckpt.load(version="latest")
model.load_state_dict(state)
```

---

## Development status

| Epic | Status |
|---|---|
| Epic 0 — Infrastructure & environment | In progress |
| Epic 1 — Core Rust runtime | To do |
| Epic 2 — Sharded data loading | To do |
| Epic 3 — Fault-tolerant checkpointing | To do |
| Epic 4 — Python API and bindings | To do |
| Epic 5 — Observability and load testing | To do |

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

## License

MIT
