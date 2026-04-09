# Contributing

## Structured logging spec

DistRuntime uses `tracing` with JSON output for machine-parsable logs.

### Required fields

- `timestamp`: RFC3339 timestamp from `tracing-subscriber`.
- `level`: log level (`TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`).
- `target`: Rust module target.
- `fields.message`: human-readable event message.
- `spans`: active span stack, including RPC span metadata.
- `correlation_id`: request/job correlation identifier.

### Job-scoped correlation IDs

- Workers generate `correlation_id` and send it over gRPC metadata as `x-correlation-id`.
- The coordinator reuses incoming `x-correlation-id` when present.
- If absent, coordinator generates one (`job-<job_id>-...` or `corr-...`).
- Job-level RPC spans include `job_id` and `correlation_id`.

### Example JSON event

```json
{
  "timestamp": "2026-04-08T23:11:57.851416Z",
  "level": "INFO",
  "fields": {
    "message": "checkpoint begun",
    "checkpoint_id": "ckpt-job-a-5-1200",
    "job_id": "job-a",
    "epoch": 5,
    "step": 1200
  },
  "target": "coordinator::service",
  "span": {
    "name": "coordinator.checkpoint_begin",
    "correlation_id": "job-job-a-...",
    "job_id": "job-a"
  }
}
```

## Observability assets

- Prometheus metric names and semantics are documented in [README.md](/Users/theerajchandra/dev/DistRuntime/README.md).
- Grafana dashboard JSON is versioned under `infra/grafana/`.
