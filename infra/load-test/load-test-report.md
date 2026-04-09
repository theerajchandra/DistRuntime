# Load Test Report

- Generated: 2026-04-09 04:12:04 UTC
- Target storage endpoint: `http://127.0.0.1:9000`
- Scale: 16 workers
- Consecutive runs per scenario: 10

## Scenario Results

| Scenario | Status | Validation |
|---|---|---|
| Rolling worker restarts (16 workers, 10 consecutive runs) | PASS | Pass requires zero checkpoint corruption and full shard partition coverage after each rolling restart. |
| Storage throttling under load (16 workers, 10 consecutive runs) | PASS | Pass requires zero checkpoint corruption and >=90% of shards within 5% of theoretical max throughput. |
| Coordinator failover under load (16 workers, 10 consecutive runs) | PASS | Pass requires stale commit rejection after failover plus successful retry checkpoint with zero corruption. |

## Acceptance Criteria

- [x] Zero checkpoint corruption in all 10 runs for every scenario.
- [x] Throughput objective met: >=90% of shards within 5% of theoretical max in storage-throttling scenario.
- [x] Report includes pass/fail per scenario.
