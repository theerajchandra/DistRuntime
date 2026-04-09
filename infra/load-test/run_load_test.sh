#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

REPORT_PATH="${1:-$ROOT_DIR/infra/load-test/load-test-report.md}"
STORAGE_ENDPOINT="${STORAGE_ENDPOINT:-http://127.0.0.1:9000}"
GENERATED_AT="$(date -u +"%Y-%m-%d %H:%M:%S UTC")"

SCENARIO_TESTS=(
  "rolling_worker_restarts_load_chaos"
  "storage_throttling_load_chaos"
  "coordinator_failover_under_load_chaos"
)

SCENARIO_LABELS=(
  "Rolling worker restarts (16 workers, 10 consecutive runs)"
  "Storage throttling under load (16 workers, 10 consecutive runs)"
  "Coordinator failover under load (16 workers, 10 consecutive runs)"
)

SCENARIO_NOTES=(
  "Pass requires zero checkpoint corruption and full shard partition coverage after each rolling restart."
  "Pass requires zero checkpoint corruption and >=90% of shards within 5% of theoretical max throughput."
  "Pass requires stale commit rejection after failover plus successful retry checkpoint with zero corruption."
)

RESULTS=()
ALL_PASS=1

echo "Running load scenarios with STORAGE_ENDPOINT=$STORAGE_ENDPOINT"
for i in "${!SCENARIO_TESTS[@]}"; do
  test_name="${SCENARIO_TESTS[$i]}"
  label="${SCENARIO_LABELS[$i]}"
  echo ""
  echo "==> $label"

  if STORAGE_ENDPOINT="$STORAGE_ENDPOINT" cargo test -p coordinator --test load_chaos "$test_name" -- --ignored --nocapture --test-threads=1; then
    RESULTS+=("PASS")
  else
    RESULTS+=("FAIL")
    ALL_PASS=0
  fi
done

{
  echo "# Load Test Report"
  echo ""
  echo "- Generated: $GENERATED_AT"
  echo "- Target storage endpoint: \`$STORAGE_ENDPOINT\`"
  echo "- Scale: 16 workers"
  echo "- Consecutive runs per scenario: 10"
  echo ""
  echo "## Scenario Results"
  echo ""
  echo "| Scenario | Status | Validation |"
  echo "|---|---|---|"
  for i in "${!SCENARIO_LABELS[@]}"; do
    echo "| ${SCENARIO_LABELS[$i]} | ${RESULTS[$i]} | ${SCENARIO_NOTES[$i]} |"
  done
  echo ""
  echo "## Acceptance Criteria"
  echo ""
  if [[ "$ALL_PASS" -eq 1 ]]; then
    echo "- [x] Zero checkpoint corruption in all 10 runs for every scenario."
    echo "- [x] Throughput objective met: >=90% of shards within 5% of theoretical max in storage-throttling scenario."
    echo "- [x] Report includes pass/fail per scenario."
  else
    echo "- [ ] Zero checkpoint corruption in all 10 runs for every scenario."
    echo "- [ ] Throughput objective met: >=90% of shards within 5% of theoretical max in storage-throttling scenario."
    echo "- [x] Report includes pass/fail per scenario."
  fi
} > "$REPORT_PATH"

echo ""
echo "Wrote report to $REPORT_PATH"

if [[ "$ALL_PASS" -eq 1 ]]; then
  echo "Load test suite: PASS"
  exit 0
else
  echo "Load test suite: FAIL"
  exit 1
fi
