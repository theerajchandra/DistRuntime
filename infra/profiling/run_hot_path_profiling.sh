#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

mkdir -p infra/profiling

HOT_PATH_LINES="${HOT_PATH_LINES:-30000}"
HOT_PATH_ITERS="${HOT_PATH_ITERS:-300}"

echo "Generating hot-path flamegraphs (legacy vs optimized)..."
HOT_PATH_LINES="$HOT_PATH_LINES" HOT_PATH_ITERS="$HOT_PATH_ITERS" \
  cargo run -p data-loader --example hot_path_flamegraph --release -- legacy infra/profiling/hot-path-before.svg
HOT_PATH_LINES="$HOT_PATH_LINES" HOT_PATH_ITERS="$HOT_PATH_ITERS" \
  cargo run -p data-loader --example hot_path_flamegraph --release -- optimized infra/profiling/hot-path-after.svg

echo "Running criterion regression benchmark..."
cargo bench -p data-loader --bench hot_path -- --noplot

echo "Profiling artifacts updated:"
echo "  - infra/profiling/hot-path-before.svg"
echo "  - infra/profiling/hot-path-after.svg"
echo "  - target/criterion/*"
