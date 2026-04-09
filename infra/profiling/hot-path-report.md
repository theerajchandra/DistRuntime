# Hot Path Profiling Report

- Date: 2026-04-09
- Scope: `data-loader` shard read path
- Tools:
  - Criterion benchmark: `cargo bench -p data-loader --bench hot_path -- --noplot`
  - Flamegraph capture: `cargo run -p data-loader --example hot_path_flamegraph --release -- <legacy|optimized> <svg_path>`

## Identified Bottlenecks (Top 3)

1. CPU bottleneck: clone-heavy record draining in shard reader path.
   - Before: per-record clone behavior (`legacy_clone_cursor`) dominates the hot loop.
   - After fix: owned iteration (`optimized_owned_iter`) removes clone overhead.
2. CPU/memory bottleneck: extra intermediate buffering in parallel shard fan-in.
   - Before: `ParallelShardReader::open_plugin` materialized an extra `Vec<Record>` per shard.
   - After fix: records are forwarded directly from `ShardReader` to the channel.
3. I/O bottleneck: per-chunk heap allocation in file reads.
   - Before: `FileReader::next_chunk` allocated a fresh `Vec<u8>` for every chunk.
   - After fix: `FileReader` reuses a persistent `BytesMut` scratch buffer.

## Before/After Flamegraphs

- Before: [hot-path-before.svg](/Users/theerajchandra/dev/DistRuntime/infra/profiling/hot-path-before.svg)
- After: [hot-path-after.svg](/Users/theerajchandra/dev/DistRuntime/infra/profiling/hot-path-after.svg)

Observed from flamegraphs:
- `bytes::bytes::shallow_clone_vec` is present in the before profile and absent from the after profile.
- Dominant time shifts to decode/I/O work rather than clone overhead after optimization.

## Benchmark Results

Criterion medians from `target/criterion/*/new/estimates.json`:

| Benchmark | Median (ns) |
|---|---:|
| `shard_reader_record_drain/legacy_clone_cursor` | 1,176,502.04 |
| `shard_reader_record_drain/optimized_owned_iter` | 635,464.57 |
| `file_reader_chunk_overhead/legacy_alloc_per_chunk` | 3,452,573.34 |
| `file_reader_chunk_overhead/optimized_scratch_reuse` | 3,126,331.16 |

Derived improvements:

- Shard-reader drain improvement: **45.98% faster** (`legacy_clone_cursor` -> `optimized_owned_iter`).
- File-reader chunk handling improvement: **9.45% faster** (`legacy_alloc_per_chunk` -> `optimized_scratch_reuse`).

Acceptance check:
- Required benchmark gain (>=10%) on identified bottleneck: **PASS** (45.98% on primary CPU bottleneck).

## CI Regression Guard

- Added Criterion regression benchmark to CI workflow:
  - Job: `Benchmark (Criterion)`
  - Command: `cargo bench -p data-loader --bench hot_path -- --noplot`
