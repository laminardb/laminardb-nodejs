# Benchmarks

tinybench means from `pnpm run bench` (plan 03 Task 2.6); nightly CI archives
`bench-output.txt` as an artifact. Rerun on target hardware before trusting a number for a
workload decision.

## 2026-09-03 — dev laptop (macOS arm64, Apple M-series, Node 24, debug addon)

| Benchmark                               |  ops/s |    mean |     p99 |
| --------------------------------------- | -----: | ------: | ------: |
| open + close (in-memory)                |  3,402 | 0.32 ms | 0.52 ms |
| insert 1k rows (row objects)            |  1,337 | 0.75 ms | 0.94 ms |
| insert 1k rows (Arrow IPC)              | 52,897 | 0.02 ms | 0.04 ms |
| query 10k rows → toArray()              |    131 | 7.78 ms | 8.80 ms |
| query 10k rows → toIPC()                |    418 | 2.40 ms | 2.87 ms |
| apache-arrow tableFromIPC (1k rows)     |    380 | 2.65 ms | 3.13 ms |
| subscription: insert → first data frame |  4,416 | 0.25 ms | 0.60 ms |

Read as: row-object ingestion ~1.3 M rows/s, IPC ingestion ~50 M rows/s (the parse path is
not the constraint), collected 10k-row results ~7.8 ms as row objects / ~2.4 ms as IPC
bytes, and end-to-end subscription latency (insert to delivered frame) ~0.25 ms.

## Zero-copy Arrow decision (plan 03 Task 2.6)

**Declined for Phase 2.** The C Data Interface fast path (arrow `ffi` export +
`arrow-js-ffi` `copy=false` with a Rust-side owner) exists to remove the one IPC
serialization copy. At the measured baseline, serializing a 10k-row result costs ~2.4 ms
while row conversion costs ~7.8 ms and the engine's own processing dominates end-to-end
latency — the copy is not the bottleneck for any current workload, and the zero-copy path
brings lifetime coupling (JS views valid only while the Rust owner lives) plus a
Decimal-gap in the JS FFI reader. Revisit when a real workload shows IPC serialization as
the hot spot; the comparison baseline is the table above.
