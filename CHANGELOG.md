# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the version scheme tracks the
pinned core (plan 00 D4, `CORE_PIN.md`).

## [Unreleased]

## [0.30.0-alpha.2] — 2026-09-06

### Changed

- Package entry points: an `exports` map with a real ESM entry (`index.mjs`) alongside the
  CJS build — named imports work on every Node version; `require()` unchanged. A
  surface-parity test pins the two together.
- README rewritten consumer-first: install, first pipeline, and the no-compilation story
  up top; engineering material moved to a development section.
- This is the first release published through the tokenless trusted-OIDC pipeline.

## [0.30.0-alpha.1] — 2026-09-06

First packaged release of the Node.js binding, pinning core `v0.30.0`. Embedded engine
only; the `alpha` dist-tag carries it until 1.0.

### Added — Phase 2: subscriptions, hardening, platform breadth

- Framed subscriptions over `SubscriptionPortal`: `subscribe(name, {filter, fromEpoch})`
  with per-frame `nextFrame()`, terminal failures as 502 (lag) / 500, idempotent
  `cancel()` that wakes pending reads, and `Symbol.asyncIterator` in the TypeScript layer.
- Push subscriptions: `subscribeWith(name, {onData, onError, onClose})` on weak threadsafe
  functions with awaited per-frame delivery (backpressure, not queueing); `close()`
  resolves after the reader stops — no callbacks after it.
- Streaming queries: `streamQuery(sql)` async-iterable over batches, with
  `cancelQuery(id)`; the reader owns the `QueryHandle` (dropping it early truncates
  results — core semantics, recorded in plan 03).
- Telemetry: `metrics`, `sourceMetrics`/`allSourceMetrics`,
  `streamMetrics`/`allStreamMetrics`, `pipelineState`, `pipelineWatermark`,
  `totalEventsProcessed`.
- The TypeScript layer converts `Date` instances in rows to epoch milliseconds
  automatically.
- CI breadth: Windows in the verify matrix and a musl cross-check; ESLint
  (typescript-eslint, TS 6) in the review gate; nightly subscription soak and benchmark
  artifact; benchmark baseline recorded in `docs/benchmarks.md` with the zero-copy Arrow
  decision (declined at this baseline).

### Added — Phase 1: embedded MVP

- Configuration-based open: `LaminarDB.open(path?, config?)` with `:memory:` sugar,
  `storageDir`, `checkpoint` options (manual + interval), buffer size, incremental emit,
  object-store settings.
- Query results: `Connection.query()`/`execute()` with fully collected results —
  `schema()`, `numRows()`, `batch(i)`, `toIPC()` (Arrow IPC stream `Buffer`), `toArray()`
  (row objects; temporal columns as epoch milliseconds, 64-bit ints as `BigInt`).
- Ingestion: `insert(source, rows)` with strict per-value validation,
  `insertArrow(source, ipcBuffer)`, and the `Writer` streaming class with watermark and
  backpressure visibility.
- Lifecycle and catalog: `start()`, `checkpoint()` (outcome with id/epoch),
  `isCheckpointEnabled()`, `listSources/Streams/Sinks()`, `sourceInfos()`, `schema(name)`.
- TypeScript public layer (`dist/`): `LaminarDB`/`Connection`/`Writer` facades, the
  `LaminarError` hierarchy with `error.code` parsed from the native `[LAMINAR_<n>]`
  contract, and `tableFrom()` interop with the optional `apache-arrow` peer dependency.
- Documentation-as-tests (README quickstart verbatim), a cold-consumer `bare-quickstart`
  proof against `npm pack`, and the phase-1 plan record.

### Added — Phase 0

- Phase 0 scaffold: napi-rs v3 cdylib binding the pinned core (`v0.30.0`, `api` feature)
  with async-first surface driven on the napi tokio runtime.
- Native surface: `open()`, `Connection.execute()`, `Connection.isClosed()`,
  `Connection.close()`, `version()`.
- Error contract: core `DbError` mapped through the `api::ApiError` taxonomy to JS errors
  whose message carries a `[LAMINAR_<n>]` code prefix (typed classes exposing `error.code`
  arrive with the Phase 1 TypeScript layer).
- vitest smoke suite, `just` orchestration (`build`/`test`/`verify`/`review`), CI on Linux
  and macOS, decision records and phase plans under `docs/plans/`.
