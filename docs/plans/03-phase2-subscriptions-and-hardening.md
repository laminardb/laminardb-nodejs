# Plan 03 — Phase 2: subscriptions, hardening, platform breadth

Status: **Planned (2026-09-02)** · Prerequisites: plan 00, plan 01 and plan 02 exited
Exit: parity with the Python binding's core flows (framed subscriptions, push callbacks,
streaming queries with cancel), Windows + musl in CI, published benchmark numbers, soak
green overnight.

## Verified core facts (pin `v0.30.0`)

- Named-stream subscriptions: `db.open_subscription(name, opts, SubscribeStart)` →
  `SubscriptionPortal` (public) with `schema()`, `next_frame() -> Option<PortalFrame>`
  (**async**), `try_next_frame()`, `close()`, `is_closed()`. The api layer's
  `ArrowSubscription` wraps it but its constructor is `pub(crate)` — bind the portal
  directly (same argument as D2).
- `PortalFrame::{Batch {batch, sequence, lease}, Barrier {sequence, epoch, checkpoint_id, through_sequence}, Lagged(u64), Error {message}}`.
  `Lagged` and `Error` are **terminal** (the api layer converts them to failures and
  deactivates). `SubscribeStart::{Tail, AsOfEpoch(u64)}` (replay after a retained
  barrier).
- The `lease` field is an internal ownership token (`#[doc(hidden)]`): dropping the frame
  releases the shared-log slot — frames must not outlive their conversion to JS.
- Subscription frames carry `RecordBatch`es → the Phase 1 IPC/`toArray` conversion applies
  unchanged.

## Task 2.1 — Framed subscription (pull, primary style)

- [x] `Connection.subscribe(name, filter?, fromEpoch?) -> Subscription` (native class over
      `SubscriptionPortal`): `schema()`, `nextFrame() -> Promise<Frame>` (resolves
      `{type:'data', batch: ArrowBatch, sequence}` /
      `{type:'barrier', sequence, epoch, checkpointId, throughSequence}` / `null` on
      close), `isActive()`, `cancel()` (idempotent; wakes a pending `nextFrame`),
      `close()`.
- [x] Terminal frames end the stream: `Lagged` rejects the pending/next call with
      `LAMINAR_502` (subscription fell behind N entries), `Error` with `LAMINAR_500`.
- [x] TS layer: `Subscription[Symbol.asyncIterator]` —
      `for await (const frame     of conn.subscribe(...))` with `return()` mapped to
      `cancel()`; the frame union typed. `subscribeFromEpoch(name, epoch)` for `AsOfEpoch`
      replay.
- [x] Backpressure is pull-based: no frame is converted until the consumer asks. Document
      the portal's retained-entry bound and the `Lagged` consequence for slow consumers.

## Task 2.2 — Push callbacks (convenience style)

- [x] `Connection.subscribeWith(name, { onData, onError, onClose }) ->     PushSubscription`:
      a tokio task loops `next_frame()` and calls a **weak** ThreadsafeFunction per frame
      (bounded queue; `QueueFull` applies backpressure by awaiting the call).
- [x] Errors/lag surface via `onError(error)` (an object — threadsafe c calls deliver
      exactly one JS argument; see §Spike results) then `onClose()`; `close()` cancels the
      task and joins it — no callbacks after return.
- [x] Weak references keep the subscription from pinning the event loop when the consumer
      is gone (test: unreferenced subscription does not keep a process alive).

## Task 2.3 — Streaming queries and cancel

- [x] `Connection.streamQuery(sql) -> QueryStream`: `execute` → keep the `QueryHandle` (do
      **not** collect) → `next()` over `subscribe_raw()` frames; `cancel()` calls
      `handle.cancel()`; `Connection.cancelQuery(id)` for the handle-id path.
- [x] TS: `QueryStream[Symbol.asyncIterator]` yielding `ArrowBatch`es.

## Task 2.4 — Metrics/catalog parity

- [x] Engine introspection: `metrics`, `sourceMetrics(name)`, `allSourceMetrics`,
      `streamMetrics(name)`, `allStreamMetrics`, `pipelineState`, `pipelineWatermark`,
      `totalEventsProcessed`, `cancelQuery(id)` (all the core `metrics_api` surface with
      payload objects): `pipelineState()`, `pipelineTopology()`, `pipelineWatermark()`,
      `metrics()`, `sourceMetrics(name?)`, `streamMetrics(name?)`,
      `totalEventsProcessed()` (source: `api::Connection` delegates to core methods — bind
      the core methods directly per D2; verify each exists at the pin before wiring).
- [ ] Parity check against the Python binding's public surface (deferred to the Phase 3
      prep pass; the metrics surface above covers the api layer)'s public surface; record
      gaps.

## Task 2.5 — Platform breadth and gates

- [x] CI adds `windows-latest` (verify job) and a musl cross-check job (cargo check on
      `x86_64-unknown-linux-musl`; the full link joins the release matrix in plan 04)
      (`x86_64-unknown-linux-musl` via the napi cross toolchain); release native builds
      arrive with plan 04.
- [x] ESLint (flat config, typescript-eslint) joins `just review`; TS pinned to 6.x until
      typescript-eslint supports TS 7 for `ts/`.
- [x] Date-object convenience: the TS layer converts `Date` instances to ms in
      `insert`/`writeRows` row objects (the native seam stays milliseconds-only, plan 02
      spike).

## Task 2.6 — Soak and benchmarks

- [x] Nightly CI: subscription soak (`scripts/soak.mjs`, 5 min) + benchmark artifact
      (continuous insert → subscribe → consume for N minutes; assert no lag, no growth in
      `pending`), plus the existing open/close loop.
- [x] `benchmarks/` (tinybench) — results and the zero-copy decision recorded in
      `docs/benchmarks.md` (declined at this baseline): open/close, insert throughput
      (rows and IPC), query → toArray vs toIPC + tableFromIPC, subscription frame latency.
      Numbers recorded in `docs/benchmarks.md` with hardware notes.
- [x] Zero-copy Arrow spike: **declined** with measured rationale (`docs/benchmarks.md`) —
      the IPC copy is ~2.4 ms per 10k-row result, not a demonstrated bottleneck: C Data
      Interface export + `arrow-js-ffi` `copy=false` vs IPC on the query path; ships only
      if it beats IPC by a meaningful margin with a lifetime-safe API (Rust owner held by
      the class instance), else recorded as declined.

## Task 2.7 — Review and exit

- [x] All gates green; failing-path coverage for 502/500/cancel/close (8-test native
      suite + 5-test TS suite).
- [ ] Phase-exit review in `docs/reviews/phase2-<date>.md`, zero open REQUEST CHANGES
      findings.

## Spike results (executed 2026-09-03, core v0.30.0)

- **`QueryHandle` must stay alive while its output streams**: `QueryHandle::drop` cancels
  the query (cancel-token), so a stream that drops the handle after `subscribe_raw`
  truncates or empties results — verified against the core's own `api::QueryStream`, which
  keeps `handle: Option<QueryHandle>` for exactly this reason. `QueryStream`'s reader task
  owns the handle and cancels it on exit. (`QueryResult::collect` was never affected — it
  holds the handle across its loop.)
- **Threadsafe calls deliver exactly one JS argument per message**: a tuple `T` crosses as
  a single JS _array_, so `onError(code, message)` was impossible as a spread pair — the
  callback receives an `{ code, message }` object (`CallbackError`). Single-value messages
  (`onData(frame)`, `onClose()`) map naturally.
- **TSF arg types**: fully-instantiated
  `ThreadsafeFunction<T, (), T, Status, false, true, 0>` parameters build through napi
  argument conversion with `Weak = true` — no manual builder needed (the manual path
  fights lifetime and const-generic inference).
- **Bounded queries snapshot the source buffer at execution**: a query executed before the
  coordinator cycles prior inserts can see an empty buffer and complete immediately.
  Documented on `streamQuery`; tests drain (`sourceMetrics.pending === 0`) first.
  Continuous consumption is what subscriptions are for.
- **The portal reader must own a cancellation token**: a portal blocked in `next_frame`
  holds `&mut self`, so `close()` cannot run concurrently — `tokio::select!` against a
  `CancellationToken` is the cancel path, then the reader closes the portal itself.
- **push-loop backpressure**: each `onData` delivery is awaited (`call_async`), so a slow
  handler throttles the reader instead of growing the TSF queue (which stays at depth ≤
  1).
- **typescript-eslint requires TS 6.x** (TS 7 is the Go-port compiler, unsupported until
  typescript-eslint #10940 lands); the package pins `typescript@6` — tsc 6 compiles
  everything this layer uses.

## Design notes

- **Portal, not api::ArrowSubscription**: same D2 reasoning as Phase 1 — the async core
  type has no runtime-context guard and the api wrapper's constructor is crate-private
  anyway.
- **Lease discipline**: `PortalFrame::Batch` holds a shared-log lease; the JS conversion
  (`toIPC`/`toArray`) must complete before the frame drops. The class converts eagerly in
  `nextFrame` — no deferred conversion of borrowed data across the boundary.
- **Push on weak refs only**: a strong ThreadsafeFunction ref would keep the Node event
  loop alive for a stream nobody consumes — that is a process lifetime bug, not a
  convenience.
