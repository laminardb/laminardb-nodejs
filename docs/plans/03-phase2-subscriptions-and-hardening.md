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

- [ ] `Connection.subscribe(name, opts?) -> Subscription` (native class over
      `SubscriptionPortal`): `schema()`, `nextFrame() -> Promise<Frame>` (resolves
      `{type:'data', batch: ArrowBatch, sequence}` /
      `{type:'barrier', sequence, epoch, checkpointId, throughSequence}` / `null` on
      close), `isActive()`, `cancel()` (idempotent; wakes a pending `nextFrame`),
      `close()`.
- [ ] Terminal frames end the stream: `Lagged` rejects the pending/next call with
      `LAMINAR_502` (subscription fell behind N entries), `Error` with `LAMINAR_500`.
- [ ] TS layer: `Subscription[Symbol.asyncIterator]` —
      `for await (const frame     of conn.subscribe(...))` with `return()` mapped to
      `cancel()`; the frame union typed. `subscribeFromEpoch(name, epoch)` for `AsOfEpoch`
      replay.
- [ ] Backpressure is pull-based: no frame is converted until the consumer asks. Document
      the portal's retained-entry bound and the `Lagged` consequence for slow consumers.

## Task 2.2 — Push callbacks (convenience style)

- [ ] `Connection.subscribeWith(name, { onData, onError, onClose }) ->     PushSubscription`:
      a tokio task loops `next_frame()` and calls a **weak** ThreadsafeFunction per frame
      (bounded queue; `QueueFull` applies backpressure by awaiting the call).
- [ ] Errors/lag surface via `onError(code, message)` then `onClose()`; `close()` cancels
      the task and joins it — no callbacks after return.
- [ ] Weak references keep the subscription from pinning the event loop when the consumer
      is gone (test: unreferenced subscription does not keep a process alive).

## Task 2.3 — Streaming queries and cancel

- [ ] `Connection.streamQuery(sql) -> QueryStream`: `execute` → keep the `QueryHandle` (do
      **not** collect) → `next()` over `subscribe_raw()` frames; `cancel()` calls
      `handle.cancel()`; `Connection.cancelQuery(id)` for the handle-id path.
- [ ] TS: `QueryStream[Symbol.asyncIterator]` yielding `ArrowBatch`es.

## Task 2.4 — Metrics/catalog parity

- [ ] Expose the api layer's introspection: `pipelineState()`, `pipelineTopology()`,
      `pipelineWatermark()`, `metrics()`, `sourceMetrics(name?)`, `streamMetrics(name?)`,
      `totalEventsProcessed()` (source: `api::Connection` delegates to core methods — bind
      the core methods directly per D2; verify each exists at the pin before wiring).
- [ ] Parity check against the Python binding's public surface; record gaps.

## Task 2.5 — Platform breadth and gates

- [ ] CI adds `windows-latest` (verify job) and a musl build check
      (`x86_64-unknown-linux-musl` via the napi cross toolchain); release native builds
      arrive with plan 04.
- [ ] ESLint (flat config, typescript-eslint) joins `just review` for `ts/`.
- [ ] Date-object convenience: the TS layer converts `Date` instances to ms in
      `insert`/`writeRows` row objects (the native seam stays milliseconds-only, plan 02
      spike).

## Task 2.6 — Soak and benchmarks

- [ ] Nightly CI: subscription soak (continuous insert → subscribe → consume for N
      minutes; assert no lag, no growth in `pending`), plus the existing open/close loop.
- [ ] `benchmarks/` (tinybench): open/close, insert throughput (rows and IPC), query →
      toArray vs toIPC + tableFromIPC, subscription frame latency. Numbers recorded in
      `docs/benchmarks.md` with hardware notes.
- [ ] Zero-copy Arrow spike: C Data Interface export + `arrow-js-ffi` `copy=false` vs IPC
      on the query path; ships only if it beats IPC by a meaningful margin with a
      lifetime-safe API (Rust owner held by the class instance), else recorded as
      declined.

## Task 2.7 — Review and exit

- [ ] All gates green; failing-path coverage for 502/500/cancel/close races.
- [ ] Phase-exit review in `docs/reviews/phase2-<date>.md`, zero open REQUEST CHANGES
      findings.

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
