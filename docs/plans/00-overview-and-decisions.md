# Plan 00 — Overview and decision records

Status: **Accepted direction; Phase 0 executing** · Date: 2026-09-02 Applies to:
`laminardb-nodejs` (npm `@laminardb/node`)

## 1. Mission

Give Node.js and TypeScript developers the `pip install laminardb` experience in npm
terms: **one dependency, one `await` to open, streaming SQL in-process**.

```ts
import { LaminarDB } from '@laminardb/node'

const conn = await LaminarDB.open()
await conn.execute('CREATE SOURCE sensors (ts TIMESTAMP, device VARCHAR, value DOUBLE)')
await conn.start()
const writer = conn.writer('sensors')
await writer.writeRows(rows)
for await (const frame of conn.subscribe('sensor_rollup')) {
  if (frame.type === 'data') consume(frame.batch)
}
await conn.close()
```

Target user: server-side Node/TypeScript engineers (services, stream processing, edge
analytics backends) on maintained Node LTS lines. Phase 1 ships **embedded only**
(in-process), with the public API shaped so a future server client-driver is another
implementation, never a rewrite.

## 2. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  User code (Node 20+, JS or TS)                              │
├──────────────────────────────────────────────────────────────┤
│  @laminardb/node — public API (hand-written TypeScript, ts/) │
│  LaminarDB · Connection · QueryResult · Writer · Subscription│
│  error classes · async iterators · arrow interop helpers     │
├──────────────── generated seam (index.js / index.d.ts) ──────┤
│  native surface: open · Connection class · version           │
├──────────────────────────────────────────────────────────────┤
│  laminar-nodejs (Rust cdylib, napi-rs v3)                    │
│  → laminar-db core (git tag pin, `api` cargo feature)        │
│  → async core methods polled on the napi tokio runtime       │
│  → Arrow RecordBatch ⇄ IPC bytes in napi Buffers             │
└──────────────────────────────────────────────────────────────┘
```

Two layers, mirroring `laminardb-java` and `laminardb-python`:

1. **Rust layer** (one cdylib): napi-rs classes and functions over the pinned core's async
   API. Owns handle lifetimes, the error contract (`[LAMINAR_<n>]` message prefix on every
   failure — see D2), and Arrow serialization.
2. **TypeScript layer** (`ts/`): the friendly, documented API. The generated
   `index.js`/`index.d.ts` are an internal seam, never the public surface.

## 3. Decision records

### D1 — napi-rs v3 compiled addon over the pinned core (not C FFI, not dynamic FFI)

**Decision.** The Rust cdylib uses napi-rs v3 and calls the pinned `laminar-db` crate
directly (cargo feature `api`), exactly as `laminardb-java` uses it through JNI and
`laminardb-python` through PyO3.

**Why.**

- The C `ffi` feature is incomplete for bindings (no config-based open, no insert, no
  checkpoint, no cdylib crate-type, no header); the `api` feature is the core's intended
  binding seam. **Zero main-repo changes** — the binding lives entirely in this repo.
- napi-rs is the ecosystem default (swc, Rspack, LanceDB, nodejs-polars): Node-API ABI
  stability across Node 20/22/24 and Electron, a managed tokio runtime for async fns,
  generated TypeScript declarations, and the per-platform optional-dependency distribution
  machinery.
- Dynamic FFI against the C ABI (koffi/ffi-rs) is a prototyping tool: no promise
  integration, hand-declared ArrowArray layouts, clumsy `.node`-free distribution.
  Rejected for the shipped SDK.

**Consequences.** Node-API discipline: no blocking the JS thread, panics converted to
rejections (never crossing FFI unchecked), callbacks marshalled via ThreadsafeFunction.
Diverging from Java (sync JNI over the sync `api` facade) is deliberate — see D2.

### D2 — Async-first: bind the core's async API, not the sync `api::Connection` facade

**Decision.** Every data-plane method returns a `Promise`; the underlying futures are
polled on napi-rs's tokio runtime. The binding calls
`LaminarDB::{execute, open_subscription, source_untyped, start, shutdown, checkpoint}` and
`QueryHandle::subscribe_raw` directly rather than
`api::Connection::{execute, query, writer, ...}`.

**Why.**

- The sync facade is designed for languages without first-class async: each call either
  spawns a scoped OS thread to `block_on` inside an ambient runtime or builds a throwaway
  per-call runtime. Under napi both behaviors are pure waste — an ambient runtime always
  exists.
- At the pin, `api::Connection::subscribe` and `ArrowSubscription::next_frame` **reject
  calls made inside a runtime** (verified in the pinned source; the Java binding works
  around this with `spawn_blocking`). The async core methods (`next_frame_async`,
  `open_subscription`) have no such guard.
- Node's idiom is promises end to end; sync-cheap status accessors (`isClosed`, `pending`,
  `isBackpressured`) stay sync where the underlying read is a lock-free check.

**Consequences.** Hot ingestion (`push_arrow`) is called directly on the source handle and
never spawns. Cancellation is explicit (`cancel()`, `close()`), never implied by dropping
a promise. If per-call benchmarks ever show the napi runtime starving the engine, a custom
runtime (napi's `create_custom_tokio_runtime` SPI) is the tuning knob — decided by
measurement, not speculation.

### D3 — Embedded-first; the sidecar already exists

**Decision.** Phase 1 embeds the engine in-process (`:memory:` and local-durable via
`storageDir` + checkpoints). Multi-node cluster execution requires server processes and is
out of scope; a client driver for the standalone `laminardb` server is a later phase with
its own decision record.

### D4 — Versioning: track the core, pin the core

**Decision.** Binding version tracks the core version (binding `0.31.0` ships core
`v0.31.0`); binding-only fixes bump the patch or pre-release. Each release pins an **exact
core git tag** in `Cargo.toml` — never a branch, never `main`. `laminardb-python` cloning
`main` at build time is the mistake not to copy. `CORE_PIN.md` is the registry; the
release gate validates tag == Cargo version == package.json version == pinned core tag ==
CHANGELOG.

### D5 — Node floor: 20

Node 18 and 20 are past or at end-of-life; 22 is the active LTS and 24 the current LTS.
Node-API keeps one binary working across all of them plus Electron. CI matrix: 20, 22, 24.
pnpm is the dev package manager (npm's optional-dependency lockfile handling is
historically buggy; napi-rs recommends pnpm).

### D6 — Arrow is the data plane, as IPC bytes

**Decision.** RecordBatches cross as Arrow IPC stream bytes in napi `Buffer`s (serialized
Rust-side, one copy). JS rehydrates via the `apache-arrow` npm package (`tableFromIPC`) —
an **optional peer dependency** — or uses the binding's own `toArray()` row conversion
(Rust-side, zero JS dependencies).

**Why.** This is what LanceDB ships and what the nodejs-polars maintainers recommend; it
is fully typed and has no exotic lifetime coupling. The zero-copy C Data Interface path
(`arrow::ffi` + arrow-js-ffi, keeping the Rust owner alive while JS views exist) is a
Phase 2 spike behind a benchmark gate — it ships only if it beats IPC measurably, and
never removes the IPC path.

### D7 — Crossings are batch-level; subscriptions are async iterables first

Push subscriptions deliver one crossing per `RecordBatch` or barrier. The primary style is
an async iterator over framed data
(`{type:'data', batch, sequence} | {type:'barrier', ...}`) with natural backpressure
(frames are pulled). Push callbacks (`onData`/`onError`/`onClose`) via weak
ThreadsafeFunction with a bounded queue are the convenience style. Lag and error frames
from the portal are terminal: the iterator ends and the error surfaces with its core code.

### D8 — The public API hides the native mechanism

The hand-written TypeScript layer (`ts/`) is the API users import. It layers async
iterators, the error-class hierarchy, and `apache-arrow` interop over the generated seam.
Nothing public mentions napi, `.node` files, or code strings; a future backend (e.g. a
server driver) slots under the same public shape.

### D9 — Distribution: per-platform optional dependencies

Main package `@laminardb/node` plus `@laminardb/node-<platform>` optional dependencies
auto-selected by npm (`darwin-x64`, `darwin-arm64`, `linux-x64-gnu`, `linux-x64-musl`,
`linux-arm64-gnu`, `linux-arm64-musl`, `win32-x64-msvc`) — the `@swc/core` pattern. No
postinstall scripts, no node-gyp, no download step; unsupported platforms fail with a
clear error. `napi prepublish` manages the platform packages; humans do not run
`npm publish` directly.

## 4. Phase map

| Phase | Plan | Ships                                                                                                           | Exit criteria                                                           |
| ----- | ---- | --------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| 0     | 01   | repo scaffold, napi build wiring, CI                                                                            | open/execute/close smoke green in CI on Linux + macOS                   |
| 1     | 02   | embedded MVP `0.30.0-alpha.x` (query results, insert, writer, config open, catalog, error classes, TS layer)    | quickstart runs from a bare project with one packed dependency          |
| 2     | 03   | subscriptions (async iterator + push), query streaming/cancel, metrics parity, benchmarks, Windows + musl in CI | parity with the Python binding's core flows; benchmark numbers recorded |
| 3     | 04   | npm release engineering: platform packages, release workflow, verify-publish                                    | `npm i @laminardb/node` works on all 7 platforms                        |
| —     | 05   | future: server client-driver, distributed surfaces                                                              | out of scope until Phase 3 exits                                        |

Plan 06 (review gates) is written before Phase 1 exits.

## 5. Cross-cutting conventions

- **Commits**: Conventional Commits; no AI/assistant attribution, no `Co-Authored-By`
  trailers, no tool-session metadata (both repos' policy).
- **Errors**: every native failure — sync throw or promise rejection — surfaces with a
  `[LAMINAR_<n>]` code prefix in the message (napi-rs 3.12 drops custom `error.code`
  values on rejections; verified, plan 01 §Spike results); the TS layer parses the prefix
  and rethrows the `LaminarError` hierarchy carrying `error.code`. Never swallow; never
  surface napi-level failures raw.
- **Lifecycles**: every handle has an idempotent `close()`; use-after-close rejects with
  `LAMINAR_101`, never segfaults. GC finalizers are a leak backstop only — no cleanup
  logic lives in them.
- **Thread-safety**: documented per class. `Connection` is safe to share across async
  contexts (Node is single-threaded, but worker_threads and concurrent promises
  interleave); `Writer`/`Subscription` are single-owner.
- **Blocking**: no napi method blocks the JS thread; anything slow is an async fn polled
  on the napi runtime.
- **Code discipline**: the main repo's readability rules apply to the Rust glue — flat
  control flow, typed errors, no `unwrap`/`expect` on user/network/config-controlled data,
  modules under ~600 lines (extract at ~800), bounded loops with visible termination.
  Every `#[allow]` carries an inline `WHY:`.
- **Verification**: the pinned core tag's source is authoritative. When a plan disagrees
  with the pinned core, fix the plan in the same PR.

## 6. Appendix — verified core surface (core `v0.30.0`, 2026-09-02)

Verified against `crates/laminar-db/src/` at tag `v0.30.0`. The pin overrides this
appendix if they drift.

```rust
// Async core methods this binding drives directly (D2):
LaminarDB::open() -> Result<Arc<LaminarDB>, DbError>
LaminarDB::open_with_config(config: LaminarConfig) -> Result<Arc<LaminarDB>, DbError>
LaminarDB::execute(&self, sql: &str) -> Result<ExecuteResult, DbError>  // async
LaminarDB::open_subscription(&self, name, opts, SubscribeStart) -> ...  // async; SubscriptionPortal
LaminarDB::source_untyped(&self, name: &str) -> Result<UntypedSourceHandle, DbError>
LaminarDB::start(&self) / shutdown(&self) / checkpoint(&self)           // async
QueryHandle::{schema, sql, id, is_active, cancel, subscribe_raw}        // subscribe_raw → Subscription<ArrowRecord>
UntypedSourceHandle::{push_arrow, watermark, current_watermark, pending, capacity,
                      is_backpressured, name, schema}

// core ExecuteResult (handle.rs): Ddl(DdlInfo) | Query(QueryHandle) | RowsAffected(u64) | Metadata(RecordBatch)
// DdlInfo::{statement_type, object_name} are pub; `applied` is pub(crate).

// api module (feature "api") used for types + the error taxonomy:
api::QueryResult::{schema, batches, num_rows, batch(i), into_batches}
api::ApiError::{code() -> i32, message() -> &str}; From<DbError> for ApiError
//   codes: 100s connection, 200s schema, 300s ingestion, 400s query,
//          500s subscription, 900 internal, 901 shutdown
// CAUTION (pin findings from laminardb-java): api::Connection::subscribe and
// ArrowSubscription::next_frame refuse to run inside a tokio runtime; use the
// async paths above. Duplicate CREATE SOURCE surfaces as code 400 (SQL layer),
// not 201.

// LaminarConfig (config.rs) — phase-1-relevant fields:
// default_buffer_size, default_backpressure, storage_dir: Option<PathBuf>,
// checkpoint: Option<StreamCheckpointConfig{interval_ms, timeout_ms, data_dir,
// max_node_data_bytes}>, incremental_emit, object_store_url,
// object_store_options, delivery_guarantee, pipeline_* tunables
```
