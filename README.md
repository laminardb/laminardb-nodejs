# laminardb-nodejs

Embedded streaming SQL for Node.js and TypeScript. This is the official Node.js binding
for [LaminarDB](https://github.com/laminardb/laminardb), built as a
[napi-rs](https://napi.rs) native addon over a pinned core release — the same two-layer
shape as [`laminardb-java`](https://github.com/laminardb/laminardb-java) and
`laminardb-python`, adapted to Node idioms: every data-plane call is a `Promise`, Arrow
data moves as IPC `Buffer`s, and failures throw a typed error hierarchy.

Status: **alpha** — embedded MVP (Phase 1). Subscriptions (async iterators), Windows/musl
CI, and npm distribution land over the next phases. Not yet on npm — build from source.

## Quickstart

```js
import { LaminarDB } from '@laminardb/node'

const conn = await LaminarDB.open()
await conn.execute('CREATE SOURCE sensors (ts TIMESTAMP, device VARCHAR, value DOUBLE)')
await conn.start()

await conn.insert('sensors', [
  { ts: Date.now(), device: 'd1', value: 21.5 },
  { ts: Date.now(), device: 'd2', value: 18.25 },
])

const result = await conn.query(
  'SELECT device, avg(value) AS avg_value FROM sensors GROUP BY device',
)
console.log(result.toArray()) // [{ device: 'd1', avg_value: 21.5 }, ...]

await conn.close()
```

Durable embedded mode is one argument plus checkpointing:

```js
const conn = await LaminarDB.open('./data', { checkpoint: { intervalMs: 5000 } })
```

Topology DDL (`CREATE SOURCE` / `STREAM` / `SINK`) must run before `start()`; manual
`checkpoint()` requires at least one stream or sink in the topology (the core wires the
checkpoint coordinator only for real pipelines).

## Data access

- `result.toArray()` / `batch.toArray()` — row objects, no dependencies. Conventions:
  temporal columns are **milliseconds since epoch** in and out; `Int64`/`UInt64` cross as
  JS `BigInt`.
- `result.toIPC()` / `batch.toIPC()` — one Arrow IPC stream `Buffer`; rehydrate with
  [`apache-arrow`](https://www.npmjs.com/package/apache-arrow) (an optional peer
  dependency): `tableFromIPC(result.toIPC())` or the bundled `tableFrom(result)` helper.
- `conn.insertArrow(source, buffer)` / `writer.writeArrow(buffer)` — bulk ingestion
  straight from Arrow IPC data.
- `conn.writer(source)` — streaming writer with `writeRows`, `watermark`, and backpressure
  visibility (`pending` / `capacity` / `isBackpressured`).

## Subscriptions

Streams and materialized views are consumable frame by frame — async-iterable first:

```js
const sub = await conn.subscribe('sensor_rollup')
for await (const frame of sub) {
  if (frame.kind === 'data') console.log(frame.batch.toArray())
  else console.log('checkpoint barrier', frame.checkpointId)
}
```

Push style delivers awaited frames to handlers (a slow handler backpressures instead of
queueing): `conn.subscribeWith('sensor_rollup', { onData, onError, onClose })`. Streaming
queries work the same way: `for await (const batch of conn.streamQuery(sql))`.

Telemetry (`metrics()`, `sourceMetrics()`, `pipelineState()`, `pipelineWatermark()`,
`totalEventsProcessed()`) and query cancellation (`cancelQuery(id)`) round out the runtime
surface. Benchmark baseline: `docs/benchmarks.md`.

## Errors

Every failure throws a `LaminarError` subclass carrying the core's numeric `code`:
`LaminarConnectionError` (100s), `LaminarSchemaError` (200s), `LaminarIngestionError`
(300s), `LaminarQueryError` (400s), `LaminarSubscriptionError` (500s),
`LaminarInternalError` (900s).

```js
try {
  conn.insert('sensors', [{ ts: 1, device: 'd1', value: 'oops' }])
} catch (error) {
  if (error.code === 300) {
    // error.message: column 'value': expected a number, got string (row 0)
  }
}
```

## Build from source

Requires Rust stable (≥ 1.95) and Node ≥ 20 with pnpm.

```sh
just install   # pnpm install (@napi-rs/cli, vitest, prettier, typescript, apache-arrow)
just build     # debug addon + generated loader + TypeScript layer (dist/)
just test      # vitest suites against the built addon
just verify    # fmt + clippy -D warnings + rust tests + build + vitest
```

The first build clones and compiles the pinned LaminarDB core (git tag in `Cargo.toml`,
registry in `CORE_PIN.md`) — expect a long cold build.

## Platform support

The release matrix (Phase 3) covers macOS x64/arm64, Linux x64/arm64 (glibc and musl), and
Windows x64, distributed as per-platform optional npm packages with no postinstall and no
source compilation. Until then, `napi build` works anywhere the toolchain does.

## Documentation

- `docs/plans/` — decision records and phase plans (start at
  `00-overview-and-decisions.md`)
- `CORE_PIN.md` — which core release each binding version ships
- `CHANGELOG.md`

Apache-2.0, like the core.
