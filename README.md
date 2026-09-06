# @laminardb/node

Embedded streaming SQL for Node.js and TypeScript, with no compilation step: install,
import, and query. Prebuilt native binaries ship for every major platform — nothing to
build, no postinstall scripts, no node-gyp.

```sh
npm install @laminardb/node
```

**Requirements:** Node.js 20 or later, on macOS (x64, arm64), Linux (x64 or arm64, glibc
or musl), or Windows (x64). TypeScript types are included — no `@types` package needed.
(npm 11+ recommended; pnpm and yarn work as-is.)

## Your first pipeline

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
console.log(result.toArray())
// [{ device: 'd1', avg_value: 21.5 }, { device: 'd2', avg_value: 18.25 }]
```

That's the whole loop: define a source, start the pipeline, insert rows, query. `CommonJS`
works too — `const { LaminarDB } = require('@laminardb/node')`.

**Durable mode** is one argument:
`LaminarDB.open('./data', { checkpoint: { intervalMs: 5000 } })` keeps your pipeline and
data across restarts.

> Two rules from the engine: `CREATE SOURCE` / `CREATE STREAM` / `CREATE SINK` must run
> **before** `start()`, and manual `checkpoint()` needs at least one stream or sink in the
> topology.

## Reading results

- `result.toArray()` — plain row objects, zero dependencies. `Date`-like columns are epoch
  milliseconds; 64-bit integers are JS `BigInt`.
- `result.toIPC()` — an [Apache Arrow](https://www.npmjs.com/package/apache-arrow) IPC
  `Buffer`, for when rows get big: `tableFromIPC(result.toIPC())` (or the bundled
  `tableFrom(result)`).
- Batch at a time: `result.numBatches()`, `result.batch(i)`.

## Writing data

- `conn.insert('sensors', rows)` — row objects, validated per value with a clear error
  naming the column.
- `conn.insertArrow('sensors', ipcBuffer)` — bulk load straight from Arrow IPC data.
- `conn.writer('sensors')` — streaming writer with event-time `watermark(ts)` and
  backpressure visibility (`pending()`, `isBackpressured()`).

## Subscribing to streams

Consume a stream or materialized view as it computes — async iteration first:

```js
const sub = await conn.subscribe('sensor_rollup')
for await (const frame of sub) {
  if (frame.kind === 'data') console.log(frame.batch.toArray())
  else console.log('checkpoint barrier', frame.checkpointId)
}
```

Prefer callbacks? `conn.subscribeWith('sensor_rollup', { onData, onError, onClose })`
delivers awaited frames — a slow handler slows the stream instead of growing a queue.
Streaming queries work the same way:
`for await (const batch of conn.streamQuery(sql)) {}`.

## Errors

Every failure throws a `LaminarError` subclass with a numeric `code`:

| Class                      | Codes | Meaning                                   |
| -------------------------- | ----- | ----------------------------------------- |
| `LaminarConnectionError`   | 100s  | connection lifecycle                      |
| `LaminarSchemaError`       | 200s  | unknown table, schema problems            |
| `LaminarIngestionError`    | 300s  | bad rows, wrong types, closed writer      |
| `LaminarQueryError`        | 400s  | SQL errors, non-queries                   |
| `LaminarSubscriptionError` | 500s  | subscription failures (502 = fell behind) |
| `LaminarInternalError`     | 900s  | engine or binding internals               |

```js
try {
  conn.insert('sensors', [{ ts: 1, device: 'd1', value: 'oops' }])
} catch (error) {
  if (error instanceof LaminarIngestionError) {
    // "column 'value': expected a number, got string (row 0)"
  }
}
```

Runtime observability: `conn.metrics()`, `conn.sourceMetrics(name)`,
`conn.pipelineState()`, `conn.pipelineWatermark()`, `conn.totalEventsProcessed()`; long
queries can be cancelled with `conn.cancelQuery(id)`.

## Status

`0.30.0-alpha` — the embedded surface is complete (queries, ingestion, subscriptions,
telemetry); the API may still change before 1.0. This binding pins
[LaminarDB](https://github.com/laminardb/laminardb) core `v0.30.0` and covers embedded
mode; multi-node clusters run through the server, not in-process.

## Developing this repository

Contributions need Rust stable (≥ 1.95), Node ≥ 20, and
[just](https://github.com/casey/just):

```sh
just install   # pnpm install
just build     # native addon + TypeScript layer
just test      # full suite against the built addon
just verify    # fmt + clippy + rust tests + build + vitest
```

The first build compiles the pinned Rust core — expect a long cold build. Engineering
records live in `docs/plans/` (decision records, phase plans) and `docs/reviews/`;
`CORE_PIN.md` tracks which core release each version ships; `docs/benchmarks.md` holds the
measured baseline.

Apache-2.0, like the core.
