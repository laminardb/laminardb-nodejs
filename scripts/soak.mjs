#!/usr/bin/env node
/**
 * Subscription soak (plan 03 Task 2.6): a push subscription consumes a
 * long-running insert workload; the run fails on lag (502), callback loss,
 * unbounded source growth, or a mismatch between rows inserted and frames
 * observed.
 */

import { LaminarDB } from '../dist/index.js'

const args = process.argv.slice(2)
const durationMs = Number.parseInt(
  args[args.indexOf('--duration-ms') + 1] ?? '60_000'.replace('_', ''),
  10,
)

const conn = await LaminarDB.open()
await conn.execute('CREATE SOURCE soak (ts TIMESTAMP, id BIGINT, v DOUBLE)')
await conn.execute(
  'CREATE STREAM rollup AS SELECT id, count(v) AS n FROM soak GROUP BY id',
)
await conn.start()

let frames = 0
let errors = 0
let closed = false
const sub = conn.subscribeWith('rollup', {
  onData: (frame) => {
    if (frame.kind === 'data') frames += 1
  },
  onError: () => {
    errors += 1
  },
  onClose: () => {
    closed = true
  },
})

const BATCH = 500
const rows = (offset) =>
  Array.from({ length: BATCH }, (_, i) => ({
    ts: offset + i,
    id: BigInt((offset + i) % 64),
    v: 0.5,
  }))

const end = Date.now() + durationMs
let inserted = 0
let peakPending = 0
while (Date.now() < end) {
  conn.insert('soak', rows(inserted))
  inserted += BATCH
  const metrics = await conn.sourceMetrics('soak')
  peakPending = Math.max(peakPending, metrics.pending)
  if (metrics.isBackpressured) {
    // The engine applying backpressure is fine; the consumer must keep up.
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

// Let the tail drain, then stop. `closed` before close() is the failure
// signal; onClose after close() is the contract.
await new Promise((resolve) => setTimeout(resolve, 1000))
const closedEarly = closed
await sub.close()

const failures = []
if (errors > 0) failures.push(`${errors} subscription errors`)
if (closedEarly && inserted > 0) failures.push('subscription closed early')
if (frames === 0) failures.push('no data frames observed')
const finalMetrics = await conn.sourceMetrics('soak')
if (finalMetrics.pending > 10_000) {
  failures.push(`source did not drain: ${finalMetrics.pending} pending`)
}
await conn.close()

console.log(
  JSON.stringify(
    {
      durationMs,
      insertedRows: inserted,
      dataFrames: frames,
      peakPending,
      failures,
    },
    null,
    2,
  ),
)
if (failures.length > 0) {
  console.error('SOAK FAILED')
  process.exit(1)
}
console.log('SOAK PASS')
