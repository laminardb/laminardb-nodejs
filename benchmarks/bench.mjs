import { writeFileSync } from 'node:fs'
import { Bench } from 'tinybench'
import { tableFromIPC, tableToIPC } from 'apache-arrow'
import { LaminarDB } from '../dist/index.js'

// Phase 2 benchmark set (plan 03 Task 2.6). Insert workloads get their own
// source (they balloon the table while being measured); query workloads run
// against a fixed, pre-seeded source. Run on quiet hardware; record results
// with hardware notes in docs/benchmarks.md.

const bench = new Bench({ time: 2000, warmupTime: 500 })

const conn = await LaminarDB.open()
await conn.execute('CREATE SOURCE ingest (ts TIMESTAMP, id BIGINT, v DOUBLE)')
await conn.execute('CREATE SOURCE qsrc (ts TIMESTAMP, id BIGINT, v DOUBLE)')
await conn.execute(
  'CREATE STREAM rollup AS SELECT id, avg(v) AS avg_v FROM ingest GROUP BY id',
)
await conn.start()

const rows = (n, offset = 0) =>
  Array.from({ length: n }, (_, i) => ({
    ts: offset + i,
    id: BigInt((offset + i) % 100),
    v: i * 0.5,
  }))

// Fixed 10k-row seed for the query benchmarks.
for (let i = 0; i < 10; i++) {
  conn.insert('qsrc', rows(1000, i * 1000))
}
await new Promise((resolve) => setTimeout(resolve, 500))

// A 1k-row engine-typed IPC buffer for the IPC-ingestion benchmark.
conn.insert('ingest', rows(1000))
const seed = await conn.query('SELECT * FROM ingest LIMIT 1000')
const ipcBuffer = tableToIPC(tableFromIPC(seed.toIPC()))

bench
  .add('open + close (in-memory)', async () => {
    const c = await LaminarDB.open()
    await c.close()
  })
  .add('insert 1k rows (row objects)', () => {
    conn.insert('ingest', rows(1000))
  })
  .add('insert 1k rows (Arrow IPC)', () => {
    conn.insertArrow('ingest', ipcBuffer)
  })
  .add('query 10k rows -> toArray()', async () => {
    const result = await conn.query('SELECT * FROM qsrc')
    result.toArray()
  })
  .add('query 10k rows -> toIPC()', async () => {
    const result = await conn.query('SELECT * FROM qsrc')
    result.toIPC()
  })
  .add('apache-arrow tableFromIPC of 1k-row result', async () => {
    const result = await conn.query('SELECT * FROM qsrc LIMIT 1000')
    tableFromIPC(result.toIPC())
  })
  .add('subscription: insert -> first data frame', async () => {
    const sub = await conn.subscribe('rollup')
    conn.insert('ingest', rows(10))
    let frame
    while (frame === undefined) {
      const next = await sub.nextFrame()
      if (next !== null) frame = next
    }
    sub.cancel()
  })

await bench.run()
await conn.close()

const table = bench.tasks.map((task) => ({
  name: task.name,
  'ops/sec': Math.round(task.result?.throughput?.mean ?? 0),
  mean: `${(task.result?.latency?.mean ?? 0).toFixed(3)}ms`,
  p99: `${(task.result?.latency?.p99 ?? 0).toFixed(3)}ms`,
}))
console.table(table)
// Plain-text copy for the nightly artifact.
writeFileSync(
  'bench-output.txt',
  `${new Date().toISOString()}\n${table
    .map(
      (row) => `${row.name}: ${row['ops/sec']} ops/s, mean ${row.mean}, p99 ${row.p99}`,
    )
    .join('\n')}\n`,
)
