import { describe, expect, it } from 'vitest'
import { LaminarDB } from '../dist/index.js'

// The TypeScript layer over the Phase 2 surface: async-iterable
// subscriptions and streams, Date convenience in rows, and typed errors.

const DEADLINE_MS = 5_000

async function pipeline() {
  const conn = await LaminarDB.open()
  await conn.execute('CREATE SOURCE sensors (ts TIMESTAMP, device VARCHAR, value DOUBLE)')
  await conn.execute(
    'CREATE STREAM rollup AS SELECT device, avg(value) AS avg_v FROM sensors GROUP BY device',
  )
  await conn.start()
  return conn
}

function withTimeout(promise, ms = DEADLINE_MS) {
  let timer
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error('timed out')), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

describe('typescript layer — async iteration', () => {
  it('for-await over a subscription, break cancels', async () => {
    const conn = await pipeline()
    const sub = await conn.subscribe('rollup')
    conn.insert('sensors', [{ ts: Date.now(), device: 'd1', value: 2.5 }])

    const seen = []
    await withTimeout(
      (async () => {
        for await (const frame of sub) {
          expect(frame.kind).toBe('data')
          seen.push(frame.batch.toArray()[0].device)
          break
        }
      })(),
    )
    expect(seen).toEqual(['d1'])
    expect(sub.isActive()).toBe(false)
    await conn.close()
  })

  it('for-await over a streaming query', async () => {
    const conn = await pipeline()
    conn.insert('sensors', [
      { ts: 1, device: 'd1', value: 1 },
      { ts: 2, device: 'd2', value: 2 },
    ])
    // Bounded queries snapshot the source buffer at execution; wait for the
    // pipeline to drain the inserts first (documented streamQuery semantics).
    const drained = Date.now() + DEADLINE_MS
    while (Date.now() < drained) {
      const m = await conn.sourceMetrics('sensors')
      if (m.totalEvents >= 2 && m.pending === 0) break
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
    const stream = await conn.streamQuery('SELECT device FROM sensors')
    const devices = []
    await withTimeout(
      (async () => {
        for await (const batch of stream) {
          devices.push(...batch.toArray().map((r) => r.device))
        }
      })(),
    )
    expect(devices.sort()).toEqual(['d1', 'd2'])
    await conn.close()
  })

  it('push handlers through the facade see typed frames', async () => {
    const conn = await pipeline()
    const frames = []
    const errors = []
    let closed = false
    const sub = conn.subscribeWith('rollup', {
      onData: (frame) => {
        frames.push(frame)
      },
      onError: (error) => errors.push(error),
      onClose: () => {
        closed = true
      },
    })
    conn.insert('sensors', [{ ts: 3, device: 'd9', value: 9 }])
    const end = Date.now() + DEADLINE_MS
    while (frames.length === 0 && Date.now() < end) {
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
    expect(frames.length).toBeGreaterThan(0)
    expect(frames[0].kind).toBe('data')
    expect(errors).toEqual([])
    await sub.close()
    expect(closed).toBe(true)
    await conn.close()
  })

  it('Date instances cross as epoch milliseconds', async () => {
    const conn = await pipeline()
    const date = new Date('2026-09-03T00:00:00Z')
    conn.insert('sensors', [{ ts: date, device: 'd1', value: 1 }])
    const rows = await withTimeout(
      conn.query('SELECT ts FROM sensors').then((r) => r.toArray()),
    )
    expect(rows[0].ts).toBe(date.getTime())
    const writer = conn.writer('sensors')
    writer.writeRows([{ ts: new Date(date.getTime() + 1000), device: 'd2', value: 2 }])
    const total = await conn.query('SELECT * FROM sensors').then((r) => r.numRows())
    expect(total).toBe(2)
    writer.close()
    await conn.close()
  })

  it('telemetry methods are wrapped and typed', async () => {
    const conn = await pipeline()
    conn.insert('sensors', [{ ts: 1, device: 'd1', value: 1 }])
    const state = await conn.pipelineState()
    expect(state).toBe('Running')
    const metrics = await conn.metrics()
    expect(typeof metrics.uptimeMs).toBe('number')
    try {
      await conn.streamQuery('SELECT * FROM sensors WHERE value > 0')
    } catch {
      // not expected
    }
    await conn.close()
  })
})
