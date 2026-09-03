import { describe, expect, it } from 'vitest'
import { open } from '../index.js'

// Phase 2 surface (plan 03) against the real addon: framed subscriptions
// (pull + push), streaming queries with cancel, and telemetry. Frames arrive
// after the coordinator cycles on inserted data, so waits are bounded by
// polling loops with deadlines rather than fixed sleeps.

const DEADLINE_MS = 5_000

async function until(predicate, { deadline = DEADLINE_MS } = {}) {
  const end = Date.now() + deadline
  while (Date.now() < end) {
    if (await predicate()) return true
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
  return false
}

let conn
async function pipeline() {
  conn = await open()
  await conn.execute('CREATE SOURCE sensors (ts TIMESTAMP, device VARCHAR, value DOUBLE)')
  await conn.execute(
    'CREATE STREAM rollup AS SELECT device, avg(value) AS avg_v FROM sensors GROUP BY device',
  )
  await conn.start()
  return conn
}

describe('phase 2 — pull subscription', () => {
  it('delivers data frames with batches as rows flow', async () => {
    const conn = await pipeline()
    const sub = await conn.subscribe('rollup')
    expect(sub.schema().map((f) => f.name)).toEqual(['device', 'avg_v'])
    expect(sub.isActive()).toBe(true)

    conn.insert('sensors', [{ ts: 1, device: 'd1', value: 2 }])
    let frame = null
    expect(
      await until(async () => {
        frame = await sub.nextFrame()
        return frame !== null
      }),
    ).toBe(true)
    expect(frame.kind).toBe('data')
    expect(frame.batch.numRows()).toBeGreaterThanOrEqual(1)
    expect(frame.batch.toArray()[0].device).toBe('d1')
    expect(frame.sequence).toBeGreaterThanOrEqual(0)

    sub.cancel()
    expect(await sub.nextFrame()).toBeNull()
    expect(sub.isActive()).toBe(false)
    await conn.close()
  })

  it('cancel wakes a pending nextFrame', async () => {
    const conn = await pipeline()
    const sub = await conn.subscribe('rollup')
    const pending = sub.nextFrame()
    setTimeout(() => sub.cancel(), 50)
    expect(await pending).toBeNull()
    await conn.close()
  })

  it('rejects subscribing to bare sources and unknown names', async () => {
    const conn = await pipeline()
    await expect(conn.subscribe('sensors')).rejects.toThrow(
      /sensors|not subscribable|not found/i,
    )
    await expect(conn.subscribe('missing')).rejects.toThrow()
    await conn.close()
  })
})

describe('phase 2 — push subscription', () => {
  it('delivers frames to onData and stops after close', async () => {
    const conn = await pipeline()
    const frames = []
    const errors = []
    let closed = false
    const sub = conn.subscribeWith(
      'rollup',
      null,
      null,
      (frame) => frames.push(frame),
      (error) => errors.push([error.code, error.message]),
      () => {
        closed = true
      },
    )
    expect(sub.isActive()).toBe(true)

    conn.insert('sensors', [{ ts: 1, device: 'd1', value: 3 }])
    expect(await until(() => frames.length > 0)).toBe(true)
    expect(frames[0].kind).toBe('data')
    expect(frames[0].batch.toArray()[0].avg_v).toBe(3)

    await sub.close()
    await sub.close() // idempotent
    expect(closed).toBe(true)
    const delivered = frames.length
    conn.insert('sensors', [{ ts: 2, device: 'd2', value: 4 }])
    await new Promise((resolve) => setTimeout(resolve, 200))
    expect(frames.length).toBe(delivered) // no callbacks after close
    expect(errors).toEqual([])
    await conn.close()
  })

  it('surfaces open failures through onError + onClose', async () => {
    const conn = await pipeline()
    const errors = []
    let closed = false
    const sub = conn.subscribeWith(
      'missing-stream',
      null,
      null,
      () => {},
      (error) => errors.push([error.code, error.message]),
      () => {
        closed = true
      },
    )
    expect(await until(() => closed, { deadline: DEADLINE_MS })).toBe(true)
    expect(errors.length).toBe(1)
    expect(String(errors[0][1])).toMatch(/missing-stream|not found|not subscribable/i)
    expect(sub.isActive()).toBe(true) // handle reports engine state, not delivery
    await sub.close()
    await conn.close()
  })
})

describe('phase 2 — streaming queries', () => {
  it('streams batches on demand and cancels', async () => {
    const conn = await pipeline()
    conn.insert('sensors', [
      { ts: 1, device: 'd1', value: 1 },
      { ts: 2, device: 'd2', value: 2 },
    ])
    const stream = await conn.streamQuery('SELECT * FROM sensors')
    expect(stream.queryId()).toBeGreaterThan(0)
    let first = null
    expect(
      await until(async () => {
        first = await stream.nextBatch()
        return first !== null
      }),
    ).toBe(true)
    expect(first.numRows()).toBe(2)

    stream.cancel()
    expect(await stream.nextBatch()).toBeNull()
    await expect(conn.streamQuery('SHOW SOURCES')).rejects.toThrow(/metadata statement/)
    await conn.close()
  })

  it('cancelQuery rejects unknown ids', async () => {
    const conn = await pipeline()
    await expect(conn.cancelQuery(999_999)).rejects.toThrow()
    await conn.close()
  })
})

describe('phase 2 — telemetry', () => {
  it('reports pipeline state, watermarks, and source metrics', async () => {
    const conn = await pipeline()
    conn.insert('sensors', [{ ts: 5000, device: 'd1', value: 1 }])
    conn.writer('sensors').watermark(5000)

    expect(await conn.pipelineState()).toBe('Running')
    expect(
      await until(async () => (await conn.sourceMetrics('sensors')).totalEvents > 0),
    ).toBe(true)
    const metrics = await conn.metrics()
    expect(metrics.state).toBe('Running')
    expect(metrics.sourceCount).toBe(1)
    expect(metrics.streamCount).toBe(1)
    expect(await conn.totalEventsProcessed()).toBeGreaterThan(0)
    await expect(conn.sourceMetrics('missing')).rejects.toThrow(/source not found/)
    const streamMetrics = await conn.streamMetrics('rollup')
    expect(streamMetrics.name).toBe('rollup')
    const allStreams = await conn.allStreamMetrics()
    expect(allStreams.map((m) => m.name)).toContain('rollup')
    await conn.close()
  })
})
