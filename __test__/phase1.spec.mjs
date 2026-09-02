import { describe, expect, it } from 'vitest'
import { tableFromIPC, tableToIPC } from 'apache-arrow'
import os from 'node:os'
import fs from 'node:fs'
import { open } from '../index.js'

// Phase 1 surface against the real addon (plan 02). Assertions on error
// messages use the [LAMINAR_<n>] prefix contract; exact codes for engine
// paths were observed at the pin and are re-verified here per call site.

const SENSORS_DDL = 'CREATE SOURCE sensors (ts TIMESTAMP, device VARCHAR, value DOUBLE)'

let conn
async function fresh() {
  conn = await open()
  await conn.execute(SENSORS_DDL)
  await conn.start()
  return conn
}

describe('phase 1 — query results', () => {
  it('collects a filtered select with schema, rows, and IPC', async () => {
    const conn = await fresh()
    await conn.insert('sensors', [
      { ts: 1000, device: 'd1', value: 1.5 },
      { ts: 2000, device: 'd2', value: 2.5 },
    ])
    const result = await conn.query('SELECT device, value FROM sensors WHERE value > 1.6')
    expect(result.numRows()).toBe(1)
    expect(result.numBatches()).toBeGreaterThanOrEqual(1)
    expect(result.schema().map((f) => f.name)).toEqual(['device', 'value'])
    expect(result.toArray()).toEqual([{ device: 'd2', value: 2.5 }])

    const table = tableFromIPC(result.toIPC())
    expect(table.numRows).toBe(1)
    expect(table.getChild('device').get(0)).toBe('d2')
    await conn.close()
  })

  it('per-batch access and batch IPC roundtrip', async () => {
    const conn = await fresh()
    await conn.insert('sensors', [{ ts: 1, device: 'd1', value: 0.5 }])
    const result = await conn.query('SELECT * FROM sensors')
    const batch = result.batch(0)
    expect(batch.numColumns()).toBe(3)
    const table = tableFromIPC(batch.toIPC())
    expect(table.numRows).toBe(1)
    expect(table.getChild('value').get(0)).toBeCloseTo(0.5)
    expect(() => result.batch(99)).toThrow(/\[LAMINAR_400\]/)
    await conn.close()
  })

  it('execute returns a metadata outcome for SHOW', async () => {
    const conn = await fresh()
    const outcome = await conn.execute('SHOW SOURCES')
    expect(outcome.kind).toBe('metadata')
    const names = outcome.result.toArray().map((r) => Object.values(r)[0])
    expect(names).toContain('sensors')
    await conn.close()
  })

  it('query() rejects non-query SQL with LAMINAR_400', async () => {
    const conn = await fresh()
    await expect(conn.query('SHOW SOURCES')).rejects.toThrow(/\[LAMINAR_400\]/)
    await conn.close()
  })
})

describe('phase 1 — ingestion', () => {
  it('inserts row objects and reads values back', async () => {
    const conn = await fresh()
    const n = await conn.insert('sensors', [
      { ts: 1700000000000, device: 'd1', value: 3.25 },
      { ts: 1700000001000, device: 'd1', value: 4.75 },
      { device: 'd2', value: 5.5 }, // missing ts -> null
    ])
    expect(n).toBe(3)
    const rows = await conn.query('SELECT * FROM sensors').then((r) => r.toArray())
    expect(rows).toHaveLength(3)
    expect(rows[0]).toEqual({ ts: 1700000000000, device: 'd1', value: 3.25 })
    await conn.close()
  })

  it('inserts Arrow IPC buffers', async () => {
    const conn = await fresh()
    // Build the buffer with the engine's own schema (TIMESTAMP is
    // Microsecond at the pin; hand-built JS vectors get units wrong).
    await conn.insert('sensors', [{ ts: 42, device: 'seed', value: 0 }])
    const seed = await conn.query('SELECT * FROM sensors')
    const table = tableFromIPC(seed.toIPC())
    const n = conn.insertArrow('sensors', tableToIPC(table))
    expect(n).toBe(1)
    const rows = await conn
      .query("SELECT * FROM sensors WHERE device = 'seed'")
      .then((r) => r.toArray())
    expect(rows).toHaveLength(2)
    expect(rows[1].value).toBe(0)
    await conn.close()
  })

  it('rejects garbage IPC with LAMINAR_300', async () => {
    const conn = await fresh()
    expect(() => conn.insertArrow('sensors', Buffer.from('not arrow'))).toThrow(
      /\[LAMINAR_300\]/,
    )
    await conn.close()
  })

  it('rejects type mismatches with column-naming LAMINAR_300', async () => {
    const conn = await fresh()
    expect(() =>
      conn.insert('sensors', [{ ts: 1, device: 'd1', value: 'not a number' }]),
    ).toThrow(/\[LAMINAR_300\].*column 'value'/)
    await conn.close()
  })
})

describe('phase 1 — writer', () => {
  it('writes rows, reports status, and closes idempotently', async () => {
    const conn = await fresh()
    const writer = conn.writer('sensors')
    expect(writer.name()).toBe('sensors')
    expect(writer.schema().map((f) => f.name)).toEqual(['ts', 'device', 'value'])
    const n = await writer.writeRows([{ ts: 5, device: 'w', value: 1.25 }])
    expect(n).toBe(1)
    writer.watermark(5000)
    expect(writer.currentWatermark()).toBe(5000)
    expect(typeof writer.pending()).toBe('number')
    expect(typeof writer.capacity()).toBe('number')
    expect(typeof writer.isBackpressured()).toBe('boolean')
    await writer.close()
    await writer.close() // idempotent
    expect(() => writer.writeRows([{ ts: 6, device: 'w', value: 2 }])).toThrow(
      /\[LAMINAR_301\]/,
    )
    await conn.close()
  })
})

describe('phase 1 — configuration and lifecycle', () => {
  it('opens in-memory via sugar forms', async () => {
    const a = await open()
    await a.close()
    const b = await open(':memory:')
    await b.close()
  })

  it('opens with a storage directory and manual checkpointing', async () => {
    const dir = `${fs.realpathSync(os.tmpdir())}/laminardb-nodejs-test-${Date.now()}`
    const conn = await open(dir, { checkpoint: {} })
    await conn.execute(SENSORS_DDL)
    // The manual checkpoint coordinator only wires up for a real pipeline
    // (at least one stream or sink) — a bare source never starts it.
    await conn.execute(
      'CREATE STREAM rollup AS SELECT device, avg(value) AS avg_v FROM sensors GROUP BY device',
    )
    await conn.start()
    expect(conn.isCheckpointEnabled()).toBe(true)
    await conn.insert('sensors', [{ ts: 1, device: 'd1', value: 1 }])
    const outcome = await conn.checkpoint()
    expect(outcome.success).toBe(true)
    expect(outcome.checkpointId).toBeGreaterThan(0)
    await conn.close()

    const memory = await open()
    expect(memory.isCheckpointEnabled()).toBe(false)
    await expect(memory.checkpoint()).rejects.toThrow(/\[LAMINAR_\d+\]/)
    await memory.close()
  })

  it('catalog lists sources, streams, sinks, and schemas', async () => {
    const conn = await open()
    await conn.execute(SENSORS_DDL)
    await conn.execute(
      'CREATE STREAM rollup AS SELECT device, avg(value) AS avg_v FROM sensors GROUP BY device',
    )
    await conn.start()
    expect(await conn.listSources()).toContain('sensors')
    expect(await conn.listStreams()).toContain('rollup')
    expect(await conn.listSinks()).toEqual([])
    const schema = await conn.schema('sensors')
    expect(schema.map((f) => f.name)).toEqual(['ts', 'device', 'value'])
    await expect(conn.schema('nope')).rejects.toThrow(/\[LAMINAR_200\]/)
    const infos = await conn.sourceInfos()
    expect(infos.find((i) => i.name === 'sensors')).toBeTruthy()
    await conn.close()
  })
})
