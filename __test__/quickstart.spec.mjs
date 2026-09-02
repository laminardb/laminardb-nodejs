import { describe, expect, it } from 'vitest'
import { tableFromIPC } from 'apache-arrow'
import { LaminarDB, LaminarIngestionError, tableFrom } from '../dist/index.js'

// Documentation-as-tests: the body of each test mirrors the README quickstart
// and examples verbatim. When this suite and the README disagree, one of them
// is wrong — fix both in the same PR.

describe('README quickstart', () => {
  it('runs the quickstart example verbatim', async () => {
    const conn = await LaminarDB.open()
    await conn.execute(
      'CREATE SOURCE sensors (ts TIMESTAMP, device VARCHAR, value DOUBLE)',
    )
    await conn.start()
    await conn.insert('sensors', [
      { ts: Date.now(), device: 'd1', value: 21.5 },
      { ts: Date.now(), device: 'd2', value: 18.25 },
    ])
    const result = await conn.query(
      'SELECT device, avg(value) AS avg_value FROM sensors GROUP BY device',
    )
    const rows = result.toArray()
    expect(rows).toHaveLength(2)
    expect(rows.map((r) => r.device).sort()).toEqual(['d1', 'd2'])

    // The Arrow path: one Buffer, rehydrated by the optional peer.
    const table = tableFrom(result)
    expect(tableFromIPC(result.toIPC()).numRows).toBe(2)
    expect(table).toBeTruthy()
    await conn.close()
  })

  it('typed errors carry code and cleaned message', async () => {
    const conn = await LaminarDB.open()
    await conn.execute('CREATE SOURCE t (a INT)')
    await conn.start()
    try {
      conn.insert('t', [{ a: 'not a number' }])
      expect.unreachable('insert must throw')
    } catch (error) {
      expect(error).toBeInstanceOf(LaminarIngestionError)
      expect(error.code).toBe(300)
      expect(error.message).toMatch(/^column 'a'/)
      expect(error.message).not.toMatch(/\[LAMINAR_/)
    }
    await conn.close()
  })
})
