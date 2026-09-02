import { describe, expect, it } from 'vitest'
import { open, version } from '../index.js'

// Mirrors laminardb-java Phase 0's JUnit smoke suite against the real cdylib:
// lifecycle, idempotent close, coded errors, and an open/close soak. Observed
// codes at the pin (v0.30.0): duplicate CREATE SOURCE surfaces as
// LAMINAR_400 (SQL layer), not 201; invalid SQL is LAMINAR_401.

describe('phase 0 smoke', () => {
  it('reports binding and pinned-core version', () => {
    expect(version()).toMatch(/^0\.30\.0-alpha\.1 \(core v0\.30\.0\)$/)
  })

  it('opens, executes DDL, and closes', async () => {
    const conn = await open()
    expect(conn.isClosed()).toBe(false)
    const outcome = await conn.execute('CREATE SOURCE t (a INT)')
    expect(outcome.kind).toBe('ddl')
    expect(outcome.statementType).toBe('CREATE SOURCE')
    expect(outcome.objectName).toBe('t')
    await conn.close()
    expect(conn.isClosed()).toBe(true)
  })

  it('double close is a no-op', async () => {
    const conn = await open()
    await conn.execute('CREATE SOURCE t (a INT)')
    await conn.close()
    await conn.close()
    expect(conn.isClosed()).toBe(true)
  })

  it('execute after close rejects with LAMINAR_101', async () => {
    const conn = await open()
    await conn.close()
    await expect(conn.execute('CREATE SOURCE t (a INT)')).rejects.toThrow(
      /^\[LAMINAR_101\]/,
    )
  })

  it('duplicate CREATE SOURCE rejects with LAMINAR_400 and a message', async () => {
    const conn = await open()
    await conn.execute('CREATE SOURCE t (a INT)')
    await expect(conn.execute('CREATE SOURCE t (a INT)')).rejects.toThrow(
      /^\[LAMINAR_400\]/,
    )
    await conn.close()
  })

  it('invalid SQL rejects with LAMINAR_401', async () => {
    const conn = await open()
    await expect(conn.execute('NOT SQL AT ALL(')).rejects.toThrow(/^\[LAMINAR_401\]/)
    await conn.close()
  })

  it('open/close loop 200x without crashing', async () => {
    for (let i = 0; i < 200; i++) {
      const conn = await open()
      await conn.close()
    }
  })
})
