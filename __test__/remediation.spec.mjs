import { describe, expect, it } from 'vitest'
import {
  LaminarDB,
  LaminarError,
  LaminarIngestionError,
  toLaminarError,
} from '../dist/index.js'

// The public layer strips the [LAMINAR_<n>] prefix and throws typed errors;
// raw-prefix assertions belong to the native-seam suites (phase1/smoke).

// Regression tests for the phase-1 exit review findings (docs/reviews):
// numeric fidelity (BigInt exactness, 2^53/2^63 edges, temporal guards),
// schema-mismatch codes, null enforcement, and the TypeScript layer's
// coercion wrapping.

const I64_MAX = 9223372036854775807n
const TWO_53_PLUS_1 = 9007199254740993n

describe('numeric fidelity', () => {
  it('BIGINT round-trips 64-bit BigInts exactly', async () => {
    const conn = await LaminarDB.open()
    await conn.execute('CREATE SOURCE b (id BIGINT)')
    await conn.start()
    conn.insert('b', [{ id: TWO_53_PLUS_1 }, { id: I64_MAX }, { id: -I64_MAX }])
    const rows = await conn.query('SELECT * FROM b').then((r) => r.toArray())
    expect(rows.map((r) => r.id)).toEqual([TWO_53_PLUS_1, I64_MAX, -I64_MAX])
    await conn.close()
  })

  it('rejects BigInt beyond i64 and unsafe JS numbers, without corruption', async () => {
    const conn = await LaminarDB.open()
    await conn.execute('CREATE SOURCE b (id BIGINT)')
    await conn.start()
    expect(() => conn.insert('b', [{ id: 9223372036854775808n }])).toThrow(
      /64-bit signed/,
    )
    expect(() => conn.insert('b', [{ id: 2n ** 63n + 100n }])).toThrow(/64-bit signed/)
    // 2^63 as a plain number must be rejected (would silently saturate).
    expect(() => conn.insert('b', [{ id: 9223372036854775808 }])).toThrow(/BigInt/)
    const rows = await conn.query('SELECT * FROM b').then((r) => r.toArray())
    expect(rows).toHaveLength(0) // nothing was admitted
    await conn.close()
  })

  it('temporal columns reject non-finite input instead of storing garbage', async () => {
    const conn = await LaminarDB.open()
    await conn.execute('CREATE SOURCE t (ts TIMESTAMP, day DATE)')
    await conn.start()
    expect(() => conn.insert('t', [{ ts: NaN, day: 0 }])).toThrow(/finite/)
    expect(() => conn.insert('t', [{ ts: 0, day: Infinity }])).toThrow(/finite/)
    expect(() => conn.insert('t', [{ ts: 9007199254740993, day: 0 }])).toThrow(/BigInt/)
    await conn.close()
  })

  it('DATE floors pre-1970 values toward negative infinity', async () => {
    const conn = await LaminarDB.open()
    await conn.execute('CREATE SOURCE d (day DATE)')
    await conn.start()
    conn.insert('d', [{ day: -43_200_000 }, { day: 0 }, { day: 86_400_000 }])
    const rows = await conn.query('SELECT * FROM d').then((r) => r.toArray())
    // -43,200,000 ms is 1969-12-31T12:00Z -> day -1 (not 0)
    expect(rows.map((r) => r.day)).toEqual([-86_400_000, 0, 86_400_000])
    await conn.close()
  })
})

describe('ingestion failure paths', () => {
  it('insertArrow schema mismatch rejects with LAMINAR_302', async () => {
    const conn = await LaminarDB.open()
    await conn.execute('CREATE SOURCE a (x INT)')
    await conn.execute('CREATE SOURCE b (y VARCHAR)')
    await conn.start()
    // Build engine-typed IPC from b, feed it to a.
    conn.insert('b', [{ y: 'text' }])
    const wrong = await conn.query('SELECT * FROM b').then((r) => r.toIPC())
    try {
      conn.insertArrow('a', wrong)
      expect.unreachable('schema mismatch must throw')
    } catch (error) {
      expect(error.code).toBe(302)
      expect(error.message).toMatch(/schema mismatch at column 'x'/)
    }
    await conn.close()
  })

  it('null in a non-nullable column rejects with LAMINAR_300', async () => {
    const conn = await LaminarDB.open()
    await conn.execute('CREATE SOURCE s (a INT NOT NULL)')
    await conn.start()
    expect(() => conn.insert('s', [{ a: null }])).toThrow(/non-nullable.*row 0/)
    expect(() => conn.insert('s', [{}])).toThrow(/non-nullable/)
    await conn.close()
  })

  it('Date objects get the helpful conversion error', async () => {
    const conn = await LaminarDB.open()
    await conn.execute('CREATE SOURCE s (ts TIMESTAMP)')
    await conn.start()
    expect(() => conn.insert('s', [{ ts: new Date(0) }])).toThrow(
      /pass Dates as milliseconds/,
    )
    await conn.close()
  })

  it('unknown sources reject with LAMINAR_200 for insert and writer', async () => {
    const conn = await LaminarDB.open()
    await conn.start()
    expect(() => conn.insert('missing', [{ a: 1 }])).toThrow(/Table not found/)
    expect(() => conn.writer('missing')).toThrow(/Table not found/)
    await conn.close()
  })
})

describe('typescript error layer', () => {
  it('toLaminarError parses codes into the class hierarchy', () => {
    const query = toLaminarError(new Error('[LAMINAR_400] bad sql'))
    expect(query).toBeInstanceOf(LaminarError)
    expect(query.constructor.name).toBe('LaminarQueryError')
    expect(query.code).toBe(400)
    expect(query.message).toBe('bad sql')
    expect(query.cause).toBeInstanceOf(Error)

    const ingestion = toLaminarError(new Error('[LAMINAR_301] closed'))
    expect(ingestion.constructor.name).toBe('LaminarIngestionError')

    const foreign = toLaminarError(new Error('StringExpected'))
    expect(foreign.code).toBe(900)
    expect(foreign.message).toBe('StringExpected')

    expect(toLaminarError(ingestion)).toBe(ingestion)
  })

  it('wraps napi argument-coercion throws on async methods', async () => {
    const conn = await LaminarDB.open()
    await expect(conn.execute(42)).rejects.toBeInstanceOf(LaminarError)
    await expect(conn.execute(42)).rejects.toMatchObject({ code: 900 })
    await expect(conn.query(null)).rejects.toBeInstanceOf(LaminarError)
    await conn.close()
  })

  it('sync methods surface coercion failures as LaminarError too', async () => {
    const conn = await LaminarDB.open()
    await conn.execute('CREATE SOURCE s (a INT)')
    await conn.start()
    try {
      conn.insert('s', 'not rows')
      expect.unreachable()
    } catch (error) {
      // Argument coercion is beneath the engine layer: wrapped as 900.
      expect(error).toBeInstanceOf(LaminarError)
      expect(error.code).toBe(900)
    }
    await conn.close()
  })
})
