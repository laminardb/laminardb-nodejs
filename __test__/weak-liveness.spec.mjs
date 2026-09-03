import { describe, expect, it } from 'vitest'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { resolve } from 'node:path'

// The weak-TSF design decision (plan 03): an abandoned push subscription
// must not keep the Node event loop alive. A strong threadsafe reference
// fails this test — the child process never exits on its own.

const childScript = String.raw`
import { LaminarDB } from ${JSON.stringify(fileURLToPath(new URL('../dist/index.js', import.meta.url)))}
const conn = await LaminarDB.open()
await conn.execute('CREATE SOURCE s (a INT)')
await conn.execute('CREATE STREAM t AS SELECT a * 2 AS a2 FROM s')
await conn.start()
// Subscribe, drop every reference, and do NOT close: only weak callbacks
// let the process end naturally.
const sub = conn.subscribeWith('t', {
  onData: async () => {},
  onError: () => {},
  onClose: () => {},
})
globalThis.__keep = sub
conn.insert('s', [{ a: 1 }])
globalThis.__keep = undefined
// No close, no ref hold: the process must exit on its own.
`

describe('weak references and process lifetime', () => {
  it('an abandoned push subscription does not pin the event loop', async () => {
    const child = spawn(process.execPath, ['--input-type=module', '-e', childScript], {
      cwd: resolve(import.meta.dirname, '..'),
      stdio: 'ignore',
    })
    const exited = await new Promise((resolvePromise) => {
      const timer = setTimeout(() => {
        child.kill('SIGKILL')
        resolvePromise('timeout')
      }, 15_000)
      child.on('exit', () => {
        clearTimeout(timer)
        resolvePromise('exited')
      })
    })
    expect(exited).toBe('exited')
  }, 20_000)
})
