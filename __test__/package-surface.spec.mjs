import { describe, expect, it } from 'vitest'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'

// The package surface: the CJS implementation (dist/index.js), the ESM shim
// (index.mjs), and the generated native loader must agree. If a name appears
// in one and not the others, consumers see different APIs depending on their
// module system — this suite pins them together.

const publicNames = [
  'LaminarDB',
  'Connection',
  'Writer',
  'Subscription',
  'PushSubscription',
  'QueryStream',
  'toLaminarError',
  'tableFrom',
  'LaminarError',
  'LaminarConnectionError',
  'LaminarSchemaError',
  'LaminarIngestionError',
  'LaminarQueryError',
  'LaminarSubscriptionError',
  'LaminarInternalError',
]

const root = resolve(import.meta.dirname, '..')

describe('package surface', () => {
  it('CJS require exposes every public name', () => {
    const require_ = createRequire(pathToFileURL(resolve(root, 'index.mjs')))
    const cjs = require_(resolve(root, 'dist/index.js'))
    for (const name of publicNames) {
      expect(cjs[name], `dist/index.js is missing ${name}`).toBeDefined()
    }
  })

  it('the ESM shim exposes exactly the public names', async () => {
    const esm = await import(pathToFileURL(resolve(root, 'index.mjs')))
    const exported = Object.keys(esm).sort()
    expect(exported).toEqual([...publicNames].sort())
  })

  it('the exports map routes require and import to real files', () => {
    const manifest = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8'))
    const entry = manifest.exports['.']
    for (const target of [entry.import, entry.require, entry.types]) {
      expect(existsSync(resolve(root, target)), `${target} missing`).toBe(true)
    }
  })
})
