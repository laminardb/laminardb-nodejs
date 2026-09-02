#!/usr/bin/env node
/**
 * Cold-consumer proof (plan 02 Task 1.6): pack the tarball, install it into a
 * throwaway project, and run the README quickstart against the installed
 * package — no build tree, no dev dependencies.
 *
 * Until platform packages publish (plan 04), the loader's first resolution
 * choice — a `<binary>.<platform>.node` file next to its index.js — is staged
 * by copying the locally built binary into the installed package.
 */

import { execSync } from 'node:child_process'
import {
  cpSync,
  mkdtempSync,
  rmSync,
  readdirSync,
  existsSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir, platform, arch } from 'node:os'
import { join, resolve } from 'node:path'

const root = resolve(import.meta.dirname, '..')

const tarball = execSync('npm pack --json', { cwd: root, encoding: 'utf8' })
const artifact = JSON.parse(tarball)[0].filename

const project = mkdtempSync(join(tmpdir(), 'laminardb-bare-'))
try {
  execSync(`npm install --no-audit --no-fund ${join(root, artifact)}`, {
    cwd: project,
    stdio: 'inherit',
  })
  const installed = join(project, 'node_modules', '@laminardb', 'node')
  const binary = `laminar_nodejs.${platform}-${arch}.node`
  const built = join(root, binary)
  if (!existsSync(built)) {
    throw new Error(`build the addon first: ${binary} not found at the repo root`)
  }
  cpSync(built, join(installed, binary))
  if (!readdirSync(installed).includes('index.js')) {
    throw new Error('generated loader missing from the tarball (files field)')
  }

  writeFileSync(
    join(project, 'quickstart.mjs'),
    `import { LaminarDB } from '@laminardb/node'

const conn = await LaminarDB.open()
await conn.execute(
  'CREATE SOURCE sensors (ts TIMESTAMP, device VARCHAR, value DOUBLE)',
)
await conn.start()
await conn.insert('sensors', [
  { ts: Date.now(), device: 'd1', value: 21.5 },
])
const result = await conn.query(
  'SELECT device, avg(value) AS avg_value FROM sensors GROUP BY device',
)
const rows = result.toArray()
if (rows.length !== 1 || rows[0].device !== 'd1') {
  throw new Error('quickstart produced wrong rows: ' + JSON.stringify(rows))
}
console.log('bare quickstart ok:', LaminarDB.version())
await conn.close()
`,
  )
  execSync('node quickstart.mjs', { cwd: project, stdio: 'inherit' })
  console.log('bare-quickstart: PASS')
} finally {
  rmSync(project, { recursive: true, force: true })
  rmSync(join(root, artifact), { force: true })
}
