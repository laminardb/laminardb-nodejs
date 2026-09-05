#!/usr/bin/env node
/**
 * Split TypeScript's chained export declarations in the compiled CJS layer.
 *
 * tsc emits `exports.A = exports.B = ... = void 0;` as TDZ-safe hoisting for
 * exported classes. Node's cjs-module-lexer (before ~23) cannot parse the
 * chained form, so static ESM named imports (`import { LaminarDB } from
 * '@laminardb/node'`) fail on Node 22 LTS with "Named export not found".
 * Rewriting the chain as one assignment per name keeps every Node version's
 * static analysis happy; runtime semantics are unchanged (each name is
 * assigned `void 0` first and its real value later, exactly as before).
 */

import { readdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

const dist = new URL('../dist', import.meta.url).pathname
let rewritten = 0

for (const file of readdirSync(dist)) {
  if (!file.endsWith('.js')) continue
  const path = join(dist, file)
  const source = readFileSync(path, 'utf8')
  const updated = source.replace(
    /^exports\.[A-Za-z_$][\w$]*(?:\s*=\s*exports\.[A-Za-z_$][\w$]*)+\s*=\s*void 0;.*$/gm,
    (chain) =>
      chain
        .split(/\s*=\s*/)
        .filter((token) => token.startsWith('exports.'))
        .map((name) => `${name} = void 0;`)
        .join('\n'),
  )
  if (updated !== source) {
    writeFileSync(path, updated)
    rewritten += 1
  }
}

console.log(`split-export-chains: rewrote ${rewritten} file(s)`)
