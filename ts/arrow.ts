/**
 * `apache-arrow` interop helpers (plan 00 D6).
 *
 * `apache-arrow` is an optional peer dependency: these helpers load it
 * lazily and throw a clear `LaminarError` when it is absent. The IPC
 * `Buffer` API on results/batches always works without it.
 */

import { LaminarInternalError } from './errors.js'
import type { ArrowBatch, QueryResult } from './index.js'

/** Minimal structural type for the peer's `tableFromIPC`. */
type ArrowModule = {
  tableFromIPC: (source: ArrayBuffer | Uint8Array) => unknown
}

let arrowModule: ArrowModule | undefined

function arrow(): ArrowModule {
  if (arrowModule === undefined) {
    try {
      // WHY require: optional peer — load lazily so absence only fails here
      arrowModule = require('apache-arrow') as ArrowModule
    } catch {
      throw new LaminarInternalError(
        'apache-arrow is not installed; add it as a dependency to use tableFrom(), or use the toIPC()/toArray() APIs',
        900,
      )
    }
  }
  return arrowModule
}

/**
 * Rehydrate a whole result (or a single batch) as an `apache-arrow` `Table`
 * via `tableFromIPC`. Requires the optional `apache-arrow` dependency.
 */
export function tableFrom(source: QueryResult | ArrowBatch): unknown {
  return arrow().tableFromIPC(source.toIPC())
}
