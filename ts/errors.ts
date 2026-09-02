/**
 * The typed error hierarchy (plan 00 D2/D8).
 *
 * The native seam throws plain `Error`s whose message starts with
 * `[LAMINAR_<code>]` (napi-rs 3.12 cannot carry custom codes across promise
 * rejections — see docs/plans/01 spike results). The public API re-throws
 * them as `LaminarError` subclasses with a real `code` property and the
 * prefix stripped from `message`.
 */

const CODE_PREFIX = /^\[LAMINAR_(\d+)\]\s?/

/** Base class: engine or binding failure with a numeric core error code. */
export class LaminarError extends Error {
  /** Numeric code from the core taxonomy (e.g. `400`). */
  readonly code: number
  /** Class name (e.g. `LaminarQueryError`). */
  readonly codeName: string

  constructor(message: string, code: number, options?: { cause?: unknown }) {
    super(message, options as ErrorOptions)
    this.name = new.target.name
    this.code = code
    this.codeName = new.target.name
  }
}

/** 100–199: connection lifecycle failures. */
export class LaminarConnectionError extends LaminarError {}
/** 200–299: schema and catalog failures. */
export class LaminarSchemaError extends LaminarError {}
/** 300–399: ingestion failures. */
export class LaminarIngestionError extends LaminarError {}
/** 400–499: query failures. */
export class LaminarQueryError extends LaminarError {}
/** 500–599: subscription failures. */
export class LaminarSubscriptionError extends LaminarError {}
/** 900–999: internal engine or binding failures. */
export class LaminarInternalError extends LaminarError {}

function classFor(code: number): typeof LaminarError {
  if (code >= 100 && code <= 199) return LaminarConnectionError
  if (code >= 200 && code <= 299) return LaminarSchemaError
  if (code >= 300 && code <= 399) return LaminarIngestionError
  if (code >= 400 && code <= 499) return LaminarQueryError
  if (code >= 500 && code <= 599) return LaminarSubscriptionError
  return LaminarInternalError
}

/**
 * Convert a thrown native error into the typed hierarchy. Coded errors get
 * the matching subclass with the prefix stripped; napi argument-coercion
 * failures (below the engine layer) and anything unrecognized wrap into
 * `LaminarInternalError` with code `900`, preserving the original as
 * `cause`.
 */
export function toLaminarError(error: unknown): LaminarError {
  if (error instanceof LaminarError) return error
  const message = error instanceof Error ? error.message : String(error)
  const match = CODE_PREFIX.exec(message)
  if (match) {
    const code = Number.parseInt(match[1]!, 10)
    const Class = classFor(code)
    return new Class(message.slice(match[0].length), code, { cause: error })
  }
  return new LaminarInternalError(message, 900, { cause: error })
}

/** Run `thunk`, re-throwing any failure through {@link toLaminarError}. */
export function wrapSync<T>(thunk: () => T): T {
  try {
    return thunk()
  } catch (error) {
    throw toLaminarError(error)
  }
}

/**
 * Call `work` and await its result, re-throwing both synchronous throws (napi
 * argument coercion happens before the promise exists) and rejections through
 * {@link toLaminarError}.
 */
export async function wrapAsync<T>(work: () => PromiseLike<T>): Promise<T> {
  try {
    return await work()
  } catch (error) {
    throw toLaminarError(error)
  }
}
