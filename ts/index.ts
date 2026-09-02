/**
 * Public API of `@laminardb/node` (plan 00 D8).
 *
 * This module is the documented surface; the generated napi binding
 * (`index.js` at the package root) is an internal seam. Everything here
 * wraps the native calls so failures surface as the typed
 * {@link LaminarError} hierarchy.
 */

import { wrapAsync, wrapSync } from './errors.js'

export {
  LaminarError,
  LaminarConnectionError,
  LaminarSchemaError,
  LaminarIngestionError,
  LaminarQueryError,
  LaminarSubscriptionError,
  LaminarInternalError,
  toLaminarError,
} from './errors.js'
export { tableFrom } from './arrow.js'

/** One column of a result schema; `dataType` is informational. */
export interface FieldInfo {
  name: string
  dataType: string
  nullable: boolean
}

/** Checkpointing options; an empty object enables manual checkpoints only. */
export interface CheckpointConfig {
  /** Interval in milliseconds; omitted = manual `checkpoint()` only. */
  intervalMs?: number
  /** One attempt deadline in milliseconds; omitted = core default (120 s). */
  timeoutMs?: number
  /** Checkpoint directory; omitted = storage directory, then `./data`. */
  dataDir?: string
  maxNodeDataBytes?: number
}

/** Connection options for {@link LaminarDB.open}. */
export interface OpenConfig {
  /** Local durability directory; wins over the positional path argument. */
  storageDir?: string
  checkpoint?: CheckpointConfig
  /** Default source buffer size in rows. */
  bufferSize?: number
  /** Emit windowed aggregates incrementally before window close. */
  incrementalEmit?: boolean
  /** Object-store URL for cloud checkpoints (e.g. `s3://bucket/prefix`). */
  objectStoreUrl?: string
  objectStoreOptions?: Record<string, string>
}

/** Row object: column name to value. `null` marks null slots. */
export type Row = Record<string, unknown>

/** A fully collected query result. Obtain via `Connection.query()` or
 * `ExecuteOutcome.result`. */
export interface QueryResult {
  /** Schema fields in declaration order. */
  schema(): FieldInfo[]
  numRows(): number
  numBatches(): number
  /** Batch `index` (0-based); throws `LaminarQueryError` (400) if out of range. */
  batch(index: number): ArrowBatch
  /** The whole result as one Arrow IPC stream `Buffer`. */
  toIPC(): Buffer
  /** All rows as objects; see the conversion notes in the README. */
  toArray(): Row[]
}

/** One Arrow RecordBatch of query output. */
export interface ArrowBatch {
  numRows(): number
  numColumns(): number
  schema(): FieldInfo[]
  toIPC(): Buffer
  toArray(): Row[]
}

/** One executed statement's outcome; `kind` discriminates the payload. */
export interface ExecuteOutcome {
  readonly kind: 'ddl' | 'rows-affected' | 'query' | 'metadata'
  readonly statementType?: string
  readonly objectName?: string
  readonly rowsAffected?: number
  readonly queryId?: number
  /** The collected result for `query`/`metadata` kinds; `undefined` otherwise. */
  readonly result?: QueryResult
}

/** One manual checkpoint's outcome. */
export interface CheckpointOutcome {
  readonly success: boolean
  readonly checkpointId: number
  readonly epoch: number
  readonly durationMs: number
  readonly error?: string
}

/** One registered source. */
export interface SourceInfo {
  name: string
  schema: FieldInfo[]
  watermarkColumn?: string
}

// Structural views of the native seam — the generated classes satisfy these
// by shape; the native `.d.ts` never becomes public API.
interface NativeFieldInfo {
  name: string
  dataType: string
  nullable: boolean
}
interface NativeArrowBatch {
  numRows(): number
  numColumns(): number
  schema(): NativeFieldInfo[]
  toIPC(): Buffer
  toArray(): Record<string, unknown>[]
}
interface NativeQueryResult {
  schema(): NativeFieldInfo[]
  numRows(): number
  numBatches(): number
  batch(index: number): NativeArrowBatch
  toIPC(): Buffer
  toArray(): Record<string, unknown>[]
}
interface NativeExecuteOutcome {
  readonly kind: string
  readonly statementType?: string
  readonly objectName?: string
  readonly rowsAffected?: number
  readonly queryId?: number
  readonly result?: NativeQueryResult
}
interface NativeConnection {
  execute(sql: string): Promise<NativeExecuteOutcome>
  query(sql: string): Promise<NativeQueryResult>
  insert(source: string, rows: Record<string, unknown>[]): number
  insertArrow(source: string, bytes: Buffer): number
  writer(source: string): NativeWriter
  start(): Promise<void>
  checkpoint(): Promise<{
    success: boolean
    checkpointId: number
    epoch: number
    durationMs: number
    error?: string
  }>
  isCheckpointEnabled(): boolean
  listSources(): Promise<string[]>
  listStreams(): Promise<string[]>
  listSinks(): Promise<string[]>
  sourceInfos(): Promise<
    { name: string; schema: NativeFieldInfo[]; watermarkColumn?: string }[]
  >
  schema(name: string): Promise<NativeFieldInfo[]>
  isClosed(): boolean
  close(): Promise<void>
}
interface NativeWriter {
  name(): string
  schema(): NativeFieldInfo[]
  writeRows(rows: Record<string, unknown>[]): number
  writeArrow(bytes: Buffer): number
  watermark(timestamp: number): void
  currentWatermark(): number
  pending(): number
  capacity(): number
  isBackpressured(): boolean
  close(): void
}
interface NativeModule {
  open(path?: string, config?: Record<string, unknown>): Promise<NativeConnection>
  version(): string
}

const native = (require('../index.js') as NativeModule) ?? undefined

/** Streaming writer for one source; single-owner. */
export class Writer {
  readonly #native: NativeWriter

  /** @internal */
  constructor(native: NativeWriter) {
    this.#native = native
  }

  name(): string {
    return this.#native.name()
  }

  schema(): FieldInfo[] {
    return wrapSync(() => this.#native.schema())
  }

  /** Push one batch built from row objects; returns rows written. */
  writeRows(rows: Row[]): number {
    return wrapSync(() => this.#native.writeRows(rows))
  }

  /** Push every batch from an Arrow IPC stream `Buffer`. */
  writeArrow(bytes: Buffer): number {
    return wrapSync(() => this.#native.writeArrow(bytes))
  }

  /** Advance the event-time watermark (milliseconds since epoch). */
  watermark(timestamp: number): void {
    this.#native.watermark(timestamp)
  }

  currentWatermark(): number {
    return this.#native.currentWatermark()
  }

  /** Rows buffered in the source, not yet consumed by the pipeline. */
  pending(): number {
    return this.#native.pending()
  }

  capacity(): number {
    return this.#native.capacity()
  }

  /** True when the source buffer is more than 80% full — slow down. */
  isBackpressured(): boolean {
    return this.#native.isBackpressured()
  }

  /** Idempotent; writes after close throw `LaminarIngestionError` (301). */
  close(): void {
    this.#native.close()
  }
}

/**
 * An open LaminarDB connection. Safe to share across async contexts;
 * `close()` is idempotent, and use after close throws
 * `LaminarConnectionError` (101) rather than crashing.
 *
 * DDL that changes topology (`CREATE SOURCE`/`STREAM`/`SINK`) must run
 * before `start()`; the engine rejects topology changes on a running
 * pipeline.
 */
export class Connection {
  readonly #native: NativeConnection

  /** @internal */
  constructor(native: NativeConnection) {
    this.#native = native
  }

  /**
   * Execute one SQL statement. `SELECT` returns `kind: 'query'` with the
   * fully collected `result`; SHOW/DESCRIBE return `kind: 'metadata'`.
   */
  execute(sql: string): Promise<ExecuteOutcome> {
    return wrapAsync(() => this.#native.execute(sql)).then(mapOutcome)
  }

  /** Execute a query and return its collected result; non-query SQL throws
   * `LaminarQueryError` (400). */
  query(sql: string): Promise<QueryResult> {
    return wrapAsync(() => this.#native.query(sql)).then(wrapResult)
  }

  /** Ingest row objects into a source; returns rows pushed. */
  insert(source: string, rows: Row[]): number {
    return wrapSync(() => this.#native.insert(source, rows))
  }

  /** Ingest an Arrow IPC stream `Buffer` into a source; returns rows pushed. */
  insertArrow(source: string, bytes: Buffer): number {
    return wrapSync(() => this.#native.insertArrow(source, bytes))
  }

  /** Open a streaming writer for a source (throws 200 if unknown). */
  writer(source: string): Writer {
    return wrapSync(() => new Writer(this.#native.writer(source)))
  }

  /** Start the streaming pipeline (idempotent). */
  start(): Promise<void> {
    return wrapAsync(() => this.#native.start())
  }

  /**
   * Trigger a manual checkpoint. Requires checkpointing in the open config
   * and at least one stream or sink in the topology (the core wires the
   * coordinator only for real pipelines).
   */
  checkpoint(): Promise<CheckpointOutcome> {
    return wrapAsync(() => this.#native.checkpoint())
  }

  isCheckpointEnabled(): boolean {
    return this.#native.isCheckpointEnabled()
  }

  listSources(): Promise<string[]> {
    return wrapAsync(() => this.#native.listSources())
  }

  listStreams(): Promise<string[]> {
    return wrapAsync(() => this.#native.listStreams())
  }

  listSinks(): Promise<string[]> {
    return wrapAsync(() => this.#native.listSinks())
  }

  sourceInfos(): Promise<SourceInfo[]> {
    return wrapAsync(() => this.#native.sourceInfos())
  }

  /** Schema of a source; unknown names throw `LaminarSchemaError` (200). */
  schema(name: string): Promise<FieldInfo[]> {
    return wrapAsync(() => this.#native.schema(name))
  }

  isClosed(): boolean {
    return this.#native.isClosed()
  }

  /** Graceful shutdown; idempotent and safe under concurrent calls. */
  close(): Promise<void> {
    return wrapAsync(() => this.#native.close())
  }
}

/** Entry point. */
export class LaminarDB {
  private constructor() {
    throw new Error('LaminarDB is a static entry point; use LaminarDB.open()')
  }

  /**
   * Open an embedded database. `open()` and `open(':memory:')` are
   * in-memory; `open(path)` sets the storage directory (local-durable
   * embedded mode when `checkpoint` is configured); `config.storageDir`
   * wins over the positional path.
   */
  static open(path?: string, config?: OpenConfig): Promise<Connection> {
    return wrapAsync(() =>
      native.open(path, config as Record<string, unknown> | undefined),
    ).then((connection) => new Connection(connection))
  }

  /** Binding and pinned-core version, e.g. `0.30.0-alpha.1 (core v0.30.0)`. */
  static version(): string {
    return native.version()
  }
}

class QueryResultImpl implements QueryResult {
  readonly #native: NativeQueryResult

  constructor(native: NativeQueryResult) {
    this.#native = native
  }

  schema(): FieldInfo[] {
    return wrapSync(() => this.#native.schema())
  }

  numRows(): number {
    return this.#native.numRows()
  }

  numBatches(): number {
    return this.#native.numBatches()
  }

  batch(index: number): ArrowBatch {
    return wrapSync(() => new ArrowBatchImpl(this.#native.batch(index)))
  }

  toIPC(): Buffer {
    return wrapSync(() => this.#native.toIPC())
  }

  toArray(): Row[] {
    return wrapSync(() => this.#native.toArray())
  }
}

class ArrowBatchImpl implements ArrowBatch {
  readonly #native: NativeArrowBatch

  constructor(native: NativeArrowBatch) {
    this.#native = native
  }

  numRows(): number {
    return this.#native.numRows()
  }

  numColumns(): number {
    return this.#native.numColumns()
  }

  schema(): FieldInfo[] {
    return wrapSync(() => this.#native.schema())
  }

  toIPC(): Buffer {
    return wrapSync(() => this.#native.toIPC())
  }

  toArray(): Row[] {
    return wrapSync(() => this.#native.toArray())
  }
}

function wrapResult(native: NativeQueryResult): QueryResult {
  return new QueryResultImpl(native)
}

function mapOutcome(native: NativeExecuteOutcome): ExecuteOutcome {
  return {
    kind: native.kind as ExecuteOutcome['kind'],
    statementType: native.statementType,
    objectName: native.objectName,
    rowsAffected: native.rowsAffected,
    queryId: native.queryId,
    result: native.result === undefined ? undefined : wrapResult(native.result),
  }
}
