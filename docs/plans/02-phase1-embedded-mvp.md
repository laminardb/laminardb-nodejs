# Plan 02 — Phase 1: embedded MVP

Status: **Complete (2026-09-02)** · Prerequisites: plan 00 (decisions), plan 01 exited
Exit: the README quickstart runs from a bare throwaway project against the packed tarball
(`scripts/bare-quickstart.mjs` green), and `just review`/`just verify` are green.
Publishing to npm is Phase 3 (plan 04); this phase makes the package publish-ready.

## Goal

The complete embedded surface minus subscriptions (plan 03): configuration-based open,
query results with Arrow IPC data, row ingestion (`insert`, `Writer`), start/checkpoint
lifecycle, catalog introspection, the TypeScript public layer with typed errors, and
documentation-as-tests.

## Verified core facts (pin `v0.30.0`; source read via `git show v0.30.0:`)

- Query collection is batch-preserving and async: `db.execute(sql)` →
  `ExecuteResult::Query(QueryHandle)` → `handle.subscribe_raw()` (first call only;
  `Option` is consumed) → `Subscription::recv_async() -> Result<RecordBatch, RecvError>`;
  loop until `Disconnected`; schema from `handle.schema()`. The underlying broadcast
  buffer holds 2048 messages (`DEFAULT_BROADCAST_CAPACITY`, sink/mod.rs); a query that
  bursts more batches than that between subscriptions **silently skips the overflow**
  (core behavior — identical for the Java/Python bindings through `api::QueryStream`).
  Documented limit: collected results are bounded by consumer promptness, ~2048 batches of
  headroom; revisit if real workloads hit it (Phase 2 note).
- `ExecuteResult::Metadata(RecordBatch)` (SHOW/DESCRIBE) is a single batch with the query
  schema.
- Ingestion: `db.source_untyped(name) -> UntypedSourceHandle`;
  `push_arrow(RecordBatch) -> Result<(), StreamingError>` (sync, owning — the hot path),
  `watermark(i64)`, `current_watermark() -> i64`, `pending()`, `capacity()`,
  `is_backpressured()`, `schema()`, `name()`. The core's `api::Writer` is a thin wrapper
  (closed flag + column-count check; `flush` is a documented no-op) — this binding
  implements the same semantics directly and checks full field names/types for better 302
  errors.
- Catalog: `db.sources()/sinks()/streams() -> Vec<SourceInfo/SinkInfo/StreamInfo>` (what
  `api::Connection::get_schema`/`list_*` call internally).
- Config (`LaminarConfig`): `storage_dir: Option<PathBuf>`,
  `checkpoint: Option<StreamCheckpointConfig>` (laminar-core; fields
  `interval_ms: Option<u64>`, `timeout_ms: Option<u64>` (default 120 s),
  `data_dir: Option<PathBuf>`, `max_node_data_bytes: Option<u64>`), `default_buffer_size`,
  `incremental_emit`, `object_store_url`, `object_store_options`, `delivery_guarantee`,
  plus `pipeline_*` tunables (deferred past Phase 1).
  `LaminarDB::open_with_config(LaminarConfig) -> Arc<LaminarDB>` is sync.
- Lifecycle: `start(self: &Arc)` (clone the Arc), `shutdown(&self)`,
  `checkpoint(&self) -> u64` (async), `is_checkpoint_enabled()`.

## Task 1.0 — Spike: Arrow IPC roundtrip + packed-tarball loading

- [x] Rust `StreamWriter` serialize a batch → `Buffer` → JS `tableFromIPC` (with
      `apache-arrow` as a devDependency) → values verified, both directions. Record exact
      API names at the versions used.
- [x] Verify the generated loader's resolution order for a **packed tarball** install (no
      platform package published yet): confirm the local `<binary>.<platform>.node`
      fallback path the loader checks, and how `scripts/bare-quickstart.mjs` must stage
      the binary. Record here.
- [x] Arrow crate enters `Cargo.toml` pinned `=58.4.0` (match the core),
      `default-features = false, features = ["ipc"]`.

## Task 1.1 — Configuration open

- [x] `open(path?, config?)` (single entry; see §Spike results). Deferred from the
      original list: `deliveryGuarantee` (maps to `LaminarConfig::delivery_guarantee`;
      lands with Phase 2 — nothing exercises it in embedded mode yet) native surface:
      `#[napi(object)]` config with `storageDir`,
      `checkpoint {intervalMs?, timeoutMs?, dataDir?,     maxNodeDataBytes?}`,
      `bufferSize`, `incrementalEmit`, `objectStoreUrl`, `objectStoreOptions`,
      `deliveryGuarantee` — every field maps 1:1 onto a real
      `LaminarConfig`/`StreamCheckpointConfig` field (the Java native-config-handle
      property, structurally).
- [x] `:memory:` sugar: no path or `':memory:'` → in-memory; a path → `storage_dir`. Path
      taken from the argument, `config.storageDir` wins if both are given (document).
- [x] `laminar-core` dependency returns at the same tag (checkpoint config types;
      satisfies machete now).
- [x] Config validation errors use the 100-range codes with precise messages
      (`checkpoint.maxNodeDataBytes` finite/non-negative; added in the phase-exit review
      remediation — `into_core` was infallible before).

## Task 1.2 — Query results

- [x] `query(sql) -> QueryResult`: native class holding
      `SchemaRef +     Vec<RecordBatch>`; `schema() -> FieldInfo[]`, `numRows`,
      `numBatches`, `batch(i) -> ArrowBatch`, `toIPC(): Buffer` (whole result as one IPC
      stream), `toArray()` (row objects).
- [x] `ArrowBatch` class: `numRows`, `numColumns`, `schema()`, `toIPC()`, `toArray()`. One
      allocation per batch serialization.
- [x] `FieldInfo {name, dataType, nullable}` — `dataType` is the Arrow type string (same
      formatting the core's FFI uses, Debug-formatted), documented as informational.
- [x] Non-query SQL passed to `query()` rejects with the core's invalid-operation error
      through the standard mapping.
- [x] `execute()`'s `query` and `metadata` outcomes now carry the collected result object
      instead of dropping it.

## Task 1.3 — Ingestion

- [x] `insert(source, rows: object[])` and `insertArrow(source, ipcBuffer)`: resolve the
      source schema from the catalog, build/import the batch, `push_arrow`. Rows-built
      batches validate per value (see conversion).
- [x] `Writer` class: `writeRows(rows)`, `writeArrow(buffer)`, `watermark(ts)`,
      `currentWatermark()`, `pending()`, `capacity()`, `isBackpressured()`, `schema()`,
      `close()` (idempotent; writes after close reject 301; schema mismatch on write
      rejects 302 with a full field diff).
- [x] `src/conversion.rs`: JS value ⇄ Arrow column value for the Phase-1 type set —
      `Boolean`, `Int8..64`, `UInt8..64`, `Float32/64`, `Utf8`, `LargeUtf8`, `Date32/64`
      (ms since epoch, JS `Date` or number), `Timestamp(_, _)` (ms/µs/s/ns from `Date` or
      number, honoring the unit), `Null`. Anything else rejects with a 300-coded message
      naming the column and type. Numbers coerce to the declared type and reject on loss.
- [x] Nulls: JS `null`/`undefined` → null slot; non-nullable column receiving null rejects
      300 naming the column.

## Task 1.4 — Lifecycle and catalog

- [x] `start()`, `checkpoint()` (rich outcome — see §Spike results),
      `isCheckpointEnabled()` on Connection.
- [x] `listSources()`/listStreams()/listSinks()`(names),`schema(name) ->
      FieldInfo[]`    (sources; unknown name rejects 200),`sourceInfos()` with schemas and
      watermark columns.
- [x] Catalog reads are async fns (catalog locks); `insert`/`insertArrow`/`writer` are
      sync by design — no engine await is involved and it keeps hot ingestion off the
      promise path (documented on the methods) are async fns (they take catalog locks; not
      lock-free status accessors).

## Task 1.5 — TypeScript public layer

- [x] `ts/` compiled by `tsc` into `dist/`; `package.json` `main`/`types` point at
      `dist/`; generated `index.js` stays at the root as the internal loader (shipped, not
      public). `just build` chains `napi build` + `tsc`.
- [x] `LaminarDB.open(path?, config?)` facade; `Connection`, `QueryResult`, `ArrowBatch`,
      `Writer` re-exported with our JSDoc.
- [x] Error hierarchy: `LaminarError` base with `code: number` and `codeName: string`
      parsed from the `[LAMINAR_<n>]` prefix, subclasses per range
      (`LaminarConnectionError` 100s, `LaminarSchemaError` 200s, `LaminarIngestionError`
      300s, `LaminarQueryError` 400s, `LaminarSubscriptionError` 500s,
      `LaminarInternalError` 900s); a mapper wraps every native call, rethrowing with
      cleaned message + `cause`. napi argument-coercion failures wrap into `LaminarError`
      (code 900, `LAMINAR_BINDING` name) per plan 00 §5.
- [x] `apache-arrow` interop helpers: `tableFrom(result|batch)` using the optional peer
      dependency, lazy-imported so the dependency stays optional.

## Task 1.6 — Documentation-as-tests and packaging proof

- [x] `__test__/quickstart.spec.mjs` runs the README quickstart (and the docs/examples
      snippets) verbatim — code blocks extracted or mirrored exactly; drift fails the
      suite.
- [x] `scripts/bare-quickstart.mjs`: `pnpm pack` → temp project → `npm install <tarball>`
      → run the quickstart; wired into CI (Phase 2 adds it to a matrix OS; Phase 1 runs it
      on ubuntu).
- [x] README rewritten for the Phase 1 surface; CHANGELOG entry; `CORE_PIN.md` unchanged
      (same pin).

## Task 1.7 — Review and exit

- [ ] `just review` + `just verify` green; coverage of the new modules asserted by the
      suite (query/insert/writer/config/catalog each have failing-path tests asserting
      codes).
- [ ] Phase-exit review in `docs/reviews/phase1-<date>.md` with zero open REQUEST CHANGES
      findings (independent reviewer pass, same process as phase 0).

## Spike results (executed 2026-09-02, core v0.30.0 / napi 3.12.2 / tsc strict)

- **IPC roundtrip verified end to end** (Task 1.0): `StreamWriter` → `Buffer` →
  `tableFromIPC` → values; reverse direction via `tableToIPC` → `insertArrow`. arrow-rs
  `StreamReader::try_new` takes a second projection argument (`None`).
- **Loader resolution for packed tarballs**: local `laminar_nodejs.<platform>-<arch>.node`
  first, then the platform package. `bare-quickstart` stages the built binary next to the
  installed `index.js`; `NAPI_RS_NATIVE_LIBRARY_PATH` is the supported env escape hatch.
- **napi naming**: snake_case methods camelCase automatically, but `to_ipc` becomes
  `toIpc` — the public name is `toIPC` via `#[napi(js_name)]` on both result types.
- **napi objects cannot hold class instances**: `ExecuteOutcome` changed from
  `#[napi(object)]` to a `#[napi]` class with getters (`kind`, `statementType`, …,
  `result`).
- **napi v3 API facts** (no `compat-mode`): the Js* wrappers are gated; everything goes
  through `napi::bindgen_prelude`; `Object::new(env)` creates bindgen objects; `i64n`
  crosses to a JS `BigInt`. Dates are not readable without chrono — the native seam takes
  temporal input as **milliseconds**, and the TS layer's contract documents that (a
  follow-up to convert `Date` objects in the TS wrappers is a Phase 2 candidate).
- **Manual checkpointing requires real topology** (probe-verified): the coordinator wires
  only when at least one stream/sink exists; a bare source gives
  `LDB-6001 ... call start() first` even after `start()`. Documented on `checkpoint()` and
  in the README.
- **Engine edges at the pin**: topology DDL after `start()` rejects (`LDB-6043`);
  `INSERT INTO ... VALUES` does not support `Timestamp(Microsecond)` literals — use
  `insert()` rows or IPC.
- **macOS `/tmp` is a symlink** and the core's checkpoint-dir check uses
  `symlink_metadata`; tests resolve `os.tmpdir()` through `realpathSync`.
- **checkpoint() returns a rich outcome** (`success`/`checkpointId`/`epoch`/
  `durationMs`/`error`), not a bare id — exposed as a `CheckpointOutcome` object.
- **ESLint deferred to Phase 2**: `tsc --strict` + prettier cover the current TS volume;
  linting joins when the TS layer grows (subscriptions).

## Design notes

- **Why collected results, not streaming, for Phase 1**: streaming query output and
  subscriptions share the framed-async-iterator machinery — both land in Phase 2 (plan 03)
  once the TS layer exists to host `Symbol.asyncIterator`. Phase 1's `query()` collects,
  matching the Java/Python MVP scope.
- **Buffer ownership**: IPC bytes are copied once into a napi-owned `Buffer`; the Rust
  batch is dropped independently. No zero-copy lifetime coupling in Phase 1 (D6).
- **Divergences from `api::Writer`**: we check full schema equality (name+type) rather
  than field count, and skip the no-op `flush()`. Both recorded here as deliberate.
