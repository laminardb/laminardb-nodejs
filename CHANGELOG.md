# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the version scheme tracks the
pinned core (plan 00 D4, `CORE_PIN.md`).

## [Unreleased]

### Added — Phase 1: embedded MVP

- Configuration-based open: `LaminarDB.open(path?, config?)` with `:memory:` sugar,
  `storageDir`, `checkpoint` options (manual + interval), buffer size, incremental emit,
  object-store settings.
- Query results: `Connection.query()`/`execute()` with fully collected results —
  `schema()`, `numRows()`, `batch(i)`, `toIPC()` (Arrow IPC stream `Buffer`), `toArray()`
  (row objects; temporal columns as epoch milliseconds, 64-bit ints as `BigInt`).
- Ingestion: `insert(source, rows)` with strict per-value validation,
  `insertArrow(source, ipcBuffer)`, and the `Writer` streaming class with watermark and
  backpressure visibility.
- Lifecycle and catalog: `start()`, `checkpoint()` (outcome with id/epoch),
  `isCheckpointEnabled()`, `listSources/Streams/Sinks()`, `sourceInfos()`, `schema(name)`.
- TypeScript public layer (`dist/`): `LaminarDB`/`Connection`/`Writer` facades, the
  `LaminarError` hierarchy with `error.code` parsed from the native `[LAMINAR_<n>]`
  contract, and `tableFrom()` interop with the optional `apache-arrow` peer dependency.
- Documentation-as-tests (README quickstart verbatim), a cold-consumer `bare-quickstart`
  proof against `npm pack`, and the phase-1 plan record.

### Added — Phase 0

- Phase 0 scaffold: napi-rs v3 cdylib binding the pinned core (`v0.30.0`, `api` feature)
  with async-first surface driven on the napi tokio runtime.
- Native surface: `open()`, `Connection.execute()`, `Connection.isClosed()`,
  `Connection.close()`, `version()`.
- Error contract: core `DbError` mapped through the `api::ApiError` taxonomy to JS errors
  whose message carries a `[LAMINAR_<n>]` code prefix (typed classes exposing `error.code`
  arrive with the Phase 1 TypeScript layer).
- vitest smoke suite, `just` orchestration (`build`/`test`/`verify`/`review`), CI on Linux
  and macOS, decision records and phase plans under `docs/plans/`.
