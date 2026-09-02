# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the version scheme tracks the
pinned core (plan 00 D4, `CORE_PIN.md`).

## [Unreleased]

### Added

- Phase 0 scaffold: napi-rs v3 cdylib binding the pinned core (`v0.30.0`, `api` feature)
  with async-first surface driven on the napi tokio runtime.
- Native surface: `open()`, `Connection.execute()`, `Connection.isClosed()`,
  `Connection.close()`, `version()`.
- Error contract: core `DbError` mapped through the `api::ApiError` taxonomy to JS errors
  whose message carries a `[LAMINAR_<n>]` code prefix (typed classes exposing `error.code`
  arrive with the Phase 1 TypeScript layer).
- vitest smoke suite, `just` orchestration (`build`/`test`/`verify`/`review`), CI on Linux
  and macOS, decision records and phase plans under `docs/plans/`.
