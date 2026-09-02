# Plan 01 — Phase 0: repo scaffold, build wiring, CI

Status: **Executing (2026-09-02)** · Prerequisite: plan 00 (decisions D1–D9) Exit:
`just verify` builds the addon and runs the vitest smoke suite green in CI on Linux x86_64
and macOS (arm64 and x86_64).

## Goal

A skeleton repository where a napi-rs cdylib over the pinned core and a minimal generated
JS surface build and test together with one command, exercised by CI. No npm publishing
(plan 04); no subscriptions (plan 03). The point is to de-risk the integration seams —
cargo↔pnpm via `just`, napi↔core-async (D2), and the error contract — while the surface is
tiny.

## Task 0.1 — Repository bootstrap

- [x] GitHub remote `laminardb/laminardb-nodejs` (HTTPS, like the Java repo's convention);
      this checkout is the repository root; `docs/plans/` is the living backlog.
- [x] `LICENSE` — Apache-2.0 (copied from the core repo).
- [x] `.gitignore` — `/target`, `node_modules/`, `*.node`, editor noise. Generated
      `index.js`/`index.d.ts` **are committed** (they ship in the npm package;
      generated-but-shipped).
- [x] `rust-toolchain.toml` — mirror the core: `channel = "stable"`, components `rustfmt`,
      `clippy`.
- [x] `AGENTS.md` — operating context for coding agents in this repo.
- [x] `justfile` — `build`/`build-release`/`test`/`verify`/`review`/
      `allows-grep`/`install`/`clean`. Orchestration rule: pnpm never invokes cargo;
      `just` owns the wiring (the Java repo's rule with the tools swapped).

## Task 0.2 — Rust crate scaffold

- [x] `Cargo.toml` — cdylib `laminar_nodejs`; `napi` v3
      (`default-features = false, features = ["napi8", "tokio_rt", "async"]`),
      `napi-derive` v3, `napi-build` v2; core pin per D4:
      `laminar-db = { git = ".../laminardb", tag = "v0.30.0", default-features = false, features = ["api"] }`
      — `laminar-core` (same tag, config types) is deferred to Phase 1: nothing references
      it yet and it would fail `cargo machete`. Release profile: `lto`,
      `codegen-units = 1`, `strip`.
- [x] `src/lib.rs` — module root, `CORE_PIN_TAG` const (INVARIANT: equals the Cargo git
      tags; unit test asserts tag and package version share the core's
      `major.minor.patch`), `version()` → `"<binding> (core <tag>)"`; Rust unit tests live
      inline per the core's layout rules.
- [x] `src/error.rs` — the single mapping point (D2): `DbError` → `api::ApiError` (numeric
      code + message) → `napi::Error` whose message is `[LAMINAR_<n>] <core message>`. The
      Phase 1 TS error mapper parses the prefix and rethrows the typed `LaminarError`
      hierarchy carrying `error.code`. (Correction from the original sketch: a napi custom
      status on `error.code` survives synchronous throws only — promise rejections convert
      through `Into<Error<Status>>` and drop it; verified against napi-3.12.2 source. See
      §Spike results.)
- [x] Test-only cargo feature `napi-noop` (aliases `napi/noop`): `cargo test` links
      without a Node host providing the Node-API symbols; `just verify` and CI use
      `cargo test --features napi-noop`.
- [x] `src/database.rs` — `#[napi]` free `open()` (async fn; the sync core constructor
      runs on the napi runtime, never the JS thread) returning the `Connection` class
      holding `Arc<LaminarDB> + AtomicBool`; methods `execute` (async, returns
      `ExecuteOutcome{kind, statementType,     objectName, rowsAffected, queryId}` —
      query/metadata payloads dropped until Phase 1), `isClosed` (sync status accessor per
      D2), `close` (idempotent via `AtomicBool::swap`; first caller runs graceful
      `shutdown()`).

## Task 0.3 — Node package scaffold

- [x] `package.json` — `@laminardb/node`; `main: index.js`, `types:     index.d.ts`;
      `files: [index.js, index.d.ts]`; `napi` block with `binaryName: laminar_nodejs` and
      the 7 release targets (D9 — the platform matrix drives CI in later phases); scripts
      `build`, `build:debug`, `artifacts`, `prepublishOnly` (`napi prepublish`), `test`
      (vitest), `format`/`format:check` (prettier). `engines.node:     >= 20`.
- [x] Dev deps: `@napi-rs/cli` ^3.8, `vitest` ^4, `prettier` ^3. pnpm (dev-only; consumers
      need nothing but npm).
- [x] `.prettierrc`/`.prettierignore` — prettier checks authored JS/TS/MD/JSON; generated
      `index.js`/`index.d.ts` and lockfiles ignored.
- [x] `vitest.config.mjs` — 30 s test timeout (engine open/shutdown deadlines), otherwise
      defaults.

## Task 0.4 — Build wiring

- [x] `just build` = `napi build --platform` (debug; regenerates `index.js`/`index.d.ts`
      and `laminar_nodejs.<platform>.node` at the repo root).
- [x] `just test` = build + `vitest run`; `just verify` = `cargo fmt --check` +
      `clippy --all-targets -D warnings` + `cargo test --features napi-noop` + build +
      vitest; `just review` is the separate lint/tooling gate — `cargo fmt --check`,
      `clippy`, `cargo machete`, the allows-grep, and prettier (no tests, no build).

## Task 0.5 — Spike: async-first seam (de-risks D2 before Phase 1)

- [x] `#[napi] async fn` + `#[napi] impl` class with `Arc<LaminarDB>` state — compiles and
      runs on the napi tokio runtime.
- [x] Coded error contract — every failure (sync or async) surfaces as
      `[LAMINAR_<n>] <message>` on the JS side.
- [x] `.d.ts` generation for classes/objects/free functions.
- [ ] (Phase 1 Task 1.0, recorded here as the forward pointer) Arrow IPC roundtrip spike
      before designing the query data path — Rust serialize → `Buffer` → JS `tableFromIPC`
      → values verified; the zero-copy C Data Interface comparison happens in Phase 2
      behind a benchmark gate.

## Task 0.6 — CI

- [x] `.github/workflows/ci.yml`, matrix `ubuntu-latest` and `macos-latest`: jobs
      `rust-lint` (fmt + clippy `-D warnings`), `review` (the same commands as
      `just review`, inlined — CI does not install `just`; machete via
      `taiki-e/install-action`), `verify` (Node 22, `pnpm install`, the `just verify`
      command set inlined). Windows/musl runners join in Phase 2 (plan 03) per the phase
      map.
- [x] The `api`-only feature set needs no system packages beyond Rust and a C/C++
      toolchain (established by the Java repo at the same pin).

## Task 0.7 — Smoke tests (vitest, against the real addon)

- [x] open → `isClosed() === false` → `CREATE SOURCE` returns
      `{kind: 'ddl', statementType, objectName}` → close → `isClosed() ===     true`.
- [x] Double `close()` is a no-op; `execute` after close rejects with code `LAMINAR_101`.
- [x] Duplicate `CREATE SOURCE` → `LAMINAR_400` (SQL-layer code at the pin — the Java
      repo's finding, not 201); invalid SQL → `LAMINAR_401`.
- [x] `version()` string matches binding + pinned core.
- [x] Open/close loop 200× without crashing (leak soak arrives properly in Phase 2).

## Acceptance checklist (Phase 0 exit)

- [x] `just verify` green locally (macOS arm64; 4 Rust unit tests + 7 vitest tests).
- [ ] CI green on both matrix OSes from a clean checkout (proves the pinned git-tag core
      dep resolves without sibling clones). Open: awaits the first push to
      `laminardb/laminardb-nodejs`.
- [x] `just review` green locally (fmt, clippy `-D warnings`, machete, allows-grep,
      prettier); `AGENTS.md` committed; plan checkboxes updated.
- [ ] Phase-exit review recorded in `docs/reviews/phase0-<date>.md` with zero open REQUEST
      CHANGES findings. Open: run after CI is green.
- [ ] Conventional commit history; no AI/assistant attribution. (Commits land with this
      plan; checked off in the review pass.)

## Spike results (core v0.30.0 / napi 3.12.2 / napi-derive 3.6.3 / @napi-rs/cli 3.8.6)

- **Custom `error.code` is sync-only in napi-rs 3.12** — the original design put
  `LAMINAR_<n>` on `error.code` via a custom `napi::Error<LaminarStatus>` status. It
  compiles and works for synchronous napi fns (the derive generates
  `JsError::from(err).throw_into(env)`, which passes `status.as_ref()` as the code
  string), but **async fns reject that return type**: the derive's async resolver and
  `execute_future_impl` require `Result<T, Error<Status>>`-shaped conversions
  (`ToNapiValue` on the whole result / `Into<Error<Status>>` on the rejection), so a
  custom status cannot cross a promise rejection — the code would be dropped to
  `GenericFailure`. **Adopted instead**: the uniform `[LAMINAR_<n>] <message>` prefix in
  `error.message` on every path, parsed by the Phase 1 TS error mapper into typed classes
  with real `.code` properties (the native seam stays internal per D8). When napi-rs grows
  custom codes on rejections, the migration touches exactly `src/error.rs` and the TS
  mapper.
- **`cargo test` needs the noop backend** — a napi cdylib's test harness links the crate
  as an rlib and the Node-API extern symbols (`_napi_reference_unref`, …) are unresolved
  without a Node host. The test-only `napi-noop` cargo feature aliases napi's `noop`
  feature to stub them; never enabled in shipped builds.
- **The pinned git-tag core dep resolves and builds from a clean checkout** on macOS arm64
  (first full build ≈ 8 minutes; `api`-only features, no system packages).
- **Async-first seam (D2) works end to end**: `open()` → `Connection` class holding
  `Arc<LaminarDB>`, async `execute`/`close` polled on the napi tokio runtime, sync
  `isClosed` status accessor; the 7-test vitest suite including a 200× open/close loop
  completes in well under a second.
- **Observed codes at the pin match the Java repo's findings**: duplicate `CREATE SOURCE`
  → `[LAMINAR_400]` (SQL layer, not 201); invalid SQL → `[LAMINAR_401]`; use-after-close →
  `[LAMINAR_101]`.
