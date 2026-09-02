# laminardb-nodejs — agent context

Node.js binding for LaminarDB embedded mode. npm package `@laminardb/node`. Sibling of
`laminardb-java` and `laminardb-python`; the main repository (`laminardb/laminardb`) is
never modified from here.

## Architecture (two layers)

1. **Rust cdylib** (`src/*.rs`, napi-rs v3) — binds the pinned core (`laminar-db`, cargo
   feature `api`, exact git tag in `Cargo.toml`) by driving its **async** methods on the
   napi tokio runtime (plan 00 D2). Owns handle lifetimes, the error contract
   (`[LAMINAR_<n>] <message>` prefix on every failure, codes from `api::ApiError`), and
   Arrow IPC serialization (Phase 1+).
2. **TypeScript layer** (`ts/`, Phase 1) — the public API: error classes, async iterators,
   `apache-arrow` interop. The generated `index.js`/`index.d.ts` are an internal seam,
   committed but not public API.

## Invariants

- **Pin rule**: the binding ships exactly one core git tag (never a branch); `Cargo.toml`
  tags == `CORE_PIN.md` newest row == `src/lib.rs:: CORE_PIN_TAG`. Bumping the pin is its
  own PR.
- **Async-first**: no napi method may block the JS thread. Data-plane calls are async fns;
  only lock-free status accessors are sync.
- **Sync `api::Connection` is off-limits** — at the pin its blocking paths reject runtime
  context or spawn per-call threads. Use the async core methods (plan 00 §6 appendix lists
  them).
- **Errors**: every failure maps to a coded rejection (message prefix `[LAMINAR_<n>]`;
  napi-rs 3.12 drops custom codes on promise rejections — see plan 01 spike results); no
  `unwrap`/`expect`/panic on user, network, storage, or config data.
- **Lifecycles**: `close()` is idempotent everywhere; use-after-close rejects with
  `LAMINAR_101`; GC finalizers are leak backstops only.
- **Crossings are batch-level** — never per row across the JS boundary.

## Build and review gates

```sh
just install   # pnpm install
just build     # napi build --platform (debug) + regenerate index.js/index.d.ts
just test      # build + vitest suite against the real addon
just verify    # cargo fmt --check, clippy --all-targets -D warnings, cargo test, vitest
just review    # verify tooling + cargo machete + allows-grep + prettier --check
```

Every `#[allow(...)]` needs an inline `WHY:`. Conventional Commits; no AI/assistant
attribution, no `Co-Authored-By` trailers, no tool-session metadata.

## Plans and authority

`docs/plans/00-overview-and-decisions.md` holds the decision records and the phase map;
`01`–`05` are the phase plans; `docs/reviews/` holds phase-exit reviews. The pinned core
tag's source is authoritative — when a plan and the pin disagree, fix the plan in the same
PR. `laminardb-java`'s docs (`docs/plans/00-overview-and-decisions.md` there) are the
closest precedent; read ours first.
