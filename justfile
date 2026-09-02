# Build orchestration for laminardb-nodejs. `napi build` (cargo underneath)
# produces the addon binary plus generated index.js/index.d.ts; pnpm never
# invokes cargo — just owns the wiring (mirrors laminardb-java's
# "Maven never invokes cargo" rule, plan 00 §2).

default:
    @just --list

# Debug build: addon + generated loader + TypeScript layer (dist/).
build:
    pnpm exec napi build --platform
    pnpm exec tsc

# Release build: addon + generated loader + TypeScript layer (dist/).
build-release:
    pnpm exec napi build --platform --release
    pnpm exec tsc

# Build + run the vitest suite against the built addon.
test: build
    pnpm test

# Correctness gate: fmt + clippy + Rust unit tests + vitest.
# Rust tests run against napi's no-op backend (test-only feature) so the
# harness links without a Node host providing the Node-API symbols.
verify:
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings
    cargo test --features napi-noop
    just build
    pnpm test

# Review gate: verify tooling for the current phase — fmt, clippy, machete,
# the allows-grep, and prettier over authored JS/TS/MD/JSON.
review:
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings
    cargo machete
    just allows-grep
    pnpm run format:check

# Every `#[allow(...)]` in src/ — plain or inside `cfg_attr` — must carry an
# inline `WHY:` justification.
allows-grep:
    @! grep -rnE '#\[(cfg_attr\([^)]*,\s*)?allow\(' src/ | grep -v 'WHY:'

# Cold-consumer proof: pack the tarball, install into a throwaway project,
# stage the local binary, run the README quickstart (plan 02 Task 1.6).
bare-quickstart: build
    node scripts/bare-quickstart.mjs

# Reinstall tool dependencies after a lockfile change.
install:
    pnpm install

clean:
    cargo clean
    rm -rf node_modules *.node dist
