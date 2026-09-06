# Plan 04 — Phase 3: npm release engineering

Status: **Complete-pending-operations (2026-09-03)** · Prerequisites: plans 00–03 exited
Exit: a tag push produces, validates, publishes, and verifies the full 7-platform release:
`@laminardb/node` plus six platform packages, installable on every matrix platform. The
**first real publish** additionally requires operational secrets the repository cannot
provide itself (see §Prerequisites).

## Prerequisites (human/operational, before first publish)

- [ ] npm: the `@laminardb` org exists; a granular publish token (packages: read-write for
      `@laminardb/*`) is stored as the `NPM_TOKEN` secret.
- [ ] Repository: `NPM_TOKEN` secret configured; tags protected (no force pushes).
- [ ] Branch protection on `main`: PR + green CI (already in place).

Without these, the workflow's `build` and `assemble` stages still run on any tag push
(`workflow_dispatch` offers a dry-run that stops before publish) — the release path is
testable end to end minus the registry write.

## Version and pin gates (decision D4)

- The release tag `v<X>` must equal: `Cargo.toml` version == `package.json` version ==
  every `npm/*/package.json` version == the pinned core tag in `Cargo.toml` (`v<X>`, same
  `major.minor.patch`) == `src/lib.rs::CORE_PIN_TAG` == a `CHANGELOG.md` entry for `v<X>`.
  The `validate` job enforces all of this before anything builds.
- Binding-only fixes bump the patch (and re-pin nothing); core bumps are their own PR
  touching `CORE_PIN.md`, `Cargo.toml`, `src/lib.rs`, and `CHANGELOG.md` together.

## Task 3.1 — Platform packages

- [x] `napi create-npm-dirs`: committed `npm/<platform>/package.json` for `darwin-x64`,
      `darwin-arm64`, `linux-x64-gnu`, `linux-x64-musl`, `linux-arm64-gnu`,
      `linux-arm64-musl`, `win32-x64-msvc` (`@laminardb/node-<platform>`, `cpu`/`os`
      restricted, `files` = the `.node` binary).
- [x] `napi prepublish -t npm` (the `prepublishOnly` script) injects the version-matched
      `optionalDependencies` into `package.json` and copies artifacts into the platform
      dirs at publish time; humans never run `npm publish` directly.

## Task 3.2 — Release workflow (`.github/workflows/release.yml`)

- [ ] Trigger: tag push `v*` (plus `workflow_dispatch` with a `dry-run` input that stops
      before publishing).
- [x] `validate` job: the full gate table above (the `just review` tooling already gates
      every push in ci.yml; the release gate adds the parity table).
- [x] `build` matrix (one job per target). As built (deviating from the original sketch
      above): arm64-linux targets build on **macOS hosts via cargo-zigbuild** — the public
      Linux fleets reclaimed every long aarch64-linux build with runner shutdowns (seven
      kills across native and cross hosts); `x86_64` macOS cross-compiles from
      `macos-latest` as sketched: - `darwin-arm64` / `darwin-x64`: native runners
      (`macos-latest`, `macos-15-intel` or cross from arm64 — x86_64 macOS runners are
      retired; cross-compile with `rustup target add` + `napi build       --target` and a
      linker env, matching the Java repo's approach). - `linux-x64-gnu` /
      `linux-arm64-gnu`: `ubuntu-latest` + `ubuntu-24.04-arm` native runners. -
      `linux-x64-musl` / `linux-arm64-musl`: `cargo-zigbuild` (zig supplies the musl cross
      C toolchain the Phase 2 check job defers). - `win32-x64-msvc`: `windows-latest`.
      Each job uploads the `.node` artifact + `sha256sums`.
- [x] `assemble-and-test` job (ubuntu + macos + windows): run `napi artifacts` into
      `npm/`, `pnpm pack`, install the tarball into a throwaway project (the
      bare-quickstart script with the platform package staged from the local `npm/` dir),
      run the quickstart.
- [x] `publish` job (needs all of the above, environment `npm`):
      `napi prepublish -t npm --tag alpha` (the pre-1.0 channel; move to `latest` at 1.0)
      — publishes the main package and the six platform packages with `NPM_TOKEN`.
- [x] `verify-publish` job: poll `npm view @laminardb/node@<version>` until resolvable,
      then install it into a scratch project on the runner and print `version()`.
- [x] `github-release`: upload checksums + the bench output from the triggering commit's
      nightly if present.

## Task 3.3 — Repository polish for publishing

- [x] `files` whitelist: `index.js` (generated loader) + `dist/`; the generated root
      `index.d.ts` stays internal (public types are `dist/index.d.ts`).
- [x] `package.json`: `publishConfig.access` public; engines `>= 20`.
- [ ] README platform-support section updated to reflect shipped natives (first release).

## Task 3.4 — Review and exit

- [ ] Dry-run of the workflow on a scratch tag proves build + assemble + tests green on
      all seven targets (publish skipped).
- [ ] Phase-exit review in `docs/reviews/phase3-<date>.md`, zero open REQUEST CHANGES
      findings.

## Design notes

- **Channel**: pre-1.0 publishes go to the `alpha` dist-tag (the Java repo's Phase-1
  channel convention); `latest` is reserved for the first stable.
- **No postinstall, ever**: platform resolution is npm's `optionalDependencies` mechanism;
  an unsupported platform fails at require time with the loader's clear error (D9).
- **npm lockfile quirk**: npm ≤10 has known optional-dependency resolution bugs; consumers
  on npm are advised ≥ 11 (pnpm/yarn unaffected) — noted in the README's install section
  at first release.
