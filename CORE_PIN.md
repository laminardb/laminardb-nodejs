# Core pin registry

Every release of this binding ships exactly one pinned LaminarDB core release. The pin is
an exact git tag on the `laminar-db` and `laminar-core` entries in `Cargo.toml` — never a
branch, never `main` (plan 00, decision D4).

| Binding version | Core tag | Date       | Notes            |
| --------------- | -------- | ---------- | ---------------- |
| 0.30.0-alpha.1  | v0.30.0  | 2026-09-02 | Phase 0 scaffold |

## Rules

- The binding version tracks the core version (binding `0.31.0` ships core `v0.31.0`);
  binding-only fixes bump the patch or pre-release segment.
- The release gate validates: git tag == `Cargo.toml` version == `package.json` version ==
  pinned core tag == `CHANGELOG.md` entry.
- `src/lib.rs::CORE_PIN_TAG` must equal the tag in `Cargo.toml`.
- Bumping the pin is its own PR that updates this table, `Cargo.toml`, `src/lib.rs`, and
  `CHANGELOG.md` together.
