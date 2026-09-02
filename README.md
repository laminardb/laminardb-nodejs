# laminardb-nodejs

Embedded streaming SQL for Node.js. This is the official Node.js binding for
[LaminarDB](https://github.com/laminardb/laminardb), built as a [napi-rs](https://napi.rs)
native addon over a pinned core release — the same two-layer shape as
[`laminardb-java`](https://github.com/laminardb/laminardb-java) and `laminardb-python`,
adapted to Node idioms: every data-plane call returns a `Promise`, subscriptions will be
async iterables, and Arrow data moves as IPC `Buffer`s.

Status: **alpha — Phase 0** (scaffold; see `docs/plans/`). The current surface is
open/execute/close and the error contract; query results, ingestion, subscriptions, and
npm distribution land over the next phases. Not yet on npm — build from source.

## Quickstart (current surface)

```js
import { open, version } from '@laminardb/node'

const conn = await open()
await conn.execute('CREATE SOURCE sensors (ts TIMESTAMP, device VARCHAR, value DOUBLE)')
console.log(version())
await conn.close()
```

Every failing native call rejects with an `Error` whose message carries a `[LAMINAR_<n>]`
code prefix from the core's error taxonomy (100–199 connection, 200–299 schema, 300–399
ingestion, 400–499 query, 500–599 subscription, 900–999 internal). The typed error-class
layer (which re-exposes the code as `error.code`) ships with Phase 1.

## Build from source

Requires Rust stable (≥ 1.95) and Node ≥ 20 with pnpm.

```sh
just install   # pnpm install (@napi-rs/cli, vitest, prettier)
just build     # debug addon + generated index.js / index.d.ts
just test      # vitest suite against the built addon
just verify    # fmt + clippy -D warnings + rust tests + build + vitest
```

The first build clones and compiles the pinned LaminarDB core (git tag in `Cargo.toml`,
registry in `CORE_PIN.md`) — expect a long cold build.

## Platform support

The release matrix (Phase 3) covers macOS x64/arm64, Linux x64/arm64 (glibc and musl), and
Windows x64, distributed as per-platform optional npm packages with no postinstall and no
source compilation. Until then, `napi build` works anywhere the toolchain does.

## Documentation

- `docs/plans/` — decision records and phase plans (start at
  `00-overview-and-decisions.md`)
- `CORE_PIN.md` — which core release each binding version ships
- `CHANGELOG.md`

Apache-2.0, like the core.
