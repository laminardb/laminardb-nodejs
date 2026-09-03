//! Node.js binding for LaminarDB embedded streaming SQL (napi-rs).
//!
//! Two layers, mirroring `laminardb-java` (plan 00 §2): this cdylib binds the
//! pinned core's async API and owns handle lifetimes and the error contract;
//! the TypeScript layer (`ts/`, Phase 1) is the friendly public surface.
//! Phase 1 surface: open (+config), Connection::{execute, query, insert,
//! writer, lifecycle, catalog}, Arrow IPC data movement.

mod arrow_ipc;
mod config;
mod conversion;
mod database;
mod error;
mod ingestion;
mod query;
mod subscription;
mod telemetry;

use napi_derive::napi;

/// Spawn onto the module's Tokio runtime. napi's own `spawn` helper is
/// compiled out under the noop test backend; the shim keeps test builds
/// linking (it is never called there — unit tests never start readers).
#[cfg(not(feature = "napi-noop"))]
pub(crate) use napi::bindgen_prelude::spawn;
#[cfg(feature = "napi-noop")]
pub(crate) fn spawn<F>(fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(fut)
}

/// Pinned core release this binding is built against.
///
/// INVARIANT: must equal the `laminar-db`/`laminar-core` git tags in
/// `Cargo.toml` and the newest row of `CORE_PIN.md`; the release gate (plan 04)
/// checks all three agree.
pub const CORE_PIN_TAG: &str = "v0.30.0";

/// Binding and pinned-core version, e.g. `"0.30.0-alpha.1 (core v0.30.0)"`.
#[napi]
pub fn version() -> String {
    format!("{} (core {})", env!("CARGO_PKG_VERSION"), CORE_PIN_TAG)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_tag_matches_core_version_scheme() {
        // The tag and the package version share the core's major.minor.patch;
        // this catches accidental core bumps that skip CORE_PIN.md.
        let core = CORE_PIN_TAG.strip_prefix('v').expect("tag is v-prefixed");
        let binding = env!("CARGO_PKG_VERSION")
            .split('-')
            .next()
            .expect("version");
        assert_eq!(core, binding);
    }
}
