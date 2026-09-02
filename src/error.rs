//! Error contract: core `DbError` → napi error with a machine-readable code.
//!
//! Every native failure — synchronous throw or promise rejection — surfaces as
//! a JS `Error` whose message is `[LAMINAR_<code>] <core message>`. The
//! TypeScript layer (plan 00 D8) parses the prefix and rethrows the typed
//! `LaminarError` hierarchy carrying `error.code`; the prefix itself is
//! internal plumbing, not public API.
//!
//! WHY prefix-in-message: napi-rs 3.12 supports custom `error.code` strings
//! only on synchronous throws; promise rejections convert through
//! `Into<Error<Status>>` and drop a custom status (verified against
//! napi-3.12.2 `tokio_runtime.rs` and napi-derive-3.6.3 codegen). The uniform
//! message contract works on every path today and migrates to a native code
//! property in exactly two places (this module and the TS error mapper) when
//! napi-rs grows support.

use laminar_db::DbError;
use napi::Error;

/// Map a core `DbError` through the `api::ApiError` taxonomy onto the JS contract.
pub fn map_db_error(error: DbError) -> Error {
    let api_error = laminar_db::api::ApiError::from(error);
    coded_error(api_error.code(), api_error.message())
}

/// Use-after-close: core code 101 (`CONNECTION_CLOSED`).
pub fn connection_closed_error() -> Error {
    coded_error(101, "connection is closed")
}

/// Build the uniform `[LAMINAR_<code>] <message>` failure.
pub fn coded_error(code: i32, message: &str) -> Error {
    Error::from_reason(format!("[LAMINAR_{code}] {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_error_carries_prefixed_code_and_message() {
        let error = map_db_error(DbError::InvalidOperation("boom".to_owned()));
        assert!(error.reason.starts_with("[LAMINAR_"));
        assert!(error.reason.ends_with("boom"));
    }

    #[test]
    fn closed_error_carries_connection_code() {
        assert_eq!(
            connection_closed_error().reason,
            "[LAMINAR_101] connection is closed"
        );
    }

    #[test]
    fn coded_error_formats_the_contract() {
        assert_eq!(coded_error(401, "bad sql").reason, "[LAMINAR_401] bad sql");
    }
}
