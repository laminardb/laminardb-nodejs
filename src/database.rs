//! The `Connection` class: one embedded `LaminarDB` instance per JS object.
//!
//! Phase 0 surface (plan 01 Task 0.4): `open`, `execute`, `isClosed`,
//! `close`, and the `ExecuteOutcome` object. Query data movement (Arrow IPC)
//! lands in Phase 1 (plan 02); subscriptions in Phase 2 (plan 03).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use laminar_db::{ExecuteResult, LaminarDB};
use napi::Result;
use napi_derive::napi;

use crate::error::{connection_closed_error, map_db_error};

/// One executed statement's outcome, discriminated by `kind`:
/// `ddl` | `rows-affected` | `query` | `metadata`.
///
/// Phase 0 drops the payload for `query` and `metadata` results (collected
/// rows arrive with `query()` in Phase 1); `ddl` and `rows-affected` carry
/// their full information.
#[napi(object)]
pub struct ExecuteOutcome {
    pub kind: String,
    pub statement_type: Option<String>,
    pub object_name: Option<String>,
    pub rows_affected: Option<i64>,
    pub query_id: Option<i64>,
}

#[napi]
pub struct Connection {
    db: Arc<LaminarDB>,
    closed: AtomicBool,
}

#[napi]
impl Connection {
    /// Execute one SQL statement. DDL and `INSERT INTO` return their outcome
    /// directly; a `SELECT` returns `kind: "query"` (results via `query()`,
    /// Phase 1). Fails with `LAMINAR_101` after `close()`.
    #[napi]
    pub async fn execute(&self, sql: String) -> Result<ExecuteOutcome> {
        if self.is_closed() {
            return Err(connection_closed_error());
        }
        let result = self.db.execute(&sql).await.map_err(map_db_error)?;
        Ok(match result {
            ExecuteResult::Ddl(info) => ExecuteOutcome {
                kind: "ddl".to_owned(),
                statement_type: Some(info.statement_type),
                object_name: Some(info.object_name),
                rows_affected: None,
                query_id: None,
            },
            ExecuteResult::RowsAffected(rows) => ExecuteOutcome {
                kind: "rows-affected".to_owned(),
                statement_type: None,
                object_name: None,
                rows_affected: Some(saturating_i64(rows)),
                query_id: None,
            },
            ExecuteResult::Query(handle) => ExecuteOutcome {
                kind: "query".to_owned(),
                statement_type: None,
                object_name: None,
                rows_affected: None,
                query_id: Some(saturating_i64(handle.id())),
            },
            ExecuteResult::Metadata(_) => ExecuteOutcome {
                kind: "metadata".to_owned(),
                statement_type: None,
                object_name: None,
                rows_affected: None,
                query_id: None,
            },
        })
    }

    #[napi]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Close the connection: graceful engine shutdown, idempotent, and safe
    /// under concurrent calls — the first caller performs the shutdown, later
    /// calls are no-ops. Using the connection after `close()` fails with
    /// `LAMINAR_101`. The core's shutdown has an internal 45 s deadline, so a
    /// close can reject on timeout rather than hang forever.
    #[napi]
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.db.shutdown().await.map_err(map_db_error)
    }
}

/// `u64 → i64` for the JS number contract without `unwrap` on foreign sizes.
fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Open an in-memory embedded database with core-default configuration.
///
/// Phase 0 opens with defaults only; `openWithConfig` (storage directory,
/// checkpoint options) arrives in Phase 1 (plan 02 §2).
#[cfg_attr(test, allow(dead_code))] // WHY: test builds strip napi registration
#[napi]
pub async fn open() -> Result<Connection> {
    let db = LaminarDB::open().map_err(map_db_error)?;
    Ok(Connection {
        db,
        closed: AtomicBool::new(false),
    })
}
