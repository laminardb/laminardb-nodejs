//! The `Connection` class: one embedded `LaminarDB` instance per JS object.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use laminar_db::{ExecuteResult, LaminarConfig, LaminarDB};
use napi::bindgen_prelude::{Buffer, Object};
use napi::Result;
use napi_derive::napi;

use crate::config::OpenConfig;
use crate::error::{coded_error, connection_closed_error, map_db_error};
use crate::ingestion::{self, Writer};
use crate::query::{field_infos, FieldInfo, QueryResult};

/// One executed statement's outcome, discriminated by the `kind` getter:
/// `ddl` | `rows-affected` | `query` | `metadata`.
///
/// `query` and `metadata` outcomes carry the fully collected `result()`;
/// `ddl` and `rows-affected` carry their statement information.
#[napi]
pub struct ExecuteOutcome {
    kind: String,
    statement_type: Option<String>,
    object_name: Option<String>,
    rows_affected: Option<u64>,
    query_id: Option<u64>,
    result: Option<QueryResult>,
}

impl ExecuteOutcome {
    fn ddl(statement_type: String, object_name: String) -> Self {
        Self {
            kind: "ddl".to_owned(),
            statement_type: Some(statement_type),
            object_name: Some(object_name),
            rows_affected: None,
            query_id: None,
            result: None,
        }
    }

    fn of_rows_affected(rows: u64) -> Self {
        Self {
            kind: "rows-affected".to_owned(),
            statement_type: None,
            object_name: None,
            rows_affected: Some(rows),
            query_id: None,
            result: None,
        }
    }

    fn of_query(query_id: u64, result: QueryResult) -> Self {
        Self {
            kind: "query".to_owned(),
            statement_type: None,
            object_name: None,
            rows_affected: None,
            query_id: Some(query_id),
            result: Some(result),
        }
    }

    fn of_metadata(result: QueryResult) -> Self {
        Self {
            kind: "metadata".to_owned(),
            statement_type: None,
            object_name: None,
            rows_affected: None,
            query_id: None,
            result: Some(result),
        }
    }
}

#[napi]
impl ExecuteOutcome {
    #[napi(getter)]
    pub fn kind(&self) -> String {
        self.kind.clone()
    }

    /// DDL statement type (e.g. `"CREATE SOURCE"`); `undefined` otherwise.
    #[napi(getter)]
    pub fn statement_type(&self) -> Option<String> {
        self.statement_type.clone()
    }

    /// DDL object name; `undefined` otherwise.
    #[napi(getter)]
    pub fn object_name(&self) -> Option<String> {
        self.object_name.clone()
    }

    #[napi(getter)]
    pub fn rows_affected(&self) -> Option<i64> {
        self.rows_affected.map(saturating_i64)
    }

    #[napi(getter)]
    pub fn query_id(&self) -> Option<i64> {
        self.query_id.map(saturating_i64)
    }

    /// The collected result for `query`/`metadata` outcomes; `undefined`
    /// for `ddl`/`rows-affected`.
    #[napi(getter)]
    pub fn result(&self) -> Option<QueryResult> {
        self.result.as_ref().map(QueryResult::share)
    }
}

/// One manual checkpoint's outcome.
#[napi(object)]
pub struct CheckpointOutcome {
    pub success: bool,
    pub checkpoint_id: i64,
    pub epoch: i64,
    pub duration_ms: f64,
    /// Continuation error (present when successful checkpoints still recorded one).
    pub error: Option<String>,
}

/// One registered source: name, schema, watermark column.
#[napi(object)]
pub struct SourceInfoObject {
    pub name: String,
    pub schema: Vec<FieldInfo>,
    pub watermark_column: Option<String>,
}

#[napi]
pub struct Connection {
    db: Arc<LaminarDB>,
    closed: AtomicBool,
}

#[napi]
impl Connection {
    /// Execute one SQL statement. `SELECT` returns `kind: "query"` with the
    /// fully collected `result`; SHOW/DESCRIBE return `kind: "metadata"` with
    /// their batch; DDL and `INSERT INTO` return their statement outcome.
    /// Fails with `LAMINAR_101` after `close()`.
    #[napi]
    pub async fn execute(&self, sql: String) -> Result<ExecuteOutcome> {
        if self.is_closed() {
            return Err(connection_closed_error());
        }
        let result = self.db.execute(&sql).await.map_err(map_db_error)?;
        Ok(match result {
            ExecuteResult::Ddl(info) => ExecuteOutcome::ddl(info.statement_type, info.object_name),
            ExecuteResult::RowsAffected(rows) => ExecuteOutcome::of_rows_affected(rows),
            ExecuteResult::Query(handle) => {
                let query_id = handle.id();
                let result = QueryResult::collect(handle).await?;
                ExecuteOutcome::of_query(query_id, result)
            }
            ExecuteResult::Metadata(batch) => {
                ExecuteOutcome::of_metadata(QueryResult::materialized(batch))
            }
        })
    }

    /// Execute a query and return its fully collected result. Non-query SQL
    /// rejects with `LAMINAR_400`.
    #[napi]
    pub async fn query(&self, sql: String) -> Result<QueryResult> {
        if self.is_closed() {
            return Err(connection_closed_error());
        }
        let result = self.db.execute(&sql).await.map_err(map_db_error)?;
        match result {
            ExecuteResult::Query(handle) => QueryResult::collect(handle).await,
            ExecuteResult::Ddl(info) => Err(coded_error(
                400,
                &format!("not a query: {} {}", info.statement_type, info.object_name),
            )),
            ExecuteResult::RowsAffected(rows) => Err(coded_error(
                400,
                &format!("not a query: statement affected {rows} rows"),
            )),
            ExecuteResult::Metadata(_) => Err(coded_error(
                400,
                "metadata statement; use execute() for SHOW/DESCRIBE",
            )),
        }
    }

    /// Ingest JS row objects into a source (one batch; returns rows pushed).
    ///
    /// Sync by design: no engine await is involved (schema resolve → convert →
    /// channel send) and the row conversion needs the JS `Env`, which is only
    /// valid on the calling turn.
    #[napi]
    pub fn insert(&self, source: String, rows: Vec<Object>) -> Result<i64> {
        self.ensure_open()?;
        ingestion::insert_rows(&self.db, &source, &rows)
    }

    /// Ingest an Arrow IPC stream `Buffer` into a source (all batches).
    #[napi]
    pub fn insert_arrow(&self, source: String, bytes: Buffer) -> Result<i64> {
        self.ensure_open()?;
        ingestion::insert_ipc(&self.db, &source, &bytes)
    }

    /// Open a streaming writer for a source. Single-owner; writes after
    /// `close()` reject with `LAMINAR_301`.
    #[napi]
    pub fn writer(&self, source: String) -> Result<Writer> {
        self.ensure_open()?;
        let handle = self.db.source_untyped(&source).map_err(map_db_error)?;
        Ok(Writer::new(handle))
    }

    /// Start the streaming pipeline (idempotent).
    #[napi]
    pub async fn start(&self) -> Result<()> {
        if self.is_closed() {
            return Err(connection_closed_error());
        }
        self.db.clone().start().await.map_err(map_db_error)
    }

    /// Trigger a manual checkpoint. Rejects when checkpointing is not
    /// configured (`checkpoint` in the open config), when no pipeline
    /// topology exists (at least one stream or sink must be defined — the
    /// core wires the coordinator only then), or when the attempt fails.
    #[napi]
    pub async fn checkpoint(&self) -> Result<CheckpointOutcome> {
        if self.is_closed() {
            return Err(connection_closed_error());
        }
        let result = self.db.checkpoint().await.map_err(map_db_error)?;
        Ok(CheckpointOutcome {
            success: result.success,
            checkpoint_id: saturating_i64(result.checkpoint_id),
            epoch: saturating_i64(result.epoch),
            duration_ms: result.duration.as_secs_f64() * 1000.0,
            error: result.error,
        })
    }

    /// True when checkpointing is configured for this connection.
    #[napi]
    pub fn is_checkpoint_enabled(&self) -> bool {
        self.db.is_checkpoint_enabled()
    }

    /// Registered source names.
    #[napi]
    pub async fn list_sources(&self) -> Result<Vec<String>> {
        self.ensure_open_async().await?;
        Ok(self.db.sources().into_iter().map(|s| s.name).collect())
    }

    /// Registered stream names.
    #[napi]
    pub async fn list_streams(&self) -> Result<Vec<String>> {
        self.ensure_open_async().await?;
        Ok(self.db.streams().into_iter().map(|s| s.name).collect())
    }

    /// Registered sink names.
    #[napi]
    pub async fn list_sinks(&self) -> Result<Vec<String>> {
        self.ensure_open_async().await?;
        Ok(self.db.sinks().into_iter().map(|s| s.name).collect())
    }

    /// All registered sources with schemas and watermark columns.
    #[napi]
    pub async fn source_infos(&self) -> Result<Vec<SourceInfoObject>> {
        self.ensure_open_async().await?;
        Ok(self
            .db
            .sources()
            .into_iter()
            .map(|source| SourceInfoObject {
                name: source.name,
                schema: field_infos(&source.schema),
                watermark_column: source.watermark_column,
            })
            .collect())
    }

    /// Schema of a source as field info; unknown names reject `LAMINAR_200`.
    #[napi]
    pub async fn schema(&self, name: String) -> Result<Vec<FieldInfo>> {
        self.ensure_open_async().await?;
        let source = self
            .db
            .sources()
            .into_iter()
            .find(|source| source.name == name)
            .ok_or_else(|| coded_error(200, &format!("table not found: '{name}'")))?;
        Ok(field_infos(&source.schema))
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

    fn ensure_open(&self) -> Result<()> {
        if self.is_closed() {
            return Err(connection_closed_error());
        }
        Ok(())
    }

    async fn ensure_open_async(&self) -> Result<()> {
        self.ensure_open()
    }
}

/// `u64 → i64` for the JS number contract without `unwrap` on foreign sizes.
fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Open an embedded database. `open()` and `open(':memory:')` are in-memory;
/// `open(path)` sets the storage directory (local-durable embedded mode when
/// checkpointing is configured); `config.storageDir` wins over the positional
/// path when both are given.
#[cfg_attr(test, allow(dead_code))] // WHY: test builds strip napi registration
#[napi]
pub async fn open(path: Option<String>, config: Option<OpenConfig>) -> Result<Connection> {
    let core_config = match config {
        Some(config) => config.into_core(path)?,
        None => match path.as_deref() {
            None | Some(":memory:") => LaminarConfig::default(),
            Some(dir) => LaminarConfig {
                storage_dir: Some(std::path::PathBuf::from(dir)),
                ..LaminarConfig::default()
            },
        },
    };
    let db = LaminarDB::open_with_config(core_config).map_err(map_db_error)?;
    Ok(Connection {
        db,
        closed: AtomicBool::new(false),
    })
}
