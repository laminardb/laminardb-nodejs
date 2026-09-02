//! Ingestion: `insert`/`insertArrow` and the `Writer` class.
//!
//! The `Writer` mirrors the core's `api::Writer` semantics (closed flag,
//! schema check before push, no flush) with one deliberate divergence
//! recorded in plan 02 §design notes: the schema check compares full field
//! names and types, not just the column count.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use laminar_db::{LaminarDB, UntypedSourceHandle};
use napi::bindgen_prelude::{Buffer, Object};
use napi::Result;
use napi_derive::napi;

use crate::arrow_ipc::ipc_to_batches;
use crate::conversion::rows_to_batch;
use crate::error::{coded_error, map_db_error};

/// Resolve a source and hand its handle to `build`.
fn with_source<T>(
    db: &LaminarDB,
    source: &str,
    build: impl FnOnce(UntypedSourceHandle) -> Result<T>,
) -> Result<T> {
    let handle = db.source_untyped(source).map_err(map_db_error)?;
    build(handle)
}

/// Ingest JS row objects into `source` (schema resolved from the catalog).
pub fn insert_rows(db: &LaminarDB, source: &str, rows: &[Object]) -> Result<i64> {
    with_source(db, source, |handle| {
        let schema = handle.schema().clone();
        let batch = rows_to_batch(&schema, rows)?;
        handle
            .push_arrow(batch)
            .map_err(|error| map_db_error(laminar_db::DbError::from(error)))?;
        Ok(rows.len() as i64)
    })
}

/// Ingest an Arrow IPC stream `Buffer` into `source` (all batches in order).
pub fn insert_ipc(db: &LaminarDB, source: &str, bytes: &[u8]) -> Result<i64> {
    with_source(db, source, |handle| {
        let expected = handle.schema().clone();
        let (_, batches) = ipc_to_batches(bytes)?;
        let mut rows = 0;
        for batch in batches {
            check_schema(&expected, Some(batch.schema()))?;
            rows += batch.num_rows();
            handle
                .push_arrow(batch)
                .map_err(|error| map_db_error(laminar_db::DbError::from(error)))?;
        }
        Ok(rows as i64)
    })
}

/// Full field-name/type comparison; empty input (schema-only) passes.
fn check_schema(expected: &SchemaRef, actual: Option<SchemaRef>) -> Result<()> {
    let Some(actual) = actual else {
        return Ok(());
    };
    let expected_fields = expected.fields();
    let actual_fields = actual.fields();
    if expected_fields.len() != actual_fields.len() {
        return Err(coded_error(
            302,
            &format!(
                "schema mismatch: expected {} columns, got {}",
                expected_fields.len(),
                actual_fields.len()
            ),
        ));
    }
    for (expected, actual) in expected_fields.iter().zip(actual_fields) {
        if expected.name() != actual.name() || expected.data_type() != actual.data_type() {
            return Err(coded_error(
                302,
                &format!(
                    "schema mismatch at column '{}': expected {} {:?}, got {} {:?}",
                    expected.name(),
                    expected.is_nullable(),
                    expected.data_type(),
                    actual.is_nullable(),
                    actual.data_type()
                ),
            ));
        }
    }
    Ok(())
}

/// Streaming writer for one source: single-owner, hot-path pushes.
///
/// `writeRows`/`writeArrow` push batch-at-a-time into the engine; backpressure
/// is observable via `pending`/`capacity`/`isBackpressured`. `close()` is
/// idempotent; writes after close reject with `LAMINAR_301`.
#[napi]
pub struct Writer {
    handle: UntypedSourceHandle,
    closed: bool,
}

impl Writer {
    pub fn new(handle: UntypedSourceHandle) -> Self {
        Self {
            handle,
            closed: false,
        }
    }
}

#[napi]
impl Writer {
    /// Source name.
    #[napi]
    pub fn name(&self) -> String {
        self.handle.name().to_owned()
    }

    /// Source schema fields.
    #[napi]
    pub fn schema(&self) -> Vec<crate::query::FieldInfo> {
        crate::query::field_infos(self.handle.schema())
    }

    /// Build from JS row objects and push one batch.
    #[napi]
    pub fn write_rows(&mut self, rows: Vec<Object>) -> Result<i64> {
        let rows_written = rows.len() as i64;
        let batch = self.build_rows(&rows)?;
        self.push(batch)?;
        Ok(rows_written)
    }

    /// Push every batch from an Arrow IPC stream `Buffer`.
    #[napi]
    pub fn write_arrow(&mut self, bytes: Buffer) -> Result<i64> {
        self.ensure_open()?;
        let (_, batches) = ipc_to_batches(&bytes)?;
        let expected = self.handle.schema().clone();
        let mut rows = 0;
        for batch in batches {
            check_schema(&expected, Some(batch.schema()))?;
            rows += batch.num_rows();
            self.push(batch)?;
        }
        Ok(rows as i64)
    }

    /// Advance the event-time watermark (milliseconds since epoch).
    #[napi]
    pub fn watermark(&self, timestamp: i64) {
        self.handle.watermark(timestamp);
    }

    /// Current event-time watermark.
    #[napi]
    pub fn current_watermark(&self) -> i64 {
        self.handle.current_watermark()
    }

    /// Rows buffered in the source, not yet consumed by the pipeline.
    #[napi]
    pub fn pending(&self) -> i64 {
        self.handle.pending() as i64
    }

    /// Source buffer capacity in rows.
    #[napi]
    pub fn capacity(&self) -> i64 {
        self.handle.capacity() as i64
    }

    /// True when the source buffer is more than 80% full — slow down.
    #[napi]
    pub fn is_backpressured(&self) -> bool {
        self.handle.is_backpressured()
    }

    /// Idempotent close; writes after close reject with `LAMINAR_301`.
    #[napi]
    pub fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }

    fn build_rows(&self, rows: &[Object]) -> Result<RecordBatch> {
        self.ensure_open()?;
        let schema = self.handle.schema().clone();
        rows_to_batch(&schema, rows)
    }

    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.ensure_open()?;
        self.handle
            .push_arrow(batch)
            .map_err(|error| map_db_error(laminar_db::DbError::from(error)))
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            return Err(coded_error(301, "writer is closed"));
        }
        Ok(())
    }
}
