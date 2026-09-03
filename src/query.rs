//! Query results: collected batches exposed as Arrow IPC and row objects.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use napi::bindgen_prelude::{Buffer, Env, Object};
use napi::Result;
use napi_derive::napi;

use crate::arrow_ipc::batches_to_ipc;
use crate::error::{coded_error, map_db_error};
use laminar_db::QueryHandle;
use tokio::sync::mpsc;

/// One column of a result schema.
///
/// `data_type` is the Arrow type string (Debug-formatted, like the core's FFI
/// layer reports it) — informational; parse it only for display.
#[napi(object)]
pub struct FieldInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Schema fields in declaration order.
pub fn field_infos(schema: &SchemaRef) -> Vec<FieldInfo> {
    schema
        .fields()
        .iter()
        .map(|field| FieldInfo {
            name: field.name().clone(),
            data_type: format!("{:?}", field.data_type()),
            nullable: field.is_nullable(),
        })
        .collect()
}

/// One Arrow `RecordBatch` of query output.
///
/// Data leaves via `toIPC()` (one copy; rehydrate with `apache-arrow`'s
/// `tableFromIPC`) or `toArray()` (row objects). The batch is immutable.
#[napi]
pub struct ArrowBatch {
    batch: RecordBatch,
}

impl From<RecordBatch> for ArrowBatch {
    fn from(batch: RecordBatch) -> Self {
        Self { batch }
    }
}

impl ArrowBatch {
    /// Cheap share for handing the same batch out of multiple accessors.
    pub(crate) fn share(&self) -> Self {
        Self {
            batch: self.batch.clone(),
        }
    }
}

#[napi]
impl ArrowBatch {
    #[napi]
    pub fn num_rows(&self) -> i64 {
        self.batch.num_rows() as i64
    }

    #[napi]
    pub fn num_columns(&self) -> i64 {
        self.batch.num_columns() as i64
    }

    #[napi]
    pub fn schema(&self) -> Vec<FieldInfo> {
        field_infos(&self.batch.schema())
    }

    /// This batch as an Arrow IPC stream `Buffer` (schema included).
    #[napi(js_name = "toIPC")]
    pub fn to_ipc(&self) -> Result<Buffer> {
        let schema = self.batch.schema();
        let bytes = batches_to_ipc(&schema, std::slice::from_ref(&self.batch))?;
        Ok(Buffer::from(bytes))
    }

    /// Row objects (one per row, keyed by column); see `conversion` for the
    /// type conventions and the supported-type set.
    #[napi]
    pub fn to_array<'env>(&self, env: &'env Env) -> Result<Vec<Object<'env>>> {
        crate::conversion::batch_to_rows(env, &self.batch)
    }
}

/// A fully collected query result: schema plus all output batches.
#[napi]
pub struct QueryResult {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl QueryResult {
    /// A cheap share of this result (Arc bumps) for handing instances out of
    /// accessors that only have `&self`.
    pub(crate) fn share(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            batches: self.batches.clone(),
        }
    }

    /// Collect a running query's output to completion (plan 02 §verified:
    /// `recv_async` until `Disconnected`; the broadcast buffer gives ~2048
    /// batches of headroom — the same collection semantics as the core's
    /// `api::QueryStream`).
    pub async fn collect(mut handle: QueryHandle) -> Result<Self> {
        let schema = handle.schema().clone();
        let mut subscription = handle.subscribe_raw().map_err(map_db_error)?;
        // Disconnected terminates the loop; recv_async never reports Timeout.
        let mut batches = Vec::new();
        while let Ok(batch) = subscription.recv_async().await {
            batches.push(batch);
        }
        Ok(Self { schema, batches })
    }

    /// A pre-materialized result (e.g. SHOW/DESCRIBE metadata batches).
    pub fn materialized(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            schema,
            batches: vec![batch],
        }
    }
}

#[napi]
impl QueryResult {
    #[napi]
    pub fn schema(&self) -> Vec<FieldInfo> {
        field_infos(&self.schema)
    }

    #[napi]
    pub fn num_rows(&self) -> i64 {
        self.batches.iter().map(|b| b.num_rows() as i64).sum()
    }

    #[napi]
    pub fn num_batches(&self) -> i64 {
        self.batches.len() as i64
    }

    /// Batch `index` (0-based); rejects with 400 when out of range.
    #[napi]
    pub fn batch(&self, index: i64) -> Result<ArrowBatch> {
        let batch = self
            .batches
            .get(usize::try_from(index).unwrap_or(usize::MAX))
            .ok_or_else(|| {
                coded_error(
                    400,
                    &format!(
                        "batch index {index} out of range (0..{})",
                        self.batches.len()
                    ),
                )
            })?;
        Ok(ArrowBatch {
            batch: batch.clone(),
        })
    }

    /// The whole result as one Arrow IPC stream `Buffer` (schema included).
    #[napi(js_name = "toIPC")]
    pub fn to_ipc(&self) -> Result<Buffer> {
        let bytes = batches_to_ipc(&self.schema, &self.batches)?;
        Ok(Buffer::from(bytes))
    }

    /// All rows as objects, batch order preserved.
    #[napi]
    pub fn to_array<'env>(&self, env: &'env Env) -> Result<Vec<Object<'env>>> {
        let mut rows = Vec::with_capacity(self.num_rows() as usize);
        for batch in &self.batches {
            rows.extend(crate::conversion::batch_to_rows(env, batch)?);
        }
        Ok(rows)
    }
}

/// A streaming query: batches arrive on demand instead of one collected
/// result (plan 03 Task 2.3).
///
/// `nextBatch()` resolves `null` at end-of-stream; `cancel()` is idempotent
/// and wakes a pending `nextBatch` with `null`. Dropping the stream cancels
/// it (the underlying query subscription closes with it).
#[napi]
pub struct QueryStream {
    schema: SchemaRef,
    query_id: u64,
    rx: tokio::sync::Mutex<mpsc::Receiver<Option<RecordBatch>>>,
    token: tokio_util::sync::CancellationToken,
}

impl QueryStream {
    /// Start a stream from a fresh query handle.
    ///
    /// The reader task OWNS the handle: `QueryHandle::drop` cancels the
    /// query, so dropping it here (before the reader drains the output)
    /// would truncate results — the core's `api::QueryStream` keeps it
    /// alive for the same reason. The handle drops when the reader exits.
    pub fn from_handle(mut handle: laminar_db::QueryHandle) -> Result<Self> {
        let schema = handle.schema().clone();
        let query_id = handle.id();
        let mut subscription = handle.subscribe_raw().map_err(map_db_error)?;
        let (tx, rx) = mpsc::channel(128);
        let token = tokio_util::sync::CancellationToken::new();
        let reader_token = token.clone();
        crate::spawn(async move {
            loop {
                let batch = tokio::select! {
                    batch = subscription.recv_async() => batch,
                    // Cancellation maps to the Disconnected arm: stop reading.
                    () = reader_token.cancelled() => Err(laminar_core::streaming::error::RecvError::Disconnected),
                };
                match batch {
                    Ok(batch) => {
                        if tx.send(Some(batch)).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => {
                        let _ = tx.send(None).await;
                        break;
                    }
                }
            }
            handle.cancel();
            drop(handle);
        });
        Ok(Self {
            schema,
            query_id,
            rx: tokio::sync::Mutex::new(rx),
            token,
        })
    }
}

impl Drop for QueryStream {
    fn drop(&mut self) {
        // Lifecycle backstop: a dropped stream must not keep the query
        // subscription alive.
        self.token.cancel();
    }
}

#[napi]
impl QueryStream {
    /// Output schema of the query.
    #[napi]
    pub fn schema(&self) -> Vec<FieldInfo> {
        field_infos(&self.schema)
    }

    /// The query handle id (usable with `Connection.cancelQuery`).
    #[napi]
    pub fn query_id(&self) -> i64 {
        i64::try_from(self.query_id).unwrap_or(i64::MAX)
    }

    /// Wait for the next batch; `null` at end-of-stream or after `cancel()`.
    #[napi]
    pub async fn next_batch(&self) -> Result<Option<ArrowBatch>> {
        let mut receiver = self.rx.lock().await;
        Ok(receiver.recv().await.flatten().map(ArrowBatch::from))
    }

    /// Cancel the query (idempotent); a pending `nextBatch` resolves `null`.
    #[napi]
    pub fn cancel(&self) {
        self.token.cancel();
    }
}
