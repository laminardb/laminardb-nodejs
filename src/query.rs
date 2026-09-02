//! Query results: collected batches exposed as Arrow IPC and row objects.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use napi::bindgen_prelude::{Buffer, Env, Object};
use napi::Result;
use napi_derive::napi;

use crate::arrow_ipc::batches_to_ipc;
use crate::error::{coded_error, map_db_error};
use laminar_db::QueryHandle;

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
