//! Arrow IPC serialization: `RecordBatch` ⇄ stream bytes in napi `Buffer`s.
//!
//! One copy per crossing (plan 00 D6): Rust owns serialization, JS owns
//! rehydration via the optional `apache-arrow` peer dependency. The zero-copy
//! C Data Interface comparison is a Phase 2 spike behind a benchmark gate.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;

use crate::error::coded_error;

/// Serialize batches (plus schema) as one Arrow IPC stream.
pub fn batches_to_ipc(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>, napi::Error> {
    let mut bytes = Vec::new();
    let writer = &mut StreamWriter::try_new(&mut bytes, schema).map_err(output_error)?;
    for batch in batches {
        writer.write(batch).map_err(output_error)?;
    }
    writer.finish().map_err(output_error)?;
    Ok(bytes)
}

/// Parse an Arrow IPC stream into its schema and batches.
///
/// Rejects truncated or non-stream data with a 300-coded message; the schema
/// travels in the stream, so callers never guess it.
pub fn ipc_to_batches(bytes: &[u8]) -> Result<(SchemaRef, Vec<RecordBatch>), napi::Error> {
    let mut reader = StreamReader::try_new(bytes, None).map_err(input_error)?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    for batch in reader.by_ref() {
        batches.push(batch.map_err(input_error)?);
    }
    Ok((schema, batches))
}

/// IPC bytes supplied by the caller are ingestion input: failures are 300.
fn input_error(error: arrow::error::ArrowError) -> napi::Error {
    coded_error(300, &format!("invalid Arrow IPC data: {error}"))
}

/// Serializing engine-owned batches cannot fail unless the engine produced an
/// invalid batch: that is an internal (900) fault, never caller error.
fn output_error(error: arrow::error::ArrowError) -> napi::Error {
    coded_error(900, &format!("Arrow IPC serialization failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float64, true),
        ]));
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), None, Some(3.0)]));
        RecordBatch::try_new(schema, vec![a, b]).expect("valid sample batch")
    }

    #[test]
    fn ipc_roundtrip_preserves_schema_values_and_nulls() {
        let batch = sample_batch();
        let bytes =
            batches_to_ipc(&batch.schema(), std::slice::from_ref(&batch)).expect("serialize");
        let (schema, batches) = ipc_to_batches(&bytes).expect("parse");
        assert_eq!(schema, batch.schema());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], batch);
    }

    #[test]
    fn ipc_multi_batch_roundtrip_preserves_order() {
        let batch = sample_batch();
        let bytes =
            batches_to_ipc(&batch.schema(), &[batch.clone(), batch.clone()]).expect("serialize");
        let (_, batches) = ipc_to_batches(&bytes).expect("parse");
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[1], batch);
    }

    #[test]
    fn garbage_input_rejects_with_ingestion_code() {
        let error = ipc_to_batches(b"not arrow").expect_err("garbage rejected");
        assert!(error.reason.starts_with("[LAMINAR_300]"));
    }
}
