//! JS value ⇄ Arrow column conversion (plan 02 Task 1.3).
//!
//! Write path: JS row objects → `RecordBatch` against the source schema.
//! Read path: `RecordBatch` → JS row objects. Conversions are strict — a
//! number never silently becomes a string — and every rejection names the
//! column and carries a 300-coded ingestion error. Unsupported Arrow types
//! reject with 900; `toIPC()` plus the `apache-arrow` peer remains the
//! escape hatch for those columns.
//!
//! Conventions (documented public behavior):
//! - JS numbers for `Timestamp`/`Date32`/`Date64` columns are **milliseconds**
//!   since epoch, converted to the column's unit; `Date` objects likewise.
//! - `Int64`/`UInt64` accept and produce JS `BigInt` (beyond 2^53 a JS number
//!   loses precision by design).

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, NullArray, RecordBatch,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Field, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, SchemaRef, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use napi::bindgen_prelude::{i64n, BigInt, Env, JsValue, Null, Object, Unknown, ValueType};

use crate::error::coded_error;

const MS_PER_DAY: i64 = 86_400_000;
/// Beyond 2^53-1 an f64 cannot represent every integer; number inputs are
/// rejected past this magnitude instead of silently corrupting the value.
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// A neutral JS cell extracted from a row object.
enum Cell {
    Null,
    Bool(bool),
    Num(f64),
    Str(String),
    BigInt(i64),
}

impl Cell {
    fn type_name(&self) -> &'static str {
        match self {
            Cell::Null => "null",
            Cell::Bool(_) => "boolean",
            Cell::Num(_) => "number",
            Cell::Str(_) => "string",
            Cell::BigInt(_) => "bigint",
        }
    }
}

fn cell_from_unknown(column: &str, value: &Unknown) -> napi::Result<Cell> {
    match value.get_type()? {
        ValueType::Undefined | ValueType::Null => Ok(Cell::Null),
        ValueType::Boolean => Ok(Cell::Bool(unsafe { value.cast::<bool>()? })),
        ValueType::Number => Ok(Cell::Num(unsafe { value.cast::<f64>()? })),
        ValueType::BigInt => {
            let (value, lossless) = unsafe { value.cast::<BigInt>()? }.get_i64();
            if !lossless {
                return Err(coded_error(
                    300,
                    "BigInt exceeds the 64-bit signed integer range",
                ));
            }
            Ok(Cell::BigInt(value))
        }
        ValueType::String => Ok(Cell::Str(unsafe { value.cast::<String>()? })),
        ValueType::Object => Err(if value.is_date()? {
            column_error(
                column,
                "pass Dates as milliseconds since epoch at the native seam".to_owned(),
            )
        } else {
            unsupported_cell("object")
        }),
        other => Err(unsupported_cell(&other.to_string())),
    }
}

fn unsupported_cell(type_name: &str) -> napi::Error {
    coded_error(300, &format!("unsupported row value type: {type_name}"))
}

fn column_error(column: &str, message: String) -> napi::Error {
    coded_error(300, &format!("column '{column}': {message}"))
}

/// Coerce a numeric cell to an exact integer in `lo..=hi`, rejecting any
/// loss: bigints keep full precision; JS numbers must be integral and within
/// ±(2^53−1) where every integer is exactly representable (the f64→i64 bound
/// comparison trap at 2^63 is why the check never touches f64 bounds).
fn int_in_range(column: &str, cell: &Cell, lo: i64, hi: i64) -> napi::Result<i64> {
    match cell {
        Cell::BigInt(value) => {
            if *value < lo || *value > hi {
                Err(column_error(
                    column,
                    format!("expected integer in {lo}..={hi}, got {value}"),
                ))
            } else {
                Ok(*value)
            }
        }
        Cell::Num(value) => {
            let truncated = value.trunc();
            if !value.is_finite() || truncated != *value {
                return Err(column_error(
                    column,
                    format!("expected an integer in {lo}..={hi}, got {value}"),
                ));
            }
            if truncated.abs() > MAX_SAFE_INTEGER {
                return Err(column_error(
                    column,
                    format!(
                        "number {truncated} exceeds the exact-integer range of JS numbers \
                         (2^53-1); pass a BigInt"
                    ),
                ));
            }
            let exact = truncated as i64; // |exact| ≤ 2^53-1 < 2^63: lossless
            if exact < lo || exact > hi {
                Err(column_error(
                    column,
                    format!("expected integer in {lo}..={hi}, got {exact}"),
                ))
            } else {
                Ok(exact)
            }
        }
        other => Err(column_error(
            column,
            format!("expected a number, got {}", other.type_name()),
        )),
    }
}

/// The approximate numeric value of a numeric cell — for float columns only,
/// where f64 semantics are the column's own semantics.
fn numeric_cell(column: &str, cell: &Cell) -> napi::Result<f64> {
    match cell {
        Cell::Num(v) => Ok(*v),
        Cell::BigInt(v) => Ok(*v as f64),
        other => Err(column_error(
            column,
            format!("expected a number, got {}", other.type_name()),
        )),
    }
}

/// The exact (truncating) milliseconds value of a temporal cell.
fn millis_cell(column: &str, cell: &Cell) -> napi::Result<i64> {
    match cell {
        Cell::BigInt(value) => Ok(*value),
        Cell::Num(value) => {
            if !value.is_finite() {
                return Err(column_error(
                    column,
                    format!("expected finite milliseconds, got {value}"),
                ));
            }
            let truncated = value.trunc();
            if truncated.abs() > MAX_SAFE_INTEGER {
                return Err(column_error(
                    column,
                    format!(
                        "millisecond magnitude {truncated} exceeds the exact-integer range \
                         of JS numbers (2^53-1); pass a BigInt"
                    ),
                ));
            }
            Ok(truncated as i64)
        }
        other => Err(column_error(
            column,
            format!(
                "expected a number of milliseconds, got {}",
                other.type_name()
            ),
        )),
    }
}

fn string_cell<'a>(column: &str, cell: &'a Cell) -> napi::Result<&'a str> {
    match cell {
        Cell::Str(v) => Ok(v),
        other => Err(column_error(
            column,
            format!("expected a string, got {}", other.type_name()),
        )),
    }
}

/// Build one column array from the extracted cells (row-major slice).
fn column_from_cells(field: &Field, cells: &[Cell]) -> napi::Result<ArrayRef> {
    let column = field.name();
    macro_rules! int_column {
        ($native:ty, $array:ty, $lo:expr, $hi:expr) => {{
            let mut values: Vec<Option<$native>> = Vec::with_capacity(cells.len());
            for cell in cells {
                values.push(match cell {
                    Cell::Null => None,
                    _ => Some(int_in_range(column, cell, $lo, $hi)? as $native),
                });
            }
            Arc::new(<$array>::from(values))
        }};
    }

    let string_values = |large: bool| -> napi::Result<ArrayRef> {
        let mut values = Vec::with_capacity(cells.len());
        for cell in cells {
            values.push(match cell {
                Cell::Null => None,
                _ => Some(string_cell(column, cell)?.to_owned()),
            });
        }
        Ok(if large {
            Arc::new(LargeStringArray::from(values)) as ArrayRef
        } else {
            Arc::new(StringArray::from(values))
        })
    };

    Ok(match field.data_type() {
        DataType::Boolean => {
            let mut values = Vec::with_capacity(cells.len());
            for cell in cells {
                values.push(match cell {
                    Cell::Null => None,
                    Cell::Bool(v) => Some(*v),
                    other => {
                        return Err(column_error(
                            column,
                            format!("expected a boolean, got {}", other.type_name()),
                        ));
                    }
                });
            }
            Arc::new(BooleanArray::from(values))
        }
        DataType::Int8 => int_column!(i8, Int8Array, i8::MIN as i64, i8::MAX as i64),
        DataType::Int16 => int_column!(i16, Int16Array, i16::MIN as i64, i16::MAX as i64),
        DataType::Int32 => int_column!(i32, Int32Array, i32::MIN as i64, i32::MAX as i64),
        DataType::Int64 => int_column!(i64, Int64Array, i64::MIN, i64::MAX),
        DataType::UInt8 => int_column!(u8, UInt8Array, 0, u8::MAX as i64),
        DataType::UInt16 => int_column!(u16, UInt16Array, 0, u16::MAX as i64),
        DataType::UInt32 => int_column!(u32, UInt32Array, 0, u32::MAX as i64),
        DataType::UInt64 => int_column!(u64, UInt64Array, 0, i64::MAX),
        DataType::Float32 => {
            let mut values = Vec::with_capacity(cells.len());
            for cell in cells {
                values.push(match cell {
                    Cell::Null => None,
                    _ => Some(numeric_cell(column, cell)? as f32),
                });
            }
            Arc::new(Float32Array::from(values))
        }
        DataType::Float64 => {
            let mut values = Vec::with_capacity(cells.len());
            for cell in cells {
                values.push(match cell {
                    Cell::Null => None,
                    _ => Some(numeric_cell(column, cell)?),
                });
            }
            Arc::new(Float64Array::from(values))
        }
        DataType::Utf8 => string_values(false)?,
        DataType::LargeUtf8 => string_values(true)?,
        DataType::Date32 => {
            let mut values = Vec::with_capacity(cells.len());
            for cell in cells {
                values.push(match cell {
                    Cell::Null => None,
                    _ => {
                        // euclidean division: pre-1970 dates floor to -1, not 0
                        let days = millis_cell(column, cell)?.div_euclid(MS_PER_DAY);
                        Some(i32::try_from(days).map_err(|_| {
                            column_error(column, format!("date out of Date32 range: {days} days"))
                        })?)
                    }
                });
            }
            Arc::new(Date32Array::from(values))
        }
        DataType::Date64 => {
            let mut values = Vec::with_capacity(cells.len());
            for cell in cells {
                values.push(match cell {
                    Cell::Null => None,
                    _ => Some(millis_cell(column, cell)?),
                });
            }
            Arc::new(Date64Array::from(values))
        }
        DataType::Timestamp(unit, _) => {
            let millis: Vec<Option<i64>> = cells
                .iter()
                .map(|cell| match cell {
                    Cell::Null => Ok(None),
                    _ => Ok(Some(millis_cell(column, cell)?)),
                })
                .collect::<napi::Result<_>>()?;
            timestamp_array(*unit, &millis)?
        }
        DataType::Null => {
            if cells.iter().any(|cell| !matches!(cell, Cell::Null)) {
                return Err(column_error(
                    column,
                    "typed null column only accepts null".to_owned(),
                ));
            }
            Arc::new(NullArray::new(cells.len()))
        }
        other => {
            return Err(coded_error(
                900,
                &format!(
                    "row ingestion does not support column '{column}' of type {other:?} yet; \
                     use insertArrow with Arrow IPC data"
                ),
            ));
        }
    })
}

fn timestamp_array(unit: TimeUnit, millis: &[Option<i64>]) -> napi::Result<ArrayRef> {
    let scale = |ms: i64| -> napi::Result<i64> {
        // Euclidean division truncates toward negative infinity, matching
        // Arrow's temporal semantics for pre-epoch instants.
        let scaled = match unit {
            TimeUnit::Second => Ok(ms.div_euclid(1000)),
            TimeUnit::Millisecond => Ok(ms),
            TimeUnit::Microsecond => ms.checked_mul(1_000).ok_or_else(|| out_of_range(unit, ms)),
            TimeUnit::Nanosecond => ms
                .checked_mul(1_000_000)
                .ok_or_else(|| out_of_range(unit, ms)),
        }?;
        Ok(scaled)
    };
    let mut values = Vec::with_capacity(millis.len());
    for ms in millis {
        values.push(match ms {
            None => None,
            Some(ms) => Some(scale(*ms)?),
        });
    }
    Ok(match unit {
        TimeUnit::Second => Arc::new(TimestampSecondArray::from(values)) as ArrayRef,
        TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(values)),
        TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(values)),
        TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(values)),
    })
}

fn out_of_range(unit: TimeUnit, ms: i64) -> napi::Error {
    coded_error(
        300,
        &format!(
            "timestamp {ms} ms is out of range for a Timestamp({unit:?}) column;              nanosecond columns span roughly ±292 years around 1970"
        ),
    )
}

/// Build a `RecordBatch` from JS row objects against `schema`.
///
/// Missing keys count as null; every present value is validated against the
/// column type with a 300-coded, column-naming error on mismatch.
pub fn rows_to_batch(schema: &SchemaRef, rows: &[Object]) -> napi::Result<RecordBatch> {
    let mut columns: Vec<Vec<Cell>> = Vec::with_capacity(schema.fields().len());
    for _ in 0..schema.fields().len() {
        columns.push(Vec::with_capacity(rows.len()));
    }
    for (row_index, row) in rows.iter().enumerate() {
        for (col_index, field) in schema.fields().iter().enumerate() {
            let unknown: Option<Unknown> = row.get(field.name())?;
            let cell = match unknown {
                None => Cell::Null,
                Some(value) => cell_from_unknown(field.name(), &value)
                    .map_err(|error| annotate_row(error, row_index))?,
            };
            if matches!(cell, Cell::Null) && !field.is_nullable() {
                return Err(annotate_row(
                    column_error(field.name(), "null in non-nullable column".to_owned()),
                    row_index,
                ));
            }
            columns[col_index].push(cell);
        }
    }
    let arrays = schema
        .fields()
        .iter()
        .zip(&columns)
        .map(|(field, cells)| column_from_cells(field, cells))
        .collect::<napi::Result<Vec<_>>>()?;
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|error| coded_error(900, &format!("row to batch conversion failed: {error}")))
}

fn annotate_row(error: napi::Error, row_index: usize) -> napi::Error {
    napi::Error::from_reason(format!("{} (row {row_index})", error.reason))
}

/// The JS representation of one array slot.
enum OutCell {
    Null,
    Bool(bool),
    Num(f64),
    Str(String),
    BigInt(i64),
}

fn set_cell(object: &mut Object, name: &str, cell: OutCell) -> napi::Result<()> {
    match cell {
        OutCell::Null => object.set(name, Null),
        OutCell::Bool(v) => object.set(name, v),
        OutCell::Num(v) => object.set(name, v),
        OutCell::Str(v) => object.set(name, v),
        OutCell::BigInt(v) => object.set(name, i64n(v)),
    }
}

macro_rules! numeric_out {
    ($array:expr, $index:expr) => {
        OutCell::Num($array.value($index) as f64)
    };
}

fn cell_from_array(array: &dyn Array, index: usize, field: &Field) -> napi::Result<OutCell> {
    if array.is_null(index) {
        return Ok(OutCell::Null);
    }
    // `as_*` downcasts rely on arrow's guarantee that an array's concrete type
    // matches the schema's declared DataType; a mismatch is an internal fault.
    Ok(match field.data_type() {
        DataType::Boolean => OutCell::Bool(array.as_boolean().value(index)),
        DataType::Int8 => {
            numeric_out!(array.as_primitive::<Int8Type>(), index)
        }
        DataType::Int16 => {
            numeric_out!(array.as_primitive::<Int16Type>(), index)
        }
        DataType::Int32 => {
            numeric_out!(array.as_primitive::<Int32Type>(), index)
        }
        DataType::Int64 => OutCell::BigInt(array.as_primitive::<Int64Type>().value(index)),
        DataType::UInt8 => {
            numeric_out!(array.as_primitive::<UInt8Type>(), index)
        }
        DataType::UInt16 => {
            numeric_out!(array.as_primitive::<UInt16Type>(), index)
        }
        DataType::UInt32 => {
            numeric_out!(array.as_primitive::<UInt32Type>(), index)
        }
        DataType::UInt64 => OutCell::BigInt(
            i64::try_from(array.as_primitive::<UInt64Type>().value(index)).unwrap_or(i64::MAX),
        ),
        DataType::Float32 => {
            numeric_out!(array.as_primitive::<Float32Type>(), index)
        }
        DataType::Float64 => {
            numeric_out!(array.as_primitive::<Float64Type>(), index)
        }
        DataType::Utf8 => OutCell::Str(array.as_string::<i32>().value(index).to_owned()),
        DataType::LargeUtf8 => OutCell::Str(array.as_string::<i64>().value(index).to_owned()),
        DataType::Date32 => OutCell::Num(
            (array.as_primitive::<Date32Type>().value(index) as i64 * MS_PER_DAY) as f64,
        ),
        DataType::Date64 => OutCell::Num(array.as_primitive::<Date64Type>().value(index) as f64),
        DataType::Timestamp(unit, _) => {
            let raw = match unit {
                TimeUnit::Second => {
                    array.as_primitive::<TimestampSecondType>().value(index) as f64 * 1000.0
                }
                TimeUnit::Millisecond => array
                    .as_primitive::<TimestampMillisecondType>()
                    .value(index) as f64,
                TimeUnit::Microsecond => {
                    array
                        .as_primitive::<TimestampMicrosecondType>()
                        .value(index) as f64
                        / 1000.0
                }
                TimeUnit::Nanosecond => {
                    array.as_primitive::<TimestampNanosecondType>().value(index) as f64
                        / 1_000_000.0
                }
            };
            OutCell::Num(raw)
        }
        other => {
            return Err(coded_error(
                900,
                &format!(
                    "row conversion does not support column '{}' of type {other:?} yet; \
                     use toIPC() with apache-arrow",
                    field.name()
                ),
            ));
        }
    })
}

/// Convert a `RecordBatch` into JS row objects (one per row, keyed by column).
pub fn batch_to_rows<'env>(env: &'env Env, batch: &RecordBatch) -> napi::Result<Vec<Object<'env>>> {
    let schema = batch.schema();
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        let mut object = Object::new(env)?;
        for (col_index, field) in schema.fields().iter().enumerate() {
            let cell = cell_from_array(batch.column(col_index), row_index, field)?;
            set_cell(&mut object, field.name(), cell)?;
        }
        rows.push(object);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_units_scale_from_millis() {
        let millis = [Some(1_000_500), None, Some(-1)];
        let second = timestamp_array(TimeUnit::Second, &millis).expect("seconds");
        assert_eq!(second.len(), 3);
        let micros = timestamp_array(TimeUnit::Microsecond, &millis).expect("micros");
        assert_eq!(micros.len(), 3);
    }

    #[test]
    fn timestamp_negative_millis_floor_toward_negative_infinity() {
        // -1 ms in seconds is -0.001 s -> euclidean floor is -1, not 0.
        let array = timestamp_array(TimeUnit::Second, &[Some(-1)]).expect("seconds");
        let values = array
            .as_primitive::<arrow::datatypes::TimestampSecondType>()
            .value(0);
        assert_eq!(values, -1);
    }

    #[test]
    fn timestamp_out_of_range_rejects_with_ingestion_code() {
        let error =
            timestamp_array(TimeUnit::Nanosecond, &[Some(i64::MAX)]).expect_err("out of range");
        assert!(error.reason.starts_with("[LAMINAR_300]"));
    }

    #[test]
    fn int_in_range_rejects_unsafe_numbers_and_out_of_range_bigints() {
        let column = "c";
        assert_eq!(
            int_in_range(
                column,
                &Cell::BigInt(9_223_372_036_854_775_807),
                i64::MIN,
                i64::MAX
            )
            .expect("i64 max bigint"),
            i64::MAX
        );
        assert!(int_in_range(column, &Cell::Num(2.0f64.powi(53)), i64::MIN, i64::MAX).is_err());
        assert!(int_in_range(column, &Cell::Num(f64::NAN), 0, 10).is_err());
        assert!(int_in_range(column, &Cell::Num(1.5), 0, 10).is_err());
    }
}
