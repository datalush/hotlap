//! Typed cell readers shared by the group accumulators.

use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use hotlap_core::snapshot::ExtremeValue;

use crate::error::EngineError;

/// Reads an integer cell as `i128`, or `None` when it is null.
pub(crate) fn int_at(
    batch: &RecordBatch,
    col: usize,
    row: usize,
) -> Result<Option<i128>, EngineError> {
    let column = batch.column(col);
    match column.data_type() {
        DataType::Int32 => Ok(option(column, row, |a: &Int32Array| a.value(row) as i128)),
        DataType::Int64 => Ok(option(column, row, |a: &Int64Array| a.value(row) as i128)),
        other => Err(EngineError::Infrastructure(format!(
            "expected an integer column, found {other:?}"
        ))),
    }
}

/// Reads a numeric cell as `f64`, or `None` when it is null.
pub(crate) fn float_at(
    batch: &RecordBatch,
    col: usize,
    row: usize,
) -> Result<Option<f64>, EngineError> {
    let column = batch.column(col);
    match column.data_type() {
        DataType::Int32 => Ok(option(column, row, |a: &Int32Array| a.value(row) as f64)),
        DataType::Int64 => Ok(option(column, row, |a: &Int64Array| a.value(row) as f64)),
        DataType::Float64 => Ok(option(column, row, |a: &Float64Array| a.value(row))),
        other => Err(EngineError::Infrastructure(format!(
            "expected a numeric column, found {other:?}"
        ))),
    }
}

/// Reads a cell as an [`ExtremeValue`], or `None` when it is null.
pub(crate) fn extreme_at(
    batch: &RecordBatch,
    col: usize,
    row: usize,
) -> Result<Option<ExtremeValue>, EngineError> {
    let column = batch.column(col);
    match column.data_type() {
        DataType::Int32 => Ok(option(column, row, |a: &Int32Array| {
            ExtremeValue::Int(a.value(row) as i128)
        })),
        DataType::Int64 => Ok(option(column, row, |a: &Int64Array| {
            ExtremeValue::Int(a.value(row) as i128)
        })),
        DataType::Float64 => Ok(option(column, row, |a: &Float64Array| {
            ExtremeValue::Float(a.value(row))
        })),
        other => Err(EngineError::Infrastructure(format!(
            "expected an integer or float column, found {other:?}"
        ))),
    }
}

/// Downcasts `column` and reads `row`, mapping null to `None`.
fn option<T: Array + 'static, V>(
    column: &ArrayRef,
    row: usize,
    read: impl FnOnce(&T) -> V,
) -> Option<V> {
    if column.is_null(row) {
        return None;
    }
    column.as_any().downcast_ref::<T>().map(read)
}
