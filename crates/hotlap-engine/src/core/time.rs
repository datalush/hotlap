//! Event-time column helpers shared by the core's watermark filtering.

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;

use crate::error::EngineError;

/// Reads an event-time column as non-negative `i64`; nulls map to zero.
pub(super) fn time_values(batch: &RecordBatch, col: usize) -> Result<Int64Array, EngineError> {
    use arrow::compute::cast;
    use arrow::datatypes::DataType;

    let column = batch
        .columns()
        .get(col)
        .ok_or_else(|| EngineError::Unsupported(format!("time column {col} out of range")))?;
    let casted = cast(column.as_ref(), &DataType::Int64)?;
    let ints = casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| EngineError::Infrastructure("int64 cast produced wrong type".to_string()))?;
    Ok(Int64Array::from(
        ints.iter()
            .map(|value| value.unwrap_or(0).max(0))
            .collect::<Vec<_>>(),
    ))
}
