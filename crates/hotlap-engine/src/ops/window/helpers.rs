//! Event-time window column helpers.

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::row::{OwnedRow, Row, RowConverter};

use crate::error::EngineError;

/// Reads an event-time column as non-negative `i64`, mapping nulls to zero.
pub(super) fn event_times(column: &ArrayRef) -> Result<Int64Array, EngineError> {
    let casted = cast(column.as_ref(), &DataType::Int64)?;
    let ints = casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| EngineError::Infrastructure("int64 cast produced wrong type".to_string()))?;
    let values: Vec<i64> = ints.iter().map(|value| value.unwrap_or(0).max(0)).collect();
    Ok(Int64Array::from(values))
}

/// Decodes `arrow::row` key bytes back into column arrays for `converter`.
pub(super) fn decode(
    converter: &RowConverter,
    rows: &[&OwnedRow],
) -> Result<Vec<ArrayRef>, EngineError> {
    let parser = converter.parser();
    let parsed: Vec<Row<'_>> = rows.iter().map(|row| parser.parse(row.as_ref())).collect();
    Ok(converter.convert_rows(parsed)?)
}
