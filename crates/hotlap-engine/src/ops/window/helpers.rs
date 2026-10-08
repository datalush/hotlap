//! Event-time window column helpers.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, new_empty_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, Row, RowConverter};

use crate::batch::ZSetBatch;
use crate::error::EngineError;

/// A single closed window: `(key row, window_start, count)`.
pub(super) type Closed = (OwnedRow, i64, i64);

/// Open buckets grouped by window start: `ws -> (key bytes -> (key, count))`.
pub(super) type Windows = BTreeMap<i64, BTreeMap<Vec<u8>, (OwnedRow, i64)>>;

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

/// Builds an empty `key ++ [window_start, count]` output batch.
pub(super) fn empty_output(
    fields: SchemaRef,
    key: &[usize],
    schema: &SchemaRef,
) -> Result<ZSetBatch, EngineError> {
    let mut columns: Vec<ArrayRef> = key
        .iter()
        .map(|&i| new_empty_array(schema.field(i).data_type()))
        .collect();
    columns.push(Arc::new(Int64Array::from(Vec::<i64>::new())));
    columns.push(Arc::new(Int64Array::from(Vec::<i64>::new())));
    let batch = RecordBatch::try_new(fields, columns)?;
    let diff: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
    Ok(ZSetBatch::new(batch, diff)?)
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
