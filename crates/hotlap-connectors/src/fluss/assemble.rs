//! Arrow assembly for the Fluss source: concatenate per-record slices and
//! append the `_event_time` (`Int64`, ms) column.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::error::ConnectorError;

use super::log_reader::Rec;

/// Name of the event-time column surfaced to the engine.
pub(crate) const EVENT_TIME: &str = "_event_time";

/// Concatenate one-row slices and append the `_event_time` (`Int64`, ms) column.
pub(crate) fn assemble(
    records: &[Rec],
    base_schema: &SchemaRef,
    full_schema: &SchemaRef,
) -> Result<RecordBatch, ConnectorError> {
    let mut slices = Vec::with_capacity(records.len());
    let mut timestamps = Vec::with_capacity(records.len());
    for record in records {
        let batch = record
            .row
            .get_record_batch()
            .ok_or_else(|| ConnectorError::Arrow("record row has no backing batch".into()))?;
        slices.push(batch.slice(record.row.get_row_id(), 1));
        timestamps.push(record.timestamp);
    }
    let data = concat_batches(base_schema, &slices).map_err(arrow_err)?;
    let mut columns = data.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(timestamps)) as ArrayRef);
    RecordBatch::try_new(full_schema.clone(), columns).map_err(arrow_err)
}

/// Append the non-nullable `_event_time` field to a base schema.
pub(crate) fn with_event_time(base: &Schema) -> SchemaRef {
    let mut fields: Vec<_> = base.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(EVENT_TIME, DataType::Int64, false)));
    Arc::new(Schema::new(fields))
}

fn arrow_err(error: arrow::error::ArrowError) -> ConnectorError {
    ConnectorError::Arrow(error.to_string())
}
