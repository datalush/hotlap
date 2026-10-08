//! Z-set -> Arrow `RecordBatch` conversion for the append sink.

use arrow::array::{ArrayRef, Int64Array, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use hotlap::ZSetBatch;

use crate::error::ConnectorError;

use super::sink::retraction_check;

/// Build one Arrow batch, repeating each row `diff` times (append multiplicity).
pub(crate) fn rows_to_batch(
    schema: &SchemaRef,
    batch: &ZSetBatch,
) -> Result<RecordBatch, ConnectorError> {
    crate::convert::ensure_supported(schema)?;
    retraction_check(batch)?;
    let indices = expand_indices(batch)?;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(batch.batch.num_columns());
    for column in batch.batch.columns() {
        arrays.push(take(column.as_ref(), &indices, None).map_err(arrow_err)?);
    }
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|error| ConnectorError::Arrow(format!("record batch: {error}")))
}

/// Row indices implied by the non-negative multiplicities (diff 0 is dropped).
fn expand_indices(batch: &ZSetBatch) -> Result<UInt32Array, ConnectorError> {
    let diffs = batch
        .diff
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ConnectorError::Arrow("diff column must be Int64".into()))?;
    let mut indices: Vec<u32> = Vec::new();
    for row in 0..batch.len() {
        for _ in 0..diffs.value(row) {
            indices.push(row as u32);
        }
    }
    Ok(UInt32Array::from(indices))
}

fn arrow_err(error: arrow::error::ArrowError) -> ConnectorError {
    ConnectorError::Arrow(error.to_string())
}
