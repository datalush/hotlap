//! Kernel `Row` -> Arrow `RecordBatch` conversion for the append sink.

use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use hotlap::{ChangeBatch, Row, Scalar};

use crate::error::ConnectorError;

use super::sink::retraction_check;

/// Build one Arrow batch, repeating each row `diff` times (append multiplicity).
pub(crate) fn rows_to_batch(
    schema: &SchemaRef,
    batch: &ChangeBatch,
) -> Result<RecordBatch, ConnectorError> {
    retraction_check(batch)?;
    let rows = expand(batch);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (col, field) in schema.fields().iter().enumerate() {
        arrays.push(build_column(field.data_type(), col, &rows)?);
    }
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|error| ConnectorError::Arrow(format!("record batch: {error}")))
}

/// Flatten a change batch into one row per multiplicity (diff 0 is dropped).
fn expand(batch: &ChangeBatch) -> Vec<&Row> {
    batch
        .rows
        .iter()
        .flat_map(|(row, diff)| std::iter::repeat_n(row, usize::try_from(*diff).unwrap_or(0)))
        .collect()
}

fn build_column(
    data_type: &DataType,
    col: usize,
    rows: &[&Row],
) -> Result<ArrayRef, ConnectorError> {
    let array: ArrayRef = match data_type {
        DataType::Int64 | DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let typed = collect(rows, col, |value| match value {
                Some(Scalar::I64(inner)) => Ok(Some(*inner)),
                Some(Scalar::Null) | None => Ok(None),
                other => Err(mismatch(data_type, other)),
            })?;
            std::sync::Arc::new(Int64Array::from(typed))
        }
        DataType::Utf8 => {
            let typed = collect(rows, col, |value| match value {
                Some(Scalar::Str(inner)) => Ok(Some(inner.clone())),
                Some(Scalar::Null) | None => Ok(None),
                other => Err(mismatch(data_type, other)),
            })?;
            std::sync::Arc::new(StringArray::from(typed))
        }
        DataType::Boolean => {
            let typed = collect(rows, col, |value| match value {
                Some(Scalar::Bool(inner)) => Ok(Some(*inner)),
                Some(Scalar::Null) | None => Ok(None),
                other => Err(mismatch(data_type, other)),
            })?;
            std::sync::Arc::new(BooleanArray::from(typed))
        }
        other => {
            return Err(ConnectorError::Unsupported(format!(
                "unsupported column type {other:?}"
            )));
        }
    };
    // Timestamps are stored as epoch milliseconds but must carry the declared
    // Arrow type; cast the freshly built Int64 array to it.
    if matches!(data_type, DataType::Timestamp(..)) {
        return arrow::compute::cast(array.as_ref(), data_type)
            .map_err(|error| ConnectorError::Arrow(format!("timestamp cast: {error}")));
    }
    Ok(array)
}

/// Map a column's cells into a typed `Option` vector.
fn collect<T, F>(rows: &[&Row], col: usize, map: F) -> Result<Vec<Option<T>>, ConnectorError>
where
    F: Fn(Option<&Scalar>) -> Result<Option<T>, ConnectorError>,
{
    rows.iter().map(|row| map(row.0.get(col))).collect()
}

fn mismatch(data_type: &DataType, scalar: Option<&Scalar>) -> ConnectorError {
    ConnectorError::Arrow(format!(
        "value {scalar:?} does not match column type {data_type:?}"
    ))
}
