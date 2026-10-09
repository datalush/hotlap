//! Engine `ZSetBatch` -> Arrow `RecordBatch` using a view schema.

use arrow::array::{Array, ArrayRef, Int64Array, UInt32Array};
use arrow::compute::{cast, take};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hotlap::ZSetBatch;

use crate::error::SqlError;

/// Maximum number of bag rows materialized from a single view snapshot.
///
/// A consolidated snapshot stores one row per distinct value with a signed
/// multiplicity; the SQL surface expands each positive multiplicity into that
/// many rows. This bound caps the expansion so an oversized snapshot fails fast
/// with a typed error instead of exhausting memory.
const MAX_SNAPSHOT_ROWS: usize = 1_048_576;

/// Reject schemas with columns the kernel cannot represent.
pub fn ensure_kernel_types(schema: &Schema) -> Result<(), SqlError> {
    for f in schema.fields() {
        match f.data_type() {
            DataType::Int64
            | DataType::Int32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Boolean => {}
            other => {
                return Err(SqlError::Unsupported(format!(
                    "column `{}` has type {other:?}, not representable by the kernel",
                    f.name()
                )));
            }
        }
    }
    Ok(())
}

/// Build one Arrow batch from a consolidated Z-set, materializing the SQL bag.
///
/// The engine snapshot is already consolidated: at most one row per distinct
/// value, sorted, with zero weights dropped. A positive weight `w` means the row
/// occurs `w` times, so the batch repeats it `w` times. This is standard SQL bag
/// semantics with no implicit `DISTINCT`. A negative weight cannot describe a
/// bag and is rejected. The expanded row count is bounded ([`MAX_SNAPSHOT_ROWS`])
/// with overflow-checked arithmetic before any index is allocated.
pub fn zset_to_batch(schema: &SchemaRef, zset: &ZSetBatch) -> Result<RecordBatch, SqlError> {
    ensure_kernel_types(schema)?;
    if zset.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let weights = row_weights(zset.diff())?;
    let indices = expanded_indices(&weights)?;
    let columns: Vec<ArrayRef> = zset
        .batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None).map_err(bad))
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(bad)
}

/// Signed multiplicity per row, rejecting values below zero.
fn row_weights(diff: &ArrayRef) -> Result<Vec<i64>, SqlError> {
    let casted = cast(diff.as_ref(), &DataType::Int64).map_err(bad)?;
    let values = casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| SqlError::Unsupported("diff column is not int64".into()))?;
    let mut weights = Vec::with_capacity(values.len());
    for index in 0..values.len() {
        let weight = values.value(index);
        if weight < 0 {
            return Err(SqlError::Unsupported(format!(
                "snapshot weight {weight} is negative and has no SQL bag meaning"
            )));
        }
        weights.push(weight);
    }
    Ok(weights)
}

/// Repeat each row index by its weight, after bounding the total expansion.
///
/// The total is summed with overflow-checked arithmetic and compared against
/// [`MAX_SNAPSHOT_ROWS`] before the index vector is allocated.
fn expanded_indices(weights: &[i64]) -> Result<UInt32Array, SqlError> {
    let mut total: i64 = 0;
    for &weight in weights {
        total = total.checked_add(weight).ok_or_else(|| {
            SqlError::Unsupported("snapshot expansion overflowed the row count".into())
        })?;
    }
    if total > MAX_SNAPSHOT_ROWS as i64 {
        return Err(SqlError::Unsupported(format!(
            "snapshot expands to {total} rows, over the {MAX_SNAPSHOT_ROWS} row limit"
        )));
    }
    let mut indices: Vec<u32> = Vec::with_capacity(total as usize);
    for (index, &weight) in weights.iter().enumerate() {
        let index = u32::try_from(index)
            .map_err(|_| SqlError::Unsupported("snapshot row index exceeds u32".into()))?;
        indices.extend(std::iter::repeat_n(index, weight as usize));
    }
    Ok(UInt32Array::from(indices))
}

fn bad(error: impl std::fmt::Display) -> SqlError {
    SqlError::Unsupported(format!("record batch: {error}"))
}

#[cfg(test)]
mod tests;
