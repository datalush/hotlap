use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, UInt32Array};
use arrow::compute::{concat, concat_batches, take};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::arrange::KeyedArrangement;
use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::KeyConverter;
use crate::zset::{consolidate, int64_diffs};

/// Materializes both arrangements and joins their current relations.
pub(super) fn snapshot(
    left: &KeyedArrangement,
    left_keys: &[usize],
    right: &KeyedArrangement,
    right_keys: &[usize],
) -> Result<ZSetBatch, EngineError> {
    let left = left.to_zset()?;
    let right = right.to_zset()?;
    equi_join(&left, left_keys, &right, right_keys)
}

/// Returns `current - previous` as a consolidated Z-set.
pub(super) fn subtract(
    current: &ZSetBatch,
    previous: &ZSetBatch,
) -> Result<ZSetBatch, EngineError> {
    let batch = concat_batches(&current.schema(), [&current.batch, &previous.batch])?;
    let retracted = negate(previous.diff())?;
    let diff = concat(&[current.diff().as_ref(), retracted.as_ref()])?;
    consolidate(&ZSetBatch::new(batch, diff)?)
}

/// Inner equi-joins two Z-sets on the given key columns.
///
/// Matching uses `arrow::row` bytes, so equal keys compare byte-wise. The result
/// is consolidated by full joined row, summing the sides' diff products.
fn equi_join(
    left: &ZSetBatch,
    left_keys: &[usize],
    right: &ZSetBatch,
    right_keys: &[usize],
) -> Result<ZSetBatch, EngineError> {
    let left_rows =
        KeyConverter::new(left.schema().as_ref(), left_keys)?.convert(left.batch.columns())?;
    let right_rows =
        KeyConverter::new(right.schema().as_ref(), right_keys)?.convert(right.batch.columns())?;

    let mut buckets: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    for index in 0..left_rows.num_rows() {
        buckets
            .entry(left_rows.row(index).as_ref().to_vec())
            .or_default()
            .push(index);
    }
    let mut matches: Vec<(usize, usize)> = Vec::new();
    for index in 0..right_rows.num_rows() {
        if let Some(left_indices) = buckets.get(right_rows.row(index).as_ref()) {
            matches.extend(left_indices.iter().map(|&left_index| (left_index, index)));
        }
    }
    materialize(left, right, &matches)
}

/// Builds the joined Z-set for the `(left index, right index)` match pairs.
fn materialize(
    left: &ZSetBatch,
    right: &ZSetBatch,
    matches: &[(usize, usize)],
) -> Result<ZSetBatch, EngineError> {
    let left_indices = UInt32Array::from(
        matches
            .iter()
            .map(|&(left_index, _)| left_index as u32)
            .collect::<Vec<u32>>(),
    );
    let right_indices = UInt32Array::from(
        matches
            .iter()
            .map(|&(_, right_index)| right_index as u32)
            .collect::<Vec<u32>>(),
    );
    let left_taken = take_batch(&left.batch, &left_indices)?;
    let right_taken = take_batch(&right.batch, &right_indices)?;

    let mut fields: Vec<Field> = left
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.extend(
        right
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone()),
    );
    let mut columns: Vec<ArrayRef> = left_taken.columns().to_vec();
    columns.extend(right_taken.columns().iter().cloned());
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;

    let left_diffs = int64_diffs(&left.diff)?;
    let right_diffs = int64_diffs(&right.diff)?;
    let diffs: Vec<i64> = matches
        .iter()
        .map(|&(left_index, right_index)| {
            left_diffs.value(left_index) * right_diffs.value(right_index)
        })
        .collect();
    consolidate(&ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs)))?)
}

/// Applies `indices` to every column of `batch`.
fn take_batch(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch, EngineError> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        columns.push(take(column.as_ref(), indices, None)?);
    }
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// Negates an integer diff column, preserving its length.
fn negate(diff: &ArrayRef) -> Result<ArrayRef, EngineError> {
    let ints = int64_diffs(diff)?;
    let negated: Int64Array = ints.iter().map(|value| value.map(|v| -v)).collect();
    Ok(Arc::new(negated))
}
