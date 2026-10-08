use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, UInt32Array};
use arrow::compute::{concat, concat_batches, take};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::RowConverter;

use crate::batch::ZSetBatch;
use crate::error::EngineError;
use crate::keys::KeyConverter;
use crate::zset::{consolidate_with, int64_diffs};

/// Distinct join-key bytes present in the delta `z`, in first-seen order.
///
/// `converter` is the caller's cached key converter for `z`'s schema.
pub(super) fn touched_keys(
    z: &ZSetBatch,
    converter: &KeyConverter,
) -> Result<Vec<Vec<u8>>, EngineError> {
    if z.is_empty() {
        return Ok(Vec::new());
    }
    let rows = converter.convert(z.batch.columns())?;
    let mut seen = HashSet::new();
    let mut touched = Vec::new();
    for index in 0..rows.num_rows() {
        let bytes = rows.row(index).as_ref().to_vec();
        if seen.insert(bytes.clone()) {
            touched.push(bytes);
        }
    }
    Ok(touched)
}

/// Distinct keys touched by both deltas, de-duplicated across the two sides.
pub(super) fn touched_union(
    left: &ZSetBatch,
    left_conv: &KeyConverter,
    right: &ZSetBatch,
    right_conv: &KeyConverter,
) -> Result<Vec<Vec<u8>>, EngineError> {
    let mut touched = touched_keys(left, left_conv)?;
    let mut seen: HashSet<Vec<u8>> = touched.iter().cloned().collect();
    for key in touched_keys(right, right_conv)? {
        if seen.insert(key.clone()) {
            touched.push(key);
        }
    }
    Ok(touched)
}

/// Builds the joined output schema as `left fields ++ right fields`.
pub(super) fn joined_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = left
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.extend(right.fields().iter().map(|field| field.as_ref().clone()));
    Arc::new(Schema::new(fields))
}

/// Concatenates per-key deltas over a shared schema.
pub(super) fn concat_zsets(
    schema: &SchemaRef,
    parts: &[ZSetBatch],
) -> Result<ZSetBatch, EngineError> {
    if parts.is_empty() {
        return Ok(ZSetBatch::empty(schema.clone()));
    }
    let batches: Vec<&RecordBatch> = parts.iter().map(|z| &z.batch).collect();
    let batch = concat_batches(schema, batches)?;
    let diffs: Vec<&dyn Array> = parts.iter().map(|z| z.diff.as_ref()).collect();
    let diff = concat(&diffs)?;
    Ok(ZSetBatch::new(batch, diff)?)
}

/// Returns `current - previous` as a consolidated Z-set.
///
/// `converter` is the caller's cached full-row converter for the joined schema.
pub(super) fn subtract(
    current: &ZSetBatch,
    previous: &ZSetBatch,
    converter: &RowConverter,
) -> Result<ZSetBatch, EngineError> {
    let batch = concat_batches(&current.schema(), [&current.batch, &previous.batch])?;
    let retracted = negate(previous.diff())?;
    let diff = concat(&[current.diff().as_ref(), retracted.as_ref()])?;
    consolidate_with(converter, &ZSetBatch::new(batch, diff)?)
}

/// Inner equi-joins two Z-sets on the given key columns, also returning the
/// number of joined pairs evaluated (both sides share the key by construction).
///
/// The key converters come from the caller's cache, keyed by each side schema.
pub(super) fn equi_join(
    left: &ZSetBatch,
    left_conv: &KeyConverter,
    right: &ZSetBatch,
    right_conv: &KeyConverter,
    out_conv: &RowConverter,
) -> Result<(ZSetBatch, usize), EngineError> {
    let left_rows = left_conv.convert(left.batch.columns())?;
    let right_rows = right_conv.convert(right.batch.columns())?;

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
    let pairs = matches.len();
    Ok((materialize(left, right, &matches, out_conv)?, pairs))
}

/// Builds the joined Z-set for the `(left index, right index)` match pairs.
fn materialize(
    left: &ZSetBatch,
    right: &ZSetBatch,
    matches: &[(usize, usize)],
    out_conv: &RowConverter,
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
    let mut diffs: Vec<i64> = Vec::with_capacity(matches.len());
    for &(left_index, right_index) in matches {
        let product = left_diffs
            .value(left_index)
            .checked_mul(right_diffs.value(right_index))
            .ok_or_else(|| {
                EngineError::Infrastructure("join diff product overflowed i64".to_string())
            })?;
        diffs.push(product);
    }
    consolidate_with(out_conv, &ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs)))?)
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
