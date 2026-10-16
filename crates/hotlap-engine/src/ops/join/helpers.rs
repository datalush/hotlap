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
    #[cfg(test)]
    let force_legacy = super::test_support::force_legacy();
    #[cfg(not(test))]
    let force_legacy = false;
    if !force_legacy
        && take_indices_fit(left_rows.num_rows())
        && take_indices_fit(right_rows.num_rows())
    {
        let mut left_indices = Vec::new();
        let mut right_indices = Vec::new();
        for right_index in 0..right_rows.num_rows() {
            if let Some(bucket) = buckets.get(right_rows.row(right_index).as_ref()) {
                for &left_index in bucket {
                    // The row-count guard above proves these casts are lossless.
                    left_indices.push(left_index as u32);
                    right_indices.push(right_index as u32);
                }
            }
        }
        let pairs = left_indices.len();
        #[cfg(test)]
        super::test_support::record_direct(pairs);
        let joined = materialize_indices(
            left,
            right,
            UInt32Array::from(left_indices),
            UInt32Array::from(right_indices),
            out_conv,
            DiffIndices::Narrow,
        )?;
        Ok((joined, pairs))
    } else {
        let mut matches: Vec<(usize, usize)> = Vec::new();
        for right_index in 0..right_rows.num_rows() {
            if let Some(left_indices) = buckets.get(right_rows.row(right_index).as_ref()) {
                matches.extend(
                    left_indices
                        .iter()
                        .map(|&left_index| (left_index, right_index)),
                );
            }
        }
        let pairs = matches.len();
        #[cfg(test)]
        if force_legacy {
            super::test_support::record_forced_legacy(pairs);
        } else {
            super::test_support::record_wide_fallback(pairs);
        }
        Ok((materialize(left, right, &matches, out_conv)?, pairs))
    }
}

/// Whether every row position in a side is representable by Arrow's UInt32 take index.
fn take_indices_fit(rows: usize) -> bool {
    rows == 0 || u32::try_from(rows - 1).is_ok()
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
    materialize_indices(
        left,
        right,
        left_indices,
        right_indices,
        out_conv,
        DiffIndices::Wide(matches),
    )
}

enum DiffIndices<'a> {
    Wide(&'a [(usize, usize)]),
    Narrow,
}

fn materialize_indices(
    left: &ZSetBatch,
    right: &ZSetBatch,
    left_indices: UInt32Array,
    right_indices: UInt32Array,
    out_conv: &RowConverter,
    diff_indices: DiffIndices<'_>,
) -> Result<ZSetBatch, EngineError> {
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
    let pair_count = left_indices.len();
    let mut diffs: Vec<i64> = Vec::with_capacity(pair_count);
    for pair_index in 0..pair_count {
        let (left_index, right_index) = match &diff_indices {
            DiffIndices::Wide(matches) => matches[pair_index],
            DiffIndices::Narrow => (
                left_indices.value(pair_index) as usize,
                right_indices.value(pair_index) as usize,
            ),
        };
        let product = left_diffs
            .value(left_index)
            .checked_mul(right_diffs.value(right_index))
            .ok_or(EngineError::NumericOverflow("join diff product"))?;
        diffs.push(product);
    }
    consolidate_with(
        out_conv,
        &ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs)))?,
    )
}

/// Applies `indices` to every column of `batch`.
fn take_batch(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch, EngineError> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        columns.push(take(column.as_ref(), indices, None)?);
    }
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}
