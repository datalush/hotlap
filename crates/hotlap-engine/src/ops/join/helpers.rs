use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
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

    #[cfg(test)]
    let force_legacy = super::test_support::force_legacy();
    #[cfg(not(test))]
    let force_legacy = false;
    let direct_eligible = !force_legacy
        && take_indices_fit(left_rows.num_rows())
        && take_indices_fit(right_rows.num_rows());
    #[cfg(test)]
    if super::test_support::force_owned_keys() {
        let mut buckets: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        for index in 0..left_rows.num_rows() {
            buckets
                .entry(left_rows.row(index).as_ref().to_vec())
                .or_default()
                .push(index);
        }
        #[cfg(test)]
        super::test_support::record_owned_key_bucket();
        evaluate_dense_buckets(
            left,
            right,
            &buckets,
            &right_rows,
            out_conv,
            direct_eligible,
            force_legacy,
        )
    } else {
        let buckets = borrowed_key_buckets(&left_rows);
        #[cfg(test)]
        super::test_support::record_borrowed_key_bucket();
        evaluate_dense_buckets(
            left,
            right,
            &buckets,
            &right_rows,
            out_conv,
            direct_eligible,
            force_legacy,
        )
    }
    #[cfg(not(test))]
    {
        let buckets = borrowed_key_buckets(&left_rows);
        evaluate_dense_buckets(
            left,
            right,
            &buckets,
            &right_rows,
            out_conv,
            direct_eligible,
            force_legacy,
        )
    }
}

fn borrowed_key_buckets(rows: &arrow::row::Rows) -> HashMap<&[u8], Vec<usize>> {
    let mut buckets = HashMap::new();
    for index in 0..rows.num_rows() {
        buckets
            .entry(rows.row(index).data())
            .or_insert_with(Vec::new)
            .push(index);
    }
    buckets
}

fn evaluate_dense_buckets<K>(
    left: &ZSetBatch,
    right: &ZSetBatch,
    buckets: &HashMap<K, Vec<usize>>,
    right_rows: &arrow::row::Rows,
    out_conv: &RowConverter,
    direct_eligible: bool,
    force_legacy: bool,
) -> Result<(ZSetBatch, usize), EngineError>
where
    K: Borrow<[u8]> + Eq + Hash,
{
    #[cfg(not(test))]
    let _ = force_legacy;
    if direct_eligible {
        let (left_indices, right_indices) = direct_pair_indices(buckets, right_rows);
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
            if let Some(left_indices) = buckets.get(right_rows.row(right_index).data()) {
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

fn direct_pair_indices<K>(
    buckets: &HashMap<K, Vec<usize>>,
    right_rows: &arrow::row::Rows,
) -> (Vec<u32>, Vec<u32>)
where
    K: Borrow<[u8]> + Eq + Hash,
{
    let mut left_indices = Vec::new();
    let mut right_indices = Vec::new();
    for right_index in 0..right_rows.num_rows() {
        if let Some(bucket) = buckets.get(right_rows.row(right_index).data()) {
            for &left_index in bucket {
                // The caller's row-count guard proves these casts are lossless.
                left_indices.push(left_index as u32);
                right_indices.push(right_index as u32);
            }
        }
    }
    (left_indices, right_indices)
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

#[cfg(test)]
mod index_width_tests {
    use super::take_indices_fit;

    #[test]
    fn take_index_eligibility_includes_u32_max_without_wrapping() {
        let max_index = usize::try_from(u32::MAX).unwrap();
        assert!(take_indices_fit(0));
        assert!(take_indices_fit(max_index));
        if let Some(rows) = max_index.checked_add(1) {
            assert!(take_indices_fit(rows));
            if let Some(rows) = rows.checked_add(1) {
                assert!(!take_indices_fit(rows));
            }
        }
    }
}

#[cfg(test)]
mod error_order_tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::keys::full_converter;
    use crate::ops::join::test_support::LegacyDenseJoinGuard;

    fn side(name: &str, diff: ArrayRef) -> ZSetBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
        )
        .unwrap();
        let _ = name;
        ZSetBatch::new(batch, diff).unwrap()
    }

    fn materialize_error(left: &ZSetBatch, right: &ZSetBatch) -> String {
        let left_conv = KeyConverter::new(left.schema().as_ref(), &[0]).unwrap();
        let right_conv = KeyConverter::new(right.schema().as_ref(), &[0]).unwrap();
        let out = joined_schema(&left.schema(), &right.schema());
        let out_conv = full_converter(out.as_ref()).unwrap();
        match equi_join(left, &left_conv, right, &right_conv, &out_conv) {
            Ok(_) => panic!("expected malformed diff dtype to fail"),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn both_routes_validate_left_diff_dtype_before_right_diff_dtype() {
        let bad_left = side("left", Arc::new(Float64Array::from(vec![1.0])));
        let bad_right = side("right", Arc::new(Float64Array::from(vec![2.0])));
        let good = side("good", Arc::new(Int64Array::from(vec![1_i64])));
        let left_first = materialize_error(&bad_left, &good);
        let right_only = materialize_error(&good, &bad_right);
        assert!(left_first.contains("diff column must be integer"));
        assert!(right_only.contains("diff column must be integer"));

        let _legacy = LegacyDenseJoinGuard::force();
        assert_eq!(materialize_error(&bad_left, &good), left_first);
        assert_eq!(materialize_error(&good, &bad_right), right_only);
        assert_eq!(materialize_error(&bad_left, &bad_right), left_first);
    }

    #[test]
    fn both_routes_keep_empty_no_pair_diff_validation() {
        let invalid = side("invalid", Arc::new(Float64Array::from(vec![1.0])));
        let empty = ZSetBatch::empty(invalid.schema());
        let direct = materialize_error(&invalid, &empty);
        let _legacy = LegacyDenseJoinGuard::force();
        assert_eq!(materialize_error(&invalid, &empty), direct);
        assert!(direct.contains("diff column must be integer"));
    }
}

#[cfg(test)]
#[path = "borrowed_key_tests.rs"]
mod borrowed_key_tests;

#[cfg(test)]
#[path = "operator_diagnostic_tests.rs"]
mod operator_diagnostic_tests;
