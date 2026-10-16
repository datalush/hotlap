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
mod operator_diagnostic_tests {
    use std::hint::black_box;
    use std::sync::Arc;
    use std::time::Instant;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::keys::full_converter;
    use crate::ops::join::test_support::{self, LegacyDenseJoinGuard};

    const SIDE_ROWS: usize = 128;
    const ITERATIONS: usize = 16;

    fn side(name: &str, offset: i64) -> ZSetBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new(name, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64; SIDE_ROWS])) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(
                    (0..SIDE_ROWS).map(|row| offset + row as i64),
                )) as ArrayRef,
            ],
        )
        .unwrap();
        ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1_i64; SIDE_ROWS]))).unwrap()
    }

    fn assert_output(output: &ZSetBatch) {
        assert_eq!(output.len(), SIDE_ROWS * SIDE_ROWS);
        assert_eq!(output.schema().fields().len(), 4);
        for field in output.schema().fields() {
            assert_eq!(field.data_type(), &DataType::Int64);
        }
        let diffs = output.diff().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(diffs.null_count(), 0);
        assert!(diffs.values().iter().all(|&diff| diff == 1));
    }

    #[test]
    #[ignore = "operator-only 16,384-pair diagnostic; one-shot smoke, not a performance acceptance test"]
    fn high_fanout_equi_join_clock_smoke() {
        let left = side("left_payload", 0);
        let right = side("right_payload", 10_000);
        let left_conv = KeyConverter::new(left.schema().as_ref(), &[0]).unwrap();
        let right_conv = KeyConverter::new(right.schema().as_ref(), &[0]).unwrap();
        let output_schema = joined_schema(&left.schema(), &right.schema());
        let output_conv = full_converter(output_schema.as_ref()).unwrap();

        let run = |legacy: bool| {
            let _legacy = legacy.then(LegacyDenseJoinGuard::force);
            let warmup = equi_join(&left, &left_conv, &right, &right_conv, &output_conv).unwrap();
            assert_eq!(warmup.1, SIDE_ROWS * SIDE_ROWS);
            assert_output(&warmup.0);
            drop(warmup);
            test_support::reset_counts();
            let started = Instant::now();
            for _ in 0..ITERATIONS {
                let (output, _) =
                    equi_join(&left, &left_conv, &right, &right_conv, &output_conv).unwrap();
                black_box(output);
            }
            let elapsed = started.elapsed().as_nanos();
            (elapsed, test_support::counts())
        };
        let legacy_first = std::env::var_os("W3HW_LEGACY_FIRST").is_some();
        let (direct_result, legacy_result) = if legacy_first {
            let legacy = run(true);
            let direct = run(false);
            (direct, legacy)
        } else {
            (run(false), run(true))
        };
        let (direct_ns, direct_routes) = direct_result;
        let (legacy_ns, legacy_routes) = legacy_result;
        assert_eq!(direct_routes.direct, ITERATIONS);
        assert_eq!(direct_routes.forced_legacy, 0);
        assert_eq!(
            direct_routes.direct_pairs,
            ITERATIONS * SIDE_ROWS * SIDE_ROWS
        );

        assert_eq!(legacy_routes.forced_legacy, ITERATIONS);
        assert_eq!(legacy_routes.direct, 0);
        assert_eq!(
            legacy_routes.forced_legacy_pairs,
            ITERATIONS * SIDE_ROWS * SIDE_ROWS
        );
        println!(
            "operator_high_fanout legacy_first={} pairs_per_call={} iterations={} direct_total_ns={} direct_ns_per_call={} legacy_total_ns={} legacy_ns_per_call={} direct_actual_pairs={} legacy_actual_pairs={}",
            legacy_first,
            SIDE_ROWS * SIDE_ROWS,
            ITERATIONS,
            direct_ns,
            direct_ns / ITERATIONS as u128,
            legacy_ns,
            legacy_ns / ITERATIONS as u128,
            direct_routes.direct_pairs,
            legacy_routes.forced_legacy_pairs,
        );
    }
}
