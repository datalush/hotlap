use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
#[cfg(feature = "bench-allocation-observer")]
use stats_alloc::{INSTRUMENTED_SYSTEM, Region};

use super::{equi_join, joined_schema};
use crate::batch::ZSetBatch;
use crate::keys::{KeyConverter, full_converter};
use crate::ops::join::test_support::{self, OwnedDenseJoinKeyGuard};

#[cfg(feature = "bench-allocation-observer")]
const LEFT_ROWS: usize = 256;

#[cfg(feature = "bench-allocation-observer")]
fn batch(name: &str, keys: &[String]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new(name, DataType::Int64, false),
    ]));
    let payload = (0..keys.len()).map(|index| index as i64);
    let record = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                keys.iter().map(String::as_str),
            )) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(payload)) as ArrayRef,
        ],
    )
    .unwrap();
    ZSetBatch::new(record, Arc::new(Int64Array::from(vec![1_i64; keys.len()]))).unwrap()
}

#[cfg(feature = "bench-allocation-observer")]
fn owned_key_direct_control(
    left: &ZSetBatch,
    left_conv: &KeyConverter,
    right: &ZSetBatch,
    right_conv: &KeyConverter,
    output_conv: &arrow::row::RowConverter,
) -> (ZSetBatch, usize) {
    let _owned_keys = OwnedDenseJoinKeyGuard::force();
    equi_join(left, left_conv, right, right_conv, output_conv).unwrap()
}

#[cfg(feature = "bench-allocation-observer")]
fn assert_exact_dense_output(output: &ZSetBatch, left_expected: &[i64], right_expected: &[i64]) {
    assert_eq!(output.len(), left_expected.len());
    assert_eq!(right_expected.len(), left_expected.len());
    let left_values = output
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let right_values = output
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diffs = output.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    for index in 0..left_expected.len() {
        assert_eq!(left_values.value(index), left_expected[index]);
        assert_eq!(right_values.value(index), right_expected[index]);
        assert_eq!(diffs.value(index), 1);
    }
}

#[cfg(feature = "bench-allocation-observer")]
fn compare_owned_and_borrowed_key_modes(
    left_keys: Vec<String>,
    right_keys: Vec<String>,
    expected_left: &[i64],
    expected_right: &[i64],
) {
    let left = batch("left_payload", &left_keys);
    let right = batch("right_payload", &right_keys);
    let left_conv = KeyConverter::new(left.schema().as_ref(), &[0]).unwrap();
    let right_conv = KeyConverter::new(right.schema().as_ref(), &[0]).unwrap();
    let output_schema = joined_schema(&left.schema(), &right.schema());
    let output_conv = full_converter(output_schema.as_ref()).unwrap();

    test_support::reset_counts();
    let candidate_region = Region::new(&INSTRUMENTED_SYSTEM);
    let (candidate, candidate_pairs) =
        equi_join(&left, &left_conv, &right, &right_conv, &output_conv).unwrap();
    let candidate_bytes = candidate_region.change().bytes_allocated;
    assert_exact_dense_output(&candidate, expected_left, expected_right);
    let candidate_routes = test_support::counts();

    test_support::reset_counts();
    let reference_region = Region::new(&INSTRUMENTED_SYSTEM);
    let (reference, reference_pairs) =
        owned_key_direct_control(&left, &left_conv, &right, &right_conv, &output_conv);
    let reference_bytes = reference_region.change().bytes_allocated;
    assert_exact_dense_output(&reference, expected_left, expected_right);
    let reference_routes = test_support::counts();

    assert_eq!(candidate_pairs, expected_left.len());
    assert_eq!(reference_pairs, expected_left.len());
    assert_eq!(candidate_routes.direct, 1);
    assert_eq!(candidate_routes.forced_legacy, 0);
    assert_eq!(candidate_routes.borrowed_key_buckets, 1);
    assert_eq!(candidate_routes.owned_key_buckets, 0);
    assert_eq!(reference_routes.direct, 1);
    assert_eq!(reference_routes.forced_legacy, 0);
    assert_eq!(reference_routes.borrowed_key_buckets, 0);
    assert_eq!(reference_routes.owned_key_buckets, 1);
    assert!(
        candidate_bytes < reference_bytes,
        "borrowed-key candidate must allocate less than owned-key/direct-index control: candidate={candidate_bytes} reference={reference_bytes}"
    );
    println!(
        "borrowed_key_allocations rows={} pairs={} encoded_key_bytes={} candidate_bytes={} owned_key_direct_bytes={} saved_bytes={}",
        left_keys.len(),
        candidate_pairs,
        left_conv
            .convert(left.batch.columns())
            .unwrap()
            .row(0)
            .data()
            .len(),
        candidate_bytes,
        reference_bytes,
        reference_bytes - candidate_bytes,
    );
}

#[test]
#[cfg(feature = "bench-allocation-observer")]
#[ignore = "isolated process allocator comparison for owned-key reference vs production borrowed keys"]
fn borrowed_key_bucket_reduces_dense_join_allocation_traffic() {
    let key = format!("{}-shared-key", "long-encoded-join-key".repeat(16));
    compare_owned_and_borrowed_key_modes(
        vec![key.clone(); LEFT_ROWS],
        vec![key],
        &(0..LEFT_ROWS as i64).collect::<Vec<_>>(),
        &vec![0; LEFT_ROWS],
    );
}

#[test]
#[cfg(feature = "bench-allocation-observer")]
#[ignore = "isolated process allocator control for all-distinct encoded join keys"]
fn borrowed_key_bucket_all_distinct_control_preserves_matches() {
    let left_keys = (0..LEFT_ROWS)
        .map(|row| format!("{}-{row:04}", "distinct-composite-key".repeat(8)))
        .collect::<Vec<_>>();
    let right_keys = vec![left_keys[LEFT_ROWS - 1].clone(), left_keys[17].clone()];
    compare_owned_and_borrowed_key_modes(
        left_keys,
        right_keys,
        &[17, LEFT_ROWS as i64 - 1],
        &[1, 0],
    );
}

type CompositeRow = (
    Option<String>,
    Option<i64>,
    i64,
    Option<String>,
    Option<i64>,
    i64,
);

fn nullable_composite_batch(
    name: &str,
    rows: &[(Option<&str>, Option<i64>, i64)],
    diffs: &[i64],
) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("text_key", DataType::Utf8, true),
        Field::new("number_key", DataType::Int64, true),
        Field::new(name, DataType::Int64, false),
    ]));
    let record = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))) as ArrayRef,
        ],
    )
    .unwrap();
    ZSetBatch::new(record, Arc::new(Int64Array::from(diffs.to_vec()))).unwrap()
}

fn composite_output_bag(output: &ZSetBatch) -> BTreeMap<CompositeRow, i64> {
    let left_text = output
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let left_number = output
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let left_payload = output
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let right_text = output
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let right_number = output
        .batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let right_payload = output
        .batch
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diffs = output.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    (0..output.len())
        .map(|row| {
            (
                (
                    left_text
                        .is_valid(row)
                        .then(|| left_text.value(row).to_owned()),
                    left_number.is_valid(row).then(|| left_number.value(row)),
                    left_payload.value(row),
                    right_text
                        .is_valid(row)
                        .then(|| right_text.value(row).to_owned()),
                    right_number.is_valid(row).then(|| right_number.value(row)),
                    right_payload.value(row),
                ),
                diffs.value(row),
            )
        })
        .collect()
}

#[test]
fn borrowed_and_owned_key_modes_preserve_nullable_composite_join_bag() {
    let left_rows = [
        (None, Some(1), 100),
        (Some("repeated-composite-key"), Some(2), 101),
        (Some("repeated-composite-key"), Some(2), 102),
        (None, None, 103),
        (Some("nonmatch"), None, 104),
    ];
    let left_diffs = [2, -1, 3, -1, 1];
    let right_rows = [
        (None, Some(1), 200),
        (Some("repeated-composite-key"), Some(2), 201),
        (None, None, 202),
        (Some("null-mismatch"), Some(2), 203),
    ];
    let right_diffs = [-2, 4, 2, 1];
    let left = nullable_composite_batch("left_payload", &left_rows, &left_diffs);
    let right = nullable_composite_batch("right_payload", &right_rows, &right_diffs);
    let left_conv = KeyConverter::new(left.schema().as_ref(), &[0, 1]).unwrap();
    let right_conv = KeyConverter::new(right.schema().as_ref(), &[0, 1]).unwrap();
    let output_schema = joined_schema(&left.schema(), &right.schema());
    let output_conv = full_converter(output_schema.as_ref()).unwrap();

    let mut expected = BTreeMap::new();
    let mut expected_pairs = 0;
    for (li, lrow) in left_rows.iter().enumerate() {
        for (ri, rrow) in right_rows.iter().enumerate() {
            if lrow.0 == rrow.0 && lrow.1 == rrow.1 {
                expected_pairs += 1;
                expected.insert(
                    (
                        lrow.0.map(str::to_owned),
                        lrow.1,
                        lrow.2,
                        rrow.0.map(str::to_owned),
                        rrow.1,
                        rrow.2,
                    ),
                    left_diffs[li] * right_diffs[ri],
                );
            }
        }
    }

    for owned in [false, true] {
        test_support::reset_counts();
        let output = if owned {
            let _guard = OwnedDenseJoinKeyGuard::force();
            equi_join(&left, &left_conv, &right, &right_conv, &output_conv).unwrap()
        } else {
            equi_join(&left, &left_conv, &right, &right_conv, &output_conv).unwrap()
        };
        assert_eq!(output.1, expected_pairs);
        assert_eq!(composite_output_bag(&output.0), expected);
        let routes = test_support::counts();
        assert_eq!(routes.direct, 1);
        assert_eq!(routes.direct_pairs, expected_pairs);
        assert_eq!(routes.borrowed_key_buckets, usize::from(!owned));
        assert_eq!(routes.owned_key_buckets, usize::from(owned));
    }
}

type BinaryCompositeOutput = (
    Option<Vec<u8>>,
    Option<i64>,
    i64,
    Option<Vec<u8>>,
    Option<i64>,
    i64,
    i64,
);

type BinaryCompositeInput<'a> = (Option<&'a [u8]>, Option<i64>, i64);

fn nullable_binary_composite_batch(
    name: &str,
    rows: &[BinaryCompositeInput<'_>],
    diffs: &[i64],
) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("binary_key", DataType::Binary, true),
        Field::new("number_key", DataType::Int64, true),
        Field::new(name, DataType::Int64, false),
    ]));
    let record = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BinaryArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))) as ArrayRef,
        ],
    )
    .unwrap();
    ZSetBatch::new(record, Arc::new(Int64Array::from(diffs.to_vec()))).unwrap()
}

fn binary_composite_output_rows(output: &ZSetBatch) -> Vec<BinaryCompositeOutput> {
    let left_binary = output
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let left_number = output
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let left_payload = output
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let right_binary = output
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let right_number = output
        .batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let right_payload = output
        .batch
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diffs = output.diff().as_any().downcast_ref::<Int64Array>().unwrap();
    (0..output.len())
        .map(|row| {
            (
                left_binary
                    .is_valid(row)
                    .then(|| left_binary.value(row).to_vec()),
                left_number.is_valid(row).then(|| left_number.value(row)),
                left_payload.value(row),
                right_binary
                    .is_valid(row)
                    .then(|| right_binary.value(row).to_vec()),
                right_number.is_valid(row).then(|| right_number.value(row)),
                right_payload.value(row),
                diffs.value(row),
            )
        })
        .collect()
}

#[test]
fn borrowed_full_binary_composite_keys_preserve_nulls_prefixes_order_and_diffs() {
    let prefix = [0xff, 0x00, 0x7f, 0x00];
    let mut key_a = prefix.to_vec();
    key_a.push(0xfe);
    let mut key_b = prefix.to_vec();
    key_b.push(0xff);
    let left_rows = [
        (Some(key_a.as_slice()), Some(2), 10),
        (Some(key_a.as_slice()), Some(2), 11),
        (Some(key_b.as_slice()), Some(2), 12),
        (None, None, 13),
        (None, Some(2), 14),
    ];
    let left_diffs = [2, -1, 3, -2, 5];
    let right_rows = [
        (Some(key_a.as_slice()), Some(2), 20),
        (Some(key_b.as_slice()), Some(2), 21),
        (None, None, 22),
        (None, Some(2), 23),
        (Some(key_a.as_slice()), Some(3), 24),
    ];
    let right_diffs = [-3, 2, 4, -1, 1];
    let left = nullable_binary_composite_batch("left_payload", &left_rows, &left_diffs);
    let right = nullable_binary_composite_batch("right_payload", &right_rows, &right_diffs);
    let left_conv = KeyConverter::new(left.schema().as_ref(), &[0, 1]).unwrap();
    let right_conv = KeyConverter::new(right.schema().as_ref(), &[0, 1]).unwrap();
    let output_schema = joined_schema(&left.schema(), &right.schema());
    let output_conv = full_converter(output_schema.as_ref()).unwrap();

    let expected = vec![
        (None, None, 13, None, None, 22, -8),
        (None, Some(2), 14, None, Some(2), 23, -5),
        (
            Some(key_a.clone()),
            Some(2),
            10,
            Some(key_a.clone()),
            Some(2),
            20,
            -6,
        ),
        (
            Some(key_a.clone()),
            Some(2),
            11,
            Some(key_a.clone()),
            Some(2),
            20,
            3,
        ),
        (
            Some(key_b.clone()),
            Some(2),
            12,
            Some(key_b),
            Some(2),
            21,
            6,
        ),
    ];
    let mut outputs = Vec::new();
    for owned in [false, true] {
        test_support::reset_counts();
        let joined = if owned {
            let _guard = OwnedDenseJoinKeyGuard::force();
            equi_join(&left, &left_conv, &right, &right_conv, &output_conv).unwrap()
        } else {
            equi_join(&left, &left_conv, &right, &right_conv, &output_conv).unwrap()
        };
        assert_eq!(
            joined.1, 5,
            "different binary tails and composite keys do not match"
        );
        let routes = test_support::counts();
        assert_eq!(routes.direct, 1);
        assert_eq!(routes.direct_pairs, 5);
        assert_eq!(routes.borrowed_key_buckets, usize::from(!owned));
        assert_eq!(routes.owned_key_buckets, usize::from(owned));
        outputs.push(joined.0);
    }
    drop(left);
    drop(right);
    assert_eq!(binary_composite_output_rows(&outputs[0]), expected);
    assert_eq!(binary_composite_output_rows(&outputs[1]), expected);
}
