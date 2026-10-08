//! Unit tests for Z-set sorting and consolidation.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use hotlap_engine::{ZSetBatch, consolidate, sort_rows};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]))
}

fn zset(keys: Vec<i64>, values: Vec<&str>, diff: Vec<i64>) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(StringArray::from(values)),
    ];
    let batch = RecordBatch::try_new(schema(), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diff))).unwrap()
}

fn diffs(zset: &ZSetBatch) -> Vec<i64> {
    let array = zset
        .diff
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 diff");
    (0..array.len()).map(|index| array.value(index)).collect()
}

fn key_values(zset: &ZSetBatch) -> Vec<i64> {
    let array = zset
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 key");
    (0..array.len()).map(|index| array.value(index)).collect()
}

#[test]
fn consolidate_sums_duplicates_and_drops_zeros() {
    // +1 +1 -1 for a repeated full row leaves +1; a zero-sum row disappears.
    let input = zset(
        vec![1, 1, 1, 2, 2],
        vec!["a", "a", "a", "b", "b"],
        vec![1, 1, -1, 1, -1],
    );
    let out = consolidate(&input).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(key_values(&out), vec![1]);
    assert_eq!(diffs(&out), vec![1]);
}

#[test]
fn consolidate_keeps_same_key_with_different_payload() {
    // Same key, different payload means different full rows: both survive.
    let input = zset(vec![1, 1], vec!["a", "b"], vec![1, 1]);
    let out = consolidate(&input).unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(key_values(&out), vec![1, 1]);
    assert_eq!(diffs(&out), vec![1, 1]);
}

#[test]
fn consolidate_is_deterministic() {
    let input = zset(vec![2, 1, 2], vec!["b", "a", "b"], vec![1, 1, 1]);
    let first = consolidate(&input).unwrap();
    let second = consolidate(&input).unwrap();
    assert_eq!(key_values(&first), key_values(&second));
    assert_eq!(diffs(&first), diffs(&second));
}

#[test]
fn sort_rows_orders_by_key() {
    let input = zset(vec![3, 1, 2], vec!["c", "a", "b"], vec![1, 2, 3]);
    let out = sort_rows(&input).unwrap();
    assert_eq!(key_values(&out), vec![1, 2, 3]);
    assert_eq!(diffs(&out), vec![2, 3, 1]);
}

#[test]
fn consolidate_errors_on_diff_sum_overflow() {
    // Two copies of the same row with i64::MAX never fit in a signed sum.
    let input = zset(vec![1, 1], vec!["a", "a"], vec![i64::MAX, i64::MAX]);
    assert!(consolidate(&input).is_err());
}
