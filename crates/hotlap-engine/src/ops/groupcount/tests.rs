//! Unit tests for the incremental group-count reducer.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::GroupCount;
use crate::batch::ZSetBatch;

fn zset(rows: &[(i64, &str, i64)]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )),
    ];
    let diffs: Vec<i64> = rows.iter().map(|row| row.2).collect();
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

/// Reads `((key, count), diff)` rows sorted for comparison.
fn rows(z: &ZSetBatch) -> Vec<((i64, i64), i64)> {
    let keys = z
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = z
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diffs = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut out: Vec<((i64, i64), i64)> = (0..z.len())
        .map(|i| ((keys.value(i), counts.value(i)), diffs.value(i)))
        .collect();
    out.sort();
    out
}

#[test]
fn work_is_proportional_to_touched_keys_not_keyspace() {
    let mut reducer = GroupCount::new(&[0]);
    for key in 0..50i64 {
        let out = reducer.apply(&zset(&[(key, "v", 1)])).unwrap();
        // Each push touches exactly one key, regardless of accumulated keys.
        assert_eq!(reducer.work(), 1);
        assert_eq!(rows(&out), vec![((key, 1), 1)]);
    }
}

#[test]
fn count_overflow_is_an_error_not_a_wrap() {
    let mut reducer = GroupCount::new(&[0]);
    reducer.apply(&zset(&[(1, "a", i64::MAX)])).unwrap();
    let result = reducer.apply(&zset(&[(1, "a", 1)]));
    assert!(result.is_err());
}

#[test]
fn retraction_to_zero_emits_old_count_only() {
    let mut reducer = GroupCount::new(&[0]);
    reducer.apply(&zset(&[(1, "a", 1), (1, "b", 1)])).unwrap();
    let out = reducer.apply(&zset(&[(1, "a", -1), (1, "b", -1)])).unwrap();
    assert_eq!(rows(&out), vec![((1, 2), -1)]);
    assert_eq!(reducer.work(), 1);
}
